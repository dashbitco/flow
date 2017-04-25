defmodule Flow.Materialize do
  @moduledoc false

  @compile :inline_list_funcs
  @map_reducer_opts [:buffer_keep, :buffer_size, :dispatcher]

  def materialize(%{producers: nil}, _, _, _) do
    raise ArgumentError, "cannot execute a flow without producers, " <>
                         "please call \"from_enumerable\" or \"from_stage\" accordingly"
  end

  def materialize(%{operations: operations, options: options, producers: producers, window: window},
                  start_link, type, type_options) do
    options = Keyword.merge(type_options, options)
    ops = split_operations(operations)
    {producers, consumers, ops, window} = start_producers(producers, ops, start_link, window, options)
    {producers, start_stages(ops, window, consumers, start_link, type, options)}
  end

  ## Helpers

  @doc """
  Splits the flow operations into layers of stages.
  """
  def split_operations([]) do
    :none
  end
  def split_operations(operations) do
    split_operations(:lists.reverse(operations), :mapper, [])
  end

  defp split_operations([{:mapper, _, _} = op | ops], :mapper, acc_ops) do
    split_operations(ops, :mapper, [op | acc_ops])
  end
  defp split_operations([op | ops], _type, acc_ops) do
    split_operations(ops, :reducer, [op | acc_ops])
  end
  defp split_operations([], :mapper, ops) do
    {:mapper, mapper_ops(ops), :lists.reverse(ops)}
  end
  defp split_operations([], :reducer, ops) do
    ops = :lists.reverse(ops)
    {:reducer, reducer_ops(ops), ops}
  end

  defp start_stages(:none, window, producers, _start_link, _type, _options) do
    if window != Flow.Window.global do
      raise ArgumentError, "a window was set but no computation is happening on this partition"
    end
    for {producer, producer_opts} <- producers do
      {producer, [cancel: :transient] ++ producer_opts}
    end
  end
  defp start_stages({_mr, compiled_ops, _ops}, window, producers, start_link, type, opts) do
    {acc, reducer, trigger} = window_ops(window, compiled_ops, opts)
    {stages, opts} = Keyword.pop(opts, :stages)
    {init_opts, subscribe_opts} = Keyword.split(opts, @map_reducer_opts)
    init_opts =
      case type do
        :consumer -> Keyword.drop(init_opts, [:dispatcher])
        _ -> init_opts
      end

    for i <- 0..stages-1 do
      subscriptions =
        for {producer, producer_opts} <- producers do
          opts = Keyword.merge(subscribe_opts, producer_opts)
          {producer, [partition: i, cancel: :transient] ++ opts}
        end
      arg = {type, [subscribe_to: subscriptions] ++ init_opts, {i, stages}, trigger, acc, reducer}
      {:ok, pid} = start_link.(Flow.MapReducer, arg, [])
      {pid, [cancel: :transient]}
    end
  end

  ## Producers

  defp start_producers({:join, kind, left, right, left_key, right_key, join},
                       ops, start_link, window, options) do
    partitions = Keyword.fetch!(options, :stages)
    {left_producers, left_consumers} = start_join(:left, left, left_key, partitions, start_link)
    {right_producers, right_consumers} = start_join(:right, right, right_key, partitions, start_link)
    {type, {acc, fun, trigger}, ops} = ensure_ops(ops)

    window =
      case window do
        %{by: by} -> %{window | by: fn x -> by.(elem(x, 1)) end}
        %{} -> window
      end

    {left_producers ++ right_producers,
     left_consumers ++ right_consumers,
     {type, join_ops(kind, join, acc, fun, trigger), ops},
     window}
  end
  defp start_producers({:departition, flow, acc_fun, merge_fun, done_fun},
                       ops, start_link, window, options) do
    flow = flow.window.__struct__.departition(flow)
    {producers, consumers} = materialize(flow, start_link, :producer_consumer, options)
    {type, {acc, fun, trigger}, ops} = ensure_ops(ops)

    stages = Keyword.fetch!(flow.options, :stages)
    partitions = Enum.to_list(0..stages-1)

    {producers, consumers,
     {type, departition_ops(acc, fun, trigger, partitions, acc_fun, merge_fun, done_fun), ops},
     window}
  end
  defp start_producers({:flows, flows}, ops, start_link, window, options) do
    options = partition(options)
    {producers, consumers} =
      Enum.reduce(flows, {[], []}, fn flow, {producers_acc, consumers_acc} ->
        {producers, consumers} = materialize(flow, start_link, :producer_consumer, options)
        {producers ++ producers_acc, consumers ++ consumers_acc}
      end)
    {producers, consumers, ensure_ops(ops), window}
  end
  defp start_producers({:stages, producers}, ops, _start_link, window, options) do
    producers = for producer <- producers, do: {producer, []}

    # If there are no more stages and there is a need for a custom
    # dispatcher, we need to wrap the sources in a custom stage.
    if Keyword.has_key?(options, :dispatcher) do
      {producers, producers, ensure_ops(ops), window}
    else
      {producers, producers, ops, window}
    end
  end
  defp start_producers({:enumerables, enumerables}, ops, start_link, window, options) do
    # options configures all stages before partition, so it effectively
    # controls the number of stages consuming the enumerables.
    stages = Keyword.fetch!(options, :stages)

    case ops do
      {:mapper, _compiled_ops, mapper_ops} when stages < length(enumerables) ->
        # Fuse mappers into enumerables if we have more enumerables than stages.
        producers = start_enumerables(enumerables, mapper_ops, options, start_link)
        {producers, producers, :none, window}
      :none ->
        # If there are no ops, just start the enumerables with the options.
        producers = start_enumerables(enumerables, [], options, start_link)
        {producers, producers, :none, window}
      _ ->
        # Otherwise it is a regular producer consumer with demand dispatcher.
        # In this case, options is used by subsequent mapper/reducer stages.
        producers = start_enumerables(enumerables, [], [], start_link)
        {producers, producers, ops, window}
    end
  end

  defp start_enumerables(enumerables, ops, opts, start_link) do
    opts = [demand: :accumulate] ++ Keyword.take(opts, @map_reducer_opts)

    for enumerable <- enumerables do
      stream =
        :lists.foldl(fn {:mapper, fun, args}, acc ->
          apply(Stream, fun, [acc | args])
        end, enumerable, ops)
      {:ok, pid} = start_link.(GenStage.Streamer, {stream, opts}, opts)
      {pid, []}
    end
  end

  defp partition(options) do
    stages = Keyword.fetch!(options, :stages)
    hash = options[:hash] || hash_by_key(options[:key], stages)
    dispatcher_opts = [partitions: 0..stages-1, hash: hash(hash)]
    [dispatcher: {GenStage.PartitionDispatcher, dispatcher_opts}]
  end

  defp hash(fun) when is_function(fun, 1) do
    fun
  end
  defp hash(other) do
    raise ArgumentError, "expected :hash to be a function that receives an event and " <>
                         "returns a tuple with the event and its partition, got: #{inspect other}"
  end

  defp hash_by_key(nil, stages) do
    &{&1, :erlang.phash2(&1, stages)}
  end
  defp hash_by_key({:elem, pos}, stages) when pos >= 0 do
    pos = pos + 1
    &{&1, :erlang.phash2(:erlang.element(pos, &1), stages)}
  end
  defp hash_by_key({:key, key}, stages) do
    &{&1, :erlang.phash2(Map.fetch!(&1, key), stages)}
  end
  defp hash_by_key(fun, stages) when is_function(fun, 1) do
    &{&1, :erlang.phash2(fun.(&1), stages)}
  end
  defp hash_by_key(other, _) do
    raise ArgumentError, """
    expected :key to be one of:

      * a function expecting an event and returning a key
      * {:elem, pos} when pos >= 0
      * {:key, key}

    instead got: #{inspect other}
    """
  end

  defp ensure_ops(:none),
    do: {:mapper, mapper_ops([]), []}
  defp ensure_ops(ops),
    do: ops

  ## Departition

  defp departition_ops(reducer_acc, reducer_fun, reducer_trigger, partitions, acc_fun, merge_fun, done_fun) do
    acc = fn -> {reducer_acc.(), %{}} end

    events = fn ref, events, {acc, windows}, index ->
      {events, windows} = dispatch_departition(events, windows, partitions, acc_fun, merge_fun, done_fun)
      {events, acc} = reducer_fun.(ref, :lists.reverse(events), acc, index)
      {events, {acc, windows}}
    end

    trigger = fn
      {acc, windows}, index, op, {_, _, :done} = name ->
        done =
          for {window, {_partitions, acc}} <- :lists.sort(:maps.to_list(windows)) do
            done_fun.(acc, window)
          end
        {events, _} = reducer_trigger.(acc, index, op, name)
        {done ++ events, {reducer_acc.(), %{}}}

      {acc, windows}, index, op, name ->
        {events, acc} = reducer_trigger.(acc, index, op, name)
        {events, {acc, windows}}
    end

    {acc, events, trigger}
  end

  defp dispatch_departition(events, windows, partitions, acc_fun, merge_fun, done_fun) do
    :lists.foldl(fn {state, partition, {_, window, name}}, {events, windows} ->
      {partitions, acc} = get_window_data(windows, window, partitions, acc_fun)
      partitions = remove_partition_on_done(name, partitions, partition)
      acc = merge_fun.(state, acc)
      case partitions do
        [] ->
          {[done_fun.(acc, window) | events], Map.delete(windows, window)}
        _  ->
          {events, Map.put(windows, window, {partitions, acc})}
      end
    end, {[], windows}, events)
  end

  defp remove_partition_on_done(:done, partitions, partition) do
    List.delete(partitions, partition)
  end
  defp remove_partition_on_done(_, partitions, _) do
    partitions
  end

  defp get_window_data(windows, window, partitions, acc_fun) do
    case windows do
      %{^window => value} -> value
      %{} -> {partitions, acc_fun.()}
    end
  end

  ## Joins

  defp start_join(side, flow, key_fun, stages, start_link) do
    hash = fn event ->
      key = key_fun.(event)
      {{key, event}, :erlang.phash2(key, stages)}
    end

    opts = [dispatcher: {GenStage.PartitionDispatcher, partitions: 0..stages-1, hash: hash}]
    {producers, consumers} = materialize(flow, start_link, :producer_consumer, opts)

    {producers,
      for {consumer, consumer_opts} <- consumers do
        {consumer, [tag: side] ++ consumer_opts}
      end}
  end

  defp join_ops(kind, join, acc, fun, trigger) do
    acc = fn -> {%{}, %{}, acc.()} end

    events = fn ref, events, {left, right, acc}, index ->
      {events, left, right} = dispatch_join(events, Process.get(ref), left, right, join, [])
      {events, acc} = fun.(ref, events, acc, index)
      {events, {left, right, acc}}
    end

    ref = make_ref()

    trigger = fn
      {left, right, acc}, index, op, {_, _, :done} = name ->
        {kind_events, acc} =
          case kind do
            :inner ->
              {[], acc}
            :left_outer ->
              fun.(ref, left_events(Map.keys(left), Map.keys(right), left, join), acc, index)
            :right_outer ->
              fun.(ref, right_events(Map.keys(right), Map.keys(left), right, join), acc, index)
            :full_outer ->
              left_keys = Map.keys(left)
              right_keys = Map.keys(right)
              {left_events, acc} = fun.(ref, left_events(left_keys, right_keys, left, join), acc, index)
              {right_events, acc} = fun.(ref, right_events(right_keys, left_keys, right, join), acc, index)
              {left_events ++ right_events, acc}
          end
        {trigger_events, acc} = trigger.(acc, index, op, name)
        {kind_events ++ trigger_events, {left, right, acc}}
      {left, right, acc}, index, op, name ->
        {events, acc} = trigger.(acc, index, op, name)
        {events, {left, right, acc}}
    end

    {acc, events, trigger}
  end

  defp left_events(left, right, source, join) do
    for key <- left -- right, entry <- Map.fetch!(source, key), do: join.(entry, nil)
  end

  defp right_events(right, left, source, join) do
    for key <- right -- left, entry <- Map.fetch!(source, key), do: join.(nil, entry)
  end

  defp dispatch_join([{key, left} | rest], :left, left_acc, right_acc, join, acc) do
    acc =
      case right_acc do
        %{^key => rights} ->
          :lists.foldl(fn right, acc -> [join.(left, right) | acc] end, acc, rights)
        %{} -> acc
      end
    left_acc = Map.update(left_acc, key, [left], &[left | &1])
    dispatch_join(rest, :left, left_acc, right_acc, join, acc)
  end
  defp dispatch_join([{key, right} | rest], :right, left_acc, right_acc, join, acc) do
    acc =
      case left_acc do
        %{^key => lefties} ->
          :lists.foldl(fn left, acc -> [join.(left, right) | acc] end, acc, lefties)
        %{} -> acc
      end
    right_acc = Map.update(right_acc, key, [right], &[right | &1])
    dispatch_join(rest, :right, left_acc, right_acc, join, acc)
  end
  defp dispatch_join([], _, left_acc, right_acc, _join, acc) do
    {:lists.reverse(acc), left_acc, right_acc}
  end

  ## Windows

  defp window_ops(%{trigger: trigger, periodically: periodically} = window,
                  {reducer_acc, reducer_fun, reducer_trigger}, options) do
    {window_acc, window_fun, window_trigger} =
      window_trigger(trigger, reducer_acc, reducer_fun, reducer_trigger)
    {type_acc, type_fun, type_trigger} =
      window.__struct__.materialize(window, window_acc, window_fun, window_trigger, options)
    {window_periodically(type_acc, periodically), type_fun, type_trigger}
  end

  defp window_trigger(nil, reducer_acc, reducer_fun, reducer_trigger) do
    {reducer_acc, reducer_fun, reducer_trigger}
  end
  defp window_trigger({punctuation_acc, punctuation_fun},
                      reducer_acc, reducer_fun, reducer_trigger) do
    {fn -> {punctuation_acc.(), reducer_acc.()} end,
     build_punctuated_reducer(punctuation_fun, reducer_fun, reducer_trigger),
     build_punctuated_trigger(reducer_trigger)}
  end

  defp build_punctuated_reducer(punctuation_fun, red_fun, trigger) do
    fn ref, events, {pun_acc, red_acc}, index, name ->
      maybe_punctuate(ref, events, punctuation_fun, pun_acc, red_acc, red_fun, index, name, trigger, [])
    end
  end

  defp build_punctuated_trigger(trigger) do
    fn {trigger_acc, red_acc}, index, op, name ->
      {events, red_acc} = trigger.(red_acc, index, op, name)
      {events, {trigger_acc, red_acc}}
    end
  end

  defp maybe_punctuate(ref, events, punctuation_fun, pun_acc, red_acc,
                       red_fun, index, name, trigger, collected) do
    case punctuation_fun.(events, pun_acc) do
      {:trigger, trigger_name, pre, op, pos, pun_acc} ->
        {red_events, red_acc} = red_fun.(ref, pre, red_acc, index)
        {trigger_events, red_acc} = trigger.(red_acc, index, op, put_elem(name, 2, trigger_name))
        maybe_punctuate(ref, pos, punctuation_fun, pun_acc, red_acc,
                        red_fun, index, name, trigger, collected ++ trigger_events ++ red_events)
      {:cont, [], pun_acc} ->
        {collected, {pun_acc, red_acc}}
      {:cont, emitted_events, pun_acc} ->
        {red_events, red_acc} = red_fun.(ref, emitted_events, red_acc, index)
        {collected ++ red_events, {pun_acc, red_acc}}
      {:cont, pun_acc} ->
        {red_events, red_acc} = red_fun.(ref, events, red_acc, index)
        {collected ++ red_events, {pun_acc, red_acc}}
    end
  end

  defp window_periodically(window_acc, []) do
    window_acc
  end
  defp window_periodically(window_acc, periodically) do
    fn ->
      for {time, keep_or_reset, name} <- periodically do
        {:ok, _} = :timer.send_interval(time, self(), {:trigger, keep_or_reset, name})
      end
      window_acc.()
    end
  end

  ## Reducers

  defp reducer_ops(ops) do
    case take_mappers(ops, []) do
      {mappers, [{:reduce, reducer_acc, reducer_fun} | ops]} ->
        {reducer_acc, build_reducer(mappers, reducer_fun), build_trigger(ops, reducer_acc)}
      {mappers, [{:uniq, uniq_by} | ops]} ->
        {acc, reducer, trigger} = reducer_ops(ops)
        {fn -> {%{}, acc.()} end,
         build_uniq_reducer(mappers, reducer, uniq_by),
         build_uniq_trigger(trigger)}
      {mappers, ops} ->
        {fn -> [] end, build_reducer(mappers, &[&1 | &2]), build_trigger(ops, fn -> [] end)}
    end
  end

  defp build_reducer(mappers, fun) do
    reducer = :lists.foldl(&mapper/2, fun, mappers)
    fn _ref, events, acc, _index ->
      {[], :lists.foldl(reducer, acc, events)}
    end
  end

  @protocol_undefined "if you would like to emit a modified state from flow, like " <>
                      "a counter or a custom data-structure, please call Flow.emit/2 accordingly"

  defp build_trigger(ops, acc_fun) do
    map_states = merge_map_state(ops)

    fn acc, index, op, name ->
      events = :lists.foldl(& &1.(&2, index, name), acc, map_states)

      try do
        Enum.to_list(events)
      rescue
        e in Protocol.UndefinedError ->
          msg = @protocol_undefined

          e = update_in e.description, fn
            "" -> msg
            dc -> dc <> " (#{msg})"
          end

          reraise e, System.stacktrace
      else
        events ->
          case op do
            :keep -> {events, acc}
            :reset -> {events, acc_fun.()}
          end
      end
    end
  end

  defp build_uniq_reducer(mappers, reducer, uniq_by) do
    uniq_by = :lists.foldl(&mapper/2, uniq_by_reducer(uniq_by), mappers)
    fn ref, events, {set, acc}, index ->
      {set, events} = :lists.foldl(uniq_by, {set, []}, events)
      {events, acc} = reducer.(ref, :lists.reverse(events), acc, index)
      {events, {set, acc}}
    end
  end

  defp uniq_by_reducer(uniq_by) do
    fn event, {set, acc} ->
      key = uniq_by.(event)
      case set do
        %{^key => true} -> {set, acc}
        %{} -> {Map.put(set, key, true), [event | acc]}
      end
    end
  end

  defp build_uniq_trigger(trigger) do
    fn {set, acc}, index, op, name ->
      {events, acc} = trigger.(acc, index, op, name)
      {events, {set, acc}}
    end
  end

  defp merge_map_state(ops) do
    case take_mappers(ops, []) do
      {[], [{:map_state, fun} | ops]} ->
        [fun | merge_map_state(ops)]
      {[], [{:uniq, by} | ops]} ->
        [fn acc, _, _ -> Enum.uniq_by(acc, by) end | merge_map_state(ops)]
      {[], []} ->
        []
      {mappers, ops} ->
        reducer = :lists.foldl(&mapper/2, &[&1 | &2], mappers)
        [fn old_acc, _, _ -> Enum.reduce(old_acc, [], reducer) end | merge_map_state(ops)]
    end
  end

  ## Mappers

  defp mapper_ops(ops) do
    reducer = :lists.foldl(&mapper/2, &[&1 | &2], ops)
    {fn -> [] end,
     fn _ref, events, [], _index -> {:lists.reverse(:lists.foldl(reducer, [], events)), []} end,
     fn _acc, _index, _op, _trigger -> {[], []} end}
  end

  defp mapper({:mapper, :each, [each]}, fun) do
    fn x, acc -> each.(x); fun.(x, acc) end
  end
  defp mapper({:mapper, :filter, [filter]}, fun) do
    fn x, acc ->
      if filter.(x) do
        fun.(x, acc)
      else
        acc
      end
    end
  end
  defp mapper({:mapper, :filter_map, [filter, mapper]}, fun) do
    fn x, acc ->
      if filter.(x) do
        fun.(mapper.(x), acc)
      else
        acc
      end
    end
  end
  defp mapper({:mapper, :flat_map, [flat_mapper]}, fun) do
    fn x, acc ->
      Enum.reduce(flat_mapper.(x), acc, fun)
    end
  end
  defp mapper({:mapper, :map, [mapper]}, fun) do
    fn x, acc -> fun.(mapper.(x), acc) end
  end
  defp mapper({:mapper, :reject, [filter]}, fun) do
    fn x, acc ->
      if filter.(x) do
        acc
      else
        fun.(x, acc)
      end
    end
  end

  defp take_mappers([{:mapper, _, _} = mapper | ops], acc),
    do: take_mappers(ops, [mapper | acc])
  defp take_mappers(ops, acc),
    do: {acc, ops}
end
