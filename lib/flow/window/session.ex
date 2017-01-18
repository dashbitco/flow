defmodule Flow.Window.Session do
  @moduledoc false

  @enforce_keys [:by]
  defstruct [:by, :gap, :trigger, periodically: []]

  def departition(_flow) do
    raise ArgumentError, "cannot departition on a session window because each session window has its own data"
  end

  def materialize(%{by: by, gap: gap}, reducer_acc, reducer_fun, reducer_trigger, options) do
    key = key_to_fun(options[:key])
    acc = fn -> %{} end

    fun =
      fn ref, events, windows, index ->
        events = annotate(events, key, by)
        dispatch(events, [], windows, gap, ref, index, reducer_acc, reducer_fun, reducer_trigger)
      end

    trigger =
      fn windows, index, op, name ->
        trigger(Map.to_list(windows), [], %{}, index, op, name, reducer_trigger)
      end

    {acc, fun, trigger}
  end

  defp key_to_fun(nil) do
    raise ArgumentError, "Flow.Window.session/3 requires the :key option to be set when partitioning"
  end
  defp key_to_fun({:elem, pos}) when pos >= 0 do
    pos = pos + 1
    &:erlang.element(pos, &1)
  end
  defp key_to_fun({:key, key}) do
    &Map.fetch!(&1, key)
  end
  defp key_to_fun(fun) when is_function(fun, 1) do
    fun
  end

  defp annotate(events, key, by) do
    for event <- events do
      {key.(event), by.(event), event}
    end
  end

  defp dispatch([{key, by, event} | rest], emit, windows,
                gap, ref, index, reducer_acc, reducer_fun, reducer_trigger) do
    {trigger_emit, first, acc} = get_window(windows, key, by, gap, index, reducer_acc, reducer_trigger)
    {events, rest, last} = look_ahead(rest, key, by, gap, [event], [])
    {reducer_emit, acc} =
      if is_function(reducer_fun, 4) do
        reducer_fun.(ref, events, acc, index)
      else
        reducer_fun.(ref, events, acc, index, {:session, {key, first, last}, :placeholder})
      end
    dispatch(rest, emit ++ trigger_emit ++ reducer_emit, Map.put(windows, key, {first, last, acc}),
             gap, ref, index, reducer_acc, reducer_fun, reducer_trigger)
  end
  defp dispatch([], emit, windows, _gap, _ref, _index, _reducer_acc, _reducer_fun, _reducer_trigger) do
    {emit, windows}
  end

  defp get_window(windows, key, by, gap, index, reducer_acc, reducer_trigger) do
    case windows do
      %{^key => {first, last, acc}} when by - last > gap ->
        {emit, _} = reducer_trigger.(acc, index, :keep, {:session, {key, first, last}, :done})
        {emit, by, reducer_acc.()}
      %{^key => {first, _last, acc}} ->
        {[], first, acc}
      %{} ->
        {[], by, reducer_acc.()}
    end
  end

  defp look_ahead([{key, by, _} | _] = tuples, key, last, gap, events, rest) when by - last > gap do
    {:lists.reverse(events), :lists.reverse(rest, tuples), last}
  end
  defp look_ahead([{key, by, event} | tuples], key, _last, gap, events, rest) do
    look_ahead(tuples, key, by, gap, [event | events], rest)
  end
  defp look_ahead([tuple | tuples], key, last, gap, events, rest) do
    look_ahead(tuples, key, last, gap, events, [tuple | rest])
  end
  defp look_ahead([], _key, last, _gap, events, rest) do
    {:lists.reverse(events), :lists.reverse(rest), last}
  end

  defp trigger([{key, {first, last, acc}} | rest], emit, windows, index, op, name, reducer_trigger) do
    {trigger_emit, acc} = reducer_trigger.(acc, index, op, {:session, {key, first, last}, name})
    trigger(rest, emit ++ trigger_emit, Map.put(windows, key, {first, last, acc}),
            index, op, name, reducer_trigger)
  end
  defp trigger([], emit, windows, _index, _op, _name, _reducer_trigger) do
    {emit, windows}
  end
end
