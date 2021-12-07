defmodule Flow.Window.Fixed do
  @moduledoc false

  @enforce_keys [:by, :duration]
  defstruct [:by, :duration, :trigger, lateness: 0, periodically: []]

  def materialize(
        %{by: by, duration: duration, lateness: lateness},
        reducer_acc,
        reducer_fun,
        reducer_trigger,
        _options
      ) do
    ref = make_ref()
    acc = fn -> {nil, %{}} end
    lateness_fun = lateness_fun(lateness, duration, ref, reducer_acc, reducer_trigger)

    static = %{
      by: by,
      duration: duration,
      reducer_acc: reducer_acc,
      reducer_fun: reducer_fun
    }

    # The reducing function works in three stages.
    #
    # 1. We start processing all events, grouping all events that belong
    #    to the same window and then reducing them. One of the outcomes
    #    of this function is the most recent window for a given producer.
    #
    # 2. Next we store the most recent timestamp for the producer and get
    #    both minimum and maximum seen windows.
    #
    # 3. Finally we see which windows have been seen by all producers (min)
    #    and if we are still missing any producer data (max is nil). We catch
    #    up the all window to min, emitting triggers for the old windows.
    #
    fun = fn producers, ref, events, {all, windows}, index ->
      {reducer_emit, recent, windows} =
        split_events(events, ref, [], nil, Map.fetch!(producers, ref), windows, index, [], static)

      # Update the latest window for this producer
      producers = Map.put(producers, ref, recent)
      min_max = producers |> Map.values() |> Enum.min_max()

      {trigger_emit, acc} = emit_trigger_messages(all, min_max, windows, index, lateness_fun)
      {producers, reducer_emit ++ trigger_emit, acc}
    end

    trigger = fn acc, index, name ->
      handle_trigger(ref, duration, acc, index, name, reducer_acc, reducer_trigger)
    end

    {acc, fun, trigger}
  end

  ## Reducer

  defp split_events([event | events], ref, buffer, current, recent, windows, index, emit, static) do
    %{by: by, duration: duration} = static
    window = div(by!(by, event), duration)

    if is_nil(current) or window === current do
      split_events(events, ref, [event | buffer], window, recent, windows, index, emit, static)
    else
      {emit, recent, windows} =
        reduce_events(ref, buffer, current, recent, windows, index, emit, static)

      split_events(events, ref, [event], window, recent, windows, index, emit, static)
    end
  end

  defp split_events([], ref, buffer, window, recent, windows, index, emit, static) do
    reduce_events(ref, buffer, window, recent, windows, index, emit, static)
  end

  defp reduce_events(_ref, [], _window, recent, windows, _index, emit, _static) do
    {emit, recent, windows}
  end

  defp reduce_events(ref, buffer, window, recent, windows, index, emit, static) do
    events = :lists.reverse(buffer)

    case recent_window(window, recent, windows, static) do
      {:ok, window_acc, recent} ->
        reducer_fun = static.reducer_fun

        {new_emit, window_acc} =
          if is_function(reducer_fun, 4) do
            reducer_fun.(ref, events, window_acc, index)
          else
            trigger = {:fixed, window * static.duration, :placeholder}
            reducer_fun.(ref, events, window_acc, index, trigger)
          end

        {emit ++ new_emit, recent, Map.put(windows, window, window_acc)}

      :error ->
        {emit, recent, windows}
    end
  end

  defp recent_window(window, nil, windows, static) do
    case windows do
      %{^window => acc} -> {:ok, acc, window}
      %{} -> {:ok, static.reducer_acc.(), window}
    end
  end

  defp recent_window(window, recent, windows, static) do
    case windows do
      %{^window => acc} -> {:ok, acc, max(window, recent)}
      %{} when window >= recent -> {:ok, static.reducer_acc.(), window}
      %{} -> :error
    end
  end

  defp by!(by, event) do
    case by.(event) do
      x when is_integer(x) ->
        x

      x ->
        raise "Flow.Window.fixed/3 expects `by` function to return an integer, " <>
                "got #{inspect(x)} from #{inspect(by)}"
    end
  end

  ## Trigger emission

  # We still haven't received from all producers.
  defp emit_trigger_messages(old, {_, nil}, windows, _index, _lateness) do
    {[], {old, windows}}
  end

  # We received data from all producers from the first time.
  defp emit_trigger_messages(nil, {min, _}, windows, index, lateness) do
    emit_trigger_messages(Enum.min(Map.keys(windows)), min, windows, index, lateness, [])
  end

  # Catch up the old (all) to the new minimum.
  defp emit_trigger_messages(old, {min, _}, windows, index, lateness) do
    emit_trigger_messages(old, min, windows, index, lateness, [])
  end

  defp emit_trigger_messages(new, new, windows, _index, _lateness, emit) do
    {emit, {new, windows}}
  end

  defp emit_trigger_messages(old, new, windows, index, lateness, emit) do
    {new_emit, windows} = lateness.(old, windows, index)
    emit_trigger_messages(old + 1, new, windows, index, lateness, emit ++ new_emit)
  end

  defp lateness_fun(lateness, duration, ref, reducer_acc, reducer_trigger) do
    fn window, windows, index ->
      acc = Map.get_lazy(windows, window, reducer_acc)

      case lateness do
        0 ->
          {emit, _} = reducer_trigger.(acc, index, {:fixed, window * duration, :done})
          {emit, Map.delete(windows, window)}

        _ ->
          Process.send_after(self(), {:trigger, {ref, window}}, lateness)

          {emit, window_acc} =
            reducer_trigger.(acc, index, {:fixed, window * duration, :watermark})

          {emit, Map.put(windows, window, window_acc)}
      end
    end
  end

  ## Trigger handling

  # Lateness termination.
  def handle_trigger(ref, duration, {current, windows}, index, {ref, window}, _acc, trigger) do
    case windows do
      %{^window => acc} ->
        {emit, _window_acc} = trigger.(acc, index, {:fixed, window * duration, :done})
        {emit, {current, Map.delete(windows, window)}}

      %{} ->
        {[], {current, windows}}
    end
  end

  # Otherwise trigger all windows.
  def handle_trigger(_ref, _duration, {current, windows}, _index, _name, _acc, _trigger)
      when map_size(windows) == 0 do
    {[], {current, windows}}
  end

  def handle_trigger(_ref, duration, {current, windows}, index, name, acc, trigger) do
    {min, max} = windows |> Map.keys() |> Enum.min_max()
    {emit, windows} = trigger_all(min, max, duration, windows, index, name, acc, trigger, [])
    {emit, {current, windows}}
  end

  defp trigger_all(min, max, _duration, windows, _index, _name, _acc, _trigger, emit)
       when min > max do
    {emit, windows}
  end

  defp trigger_all(min, max, duration, windows, index, name, acc, trigger, emit) do
    window_acc = Map.get_lazy(windows, min, acc)
    {new_emit, window_acc} = trigger.(window_acc, index, {:fixed, min * duration, name})
    windows = Map.put(windows, min, window_acc)
    trigger_all(min + 1, max, duration, windows, index, name, acc, trigger, emit ++ new_emit)
  end
end
