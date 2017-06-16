defmodule Flow.Window.SlidingEvent do
  @moduledoc false

  @enforce_keys [:count, :overlap]
  defstruct [:count, :overlap, :trigger, periodically: []]

  def departition(flow) do
    flow
  end

  def materialize(%{count: max, overlap: max_overlap}, reducer_acc, reducer_fun, reducer_trigger, _options) do
    acc =
      fn -> {0, max, [], reducer_acc.()} end

    fun =
      fn ref, events, {window, count, overlap_events, acc}, index ->
        dispatch(events, overlap_events, window, count, [], acc,
                 ref, index, max, max_overlap, reducer_acc, reducer_fun, reducer_trigger)
      end

    trigger =
      fn {window, count, _overlap_events, acc}, index, op, name ->
        {emit, acc} = reducer_trigger.(acc, index, op, {:sliding, window, name})
        {emit, {window, count, acc}}
      end

    {acc, fun, trigger}
  end

  defp dispatch([], overlap_events, window, count, emit, acc, _ref, _index, _max, _overlap, _reducer_acc, _reducer_fun, _reducer_trigger) do
    {emit, {window, count, overlap_events, acc}}
  end
  defp dispatch(events, overlap_events, window, count, emit, acc, ref, index, max, overlap, reducer_acc, reducer_fun, reducer_trigger) do
    {count, events, rest} =
      collect(events, count, [])

    overlap_events = if count < overlap do
      overlap_events ++ collect_overlap(events, count, overlap)
    else
      overlap_events
    end

    {overlap_events, rest} = if length(overlap_events) == overlap do
      {[], overlap_events ++ rest}
    else
      {overlap_events, rest}
    end

    {reducer_emit, acc} =
      maybe_dispatch(events, acc, ref, index, window, reducer_fun)
    {trigger_emit, acc, window, count} =
      maybe_trigger(window, count, acc, index, max, reducer_acc, reducer_trigger)
    dispatch(rest, overlap_events, window, count, emit ++ reducer_emit ++ trigger_emit, acc,
             ref, index, max, overlap, reducer_acc, reducer_fun, reducer_trigger)
  end

  defp maybe_trigger(window, 0, acc, index, max, reducer_acc, reducer_trigger) do
    {trigger_emit, _} = reducer_trigger.(acc, index, :keep, {:sliding, window, :done})
    {trigger_emit, reducer_acc.(), window + 1, max}
  end
  defp maybe_trigger(window, count, acc, _index, _max, _reducer_acc, _reducer_trigger) do
    {[], acc, window, count}
  end

  defp maybe_dispatch([], acc, _ref, _index, _window, _reducer_fun) do
    {[], acc}
  end
  defp maybe_dispatch(events, acc, ref, index, window, reducer_fun) do
    if is_function(reducer_fun, 4) do
      reducer_fun.(ref, events, acc, index)
    else
      reducer_fun.(ref, events, acc, index, {:sliding, window, :placeholder})
    end
  end

  defp collect([], count, acc), do: {count, :lists.reverse(acc), []}
  defp collect(events, 0, acc), do: {0, :lists.reverse(acc), events}
  defp collect([event | events], count, acc), do: collect(events, count - 1, [event | acc])

  defp collect_overlap(events, count, overlap) do
    Enum.reverse(events) |> Enum.take(overlap - count) |> Enum.reverse()
  end
end
