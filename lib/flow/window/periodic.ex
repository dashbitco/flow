defmodule Flow.Window.Periodic do
  @moduledoc false

  @enforce_keys [:duration]
  defstruct [:duration, :trigger, periodically: []]

  def departition(flow) do
    flow
  end

  def materialize(%{duration: duration}, reducer_acc, reducer_fun, reducer_trigger, _options) do
    ref = make_ref()
    acc =
      fn ->
        send_after(ref, duration)
        {0, reducer_acc.()}
      end

    fun =
      if is_function(reducer_fun, 4) do
        fn ref, events, {window, acc}, index ->
          {emit, acc} = reducer_fun.(ref, events, acc, index)
          {emit, {window, acc}}
        end
      else
        fn ref, events, {window, acc}, index ->
          {emit, acc} = reducer_fun.(ref, events, acc, index, {:periodic, window, :placeholder})
          {emit, {window, acc}}
        end
      end

    trigger =
      fn
        {window, acc}, index, op, ^ref ->
          {emit, _} = reducer_trigger.(acc, index, op, {:periodic, window, :done})
          send_after(ref, duration)
          {emit, {window + 1, reducer_acc.()}}
        {window, acc}, index, op, name ->
          {emit, acc} = reducer_trigger.(acc, index, op, {:periodic, window, name})
          {emit, {window, acc}}
      end

    {acc, fun, trigger}
  end

  defp send_after(ref, duration) do
    Process.send_after(self(), {:trigger, :keep, ref}, duration)
  end
end
