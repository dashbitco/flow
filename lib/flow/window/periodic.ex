defmodule Flow.Window.Periodic do
  @moduledoc false

  @enforce_keys [:duration]
  defstruct [:duration, :trigger, periodically: []]

  def materialize(%{duration: duration}, reducer_acc, reducer_fun, reducer_trigger, _options) do
    ref = make_ref()

    acc = fn ->
      timer = send_after(ref, duration)
      {0, timer, reducer_acc.()}
    end

    fun =
      if is_function(reducer_fun, 4) do
        fn ref, events, {window, timer, acc}, index ->
          {emit, acc} = reducer_fun.(ref, events, acc, index)
          {emit, {window, timer, acc}}
        end
      else
        fn ref, events, {window, timer, acc}, index ->
          {emit, acc} = reducer_fun.(ref, events, acc, index, {:periodic, window, :placeholder})
          {emit, {window, timer, acc}}
        end
      end

    trigger = fn
      {window, _timer, acc}, index, ^ref ->
        {emit, _} = reducer_trigger.(acc, index, {:periodic, window, :done})
        timer = send_after(ref, duration)
        {emit, {window + 1, timer, reducer_acc.()}}

      {window, timer, acc}, index, name ->
        if name == :done, do: cancel_after(ref, timer)
        {emit, acc} = reducer_trigger.(acc, index, {:periodic, window, name})
        {emit, {window, timer, acc}}
    end

    {acc, fun, trigger}
  end

  defp send_after(ref, duration) do
    Process.send_after(self(), {:trigger, ref}, duration)
  end

  defp cancel_after(ref, timer) do
    case Process.cancel_timer(timer) do
      false ->
        receive do
          {:trigger, ^ref} -> :ok
        after
          0 -> :ok
        end

      _ ->
        :ok
    end
  end
end
