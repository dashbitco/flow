defmodule Flow.Window.SlidingEventTest do
  use ExUnit.Case, async: true

  defp single_window do
    Flow.Window.sliding_event(2, 1)
  end

  describe "single window" do
    test "with multiple mappers and reducers" do
      assert Flow.from_enumerable(1..100, stages: 4, max_demand: 5)
             |> Flow.map(&(&1))
             |> Flow.partition(window: single_window(), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.sum() == 10099
    end
  end
end
