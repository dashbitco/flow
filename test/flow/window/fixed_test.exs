defmodule Flow.Window.FixedTest do
  use ExUnit.Case, async: true

  defp single_window do
    Flow.Window.fixed(1, :second, fn _ -> 0 end)
  end

  describe "single window" do
    test "with multiple mappers and reducers" do
      assert Flow.from_enumerable(1..100, stages: 4, max_demand: 5)
             |> Flow.map(&(&1))
             |> Flow.partition(window: single_window(), stages: 4)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.sum() == 5050
    end

    test "trigger keep with large demand" do
      assert Flow.from_enumerable(1..100)
             |> Flow.partition(window: single_window() |> Flow.Window.trigger_every(10), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]
    end

    test "trigger keep with small demand" do
      assert Flow.from_enumerable(1..100)
             |> Flow.partition(window: single_window() |> Flow.Window.trigger_every(10), stages: 1, max_demand: 5)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]
    end

    test "trigger discard with large demand" do
      assert Flow.from_enumerable(1..100)
             |> Flow.partition(window: single_window() |> Flow.Window.trigger_every(10, :reset), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]
    end

    test "trigger discard with small demand" do
      assert Flow.from_enumerable(1..100)
             |> Flow.partition(window: single_window() |> Flow.Window.trigger_every(10, :reset), stages: 1, max_demand: 5)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]
    end

    test "trigger ordering" do
      window =
        Flow.Window.trigger(single_window(), fn -> true end, fn events, true ->
          {:cont, Enum.all?(events, &rem(&1, 2) == 0)}
        end)

      assert Flow.from_enumerable(1..10)
             |> Flow.partition(window: window, stages: 1)
             |> Flow.map(& &1 + 1)
             |> Flow.map(& &1 * 2)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.sort() == [130]
    end

    test "trigger names" do
      assert Flow.from_enumerable(1..100)
             |> Flow.partition(window: single_window() |> Flow.Window.trigger_every(10, :reset), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, 0, trigger} -> {trigger, state} end)
             |> Flow.emit(:state)
             |> Enum.sort() == [{:done, 0},
                                {{:every, 10}, 55}, {{:every, 10}, 155},
                                {{:every, 10}, 255}, {{:every, 10}, 355},
                                {{:every, 10}, 455}, {{:every, 10}, 555},
                                {{:every, 10}, 655}, {{:every, 10}, 755},
                                {{:every, 10}, 855}, {{:every, 10}, 955}]
    end

    test "trigger based on intervals" do
      assert Flow.from_enumerable(Stream.concat(1..10, Stream.timer(:infinity)), max_demand: 5, stages: 2)
             |> Flow.partition(window: single_window() |> Flow.Window.trigger_periodically(100, :millisecond),
                               stages: 1, max_demand: 10)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(& &1 * 2)
             |> Flow.emit(:state)
             |> Enum.take(1) == [110]
    end

    test "trigger based on timers" do
      assert Flow.from_enumerable(Stream.concat(1..10, Stream.timer(:infinity)), max_demand: 5, stages: 2)
             |> Flow.partition(stages: 1, max_demand: 10, window: single_window())
             |> Flow.reduce(fn ->
                  Process.send_after(self(), {:trigger, :reset, :sample}, 200)
                  0
                end, & &1 + &2)
             |> Flow.map_state(&{&1 * 2, &2, &3})
             |> Flow.emit(:state)
             |> Enum.take(1) == [{110, {0, 1}, {:fixed, 0, :sample}}]
    end
  end

  defp double_ordered_window do
    Flow.Window.fixed(1, :second, fn
      x when x <= 50 -> 0 + x
      x when x <= 100 -> 1_000 + x
    end)
  end

  describe "double ordered windows" do
    test "reduces per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_ordered_window(), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [1275, 3775]
    end

    test "triggers per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_ordered_window() |> Flow.Window.trigger_every(12), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.to_list() == [{78, 0, {:every, 12}},
                                   {300, 0, {:every, 12}},
                                   {666, 0, {:every, 12}},
                                   {1176, 0, {:every, 12}},
                                   {678, 1000, {:every, 12}},
                                   {1500, 1000, {:every, 12}},
                                   {2466, 1000, {:every, 12}},
                                   {3576, 1000, {:every, 12}},
                                   {1275, 0, :done},
                                   {3775, 1000, :done}]
    end

    test "reduces per window with small demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_ordered_window(), stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [1275, 3775]
    end

    test "triggers per window with small demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_ordered_window() |> Flow.Window.trigger_every(12),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.to_list() == [{78, 0, {:every, 12}},
                                   {300, 0, {:every, 12}},
                                   {666, 0, {:every, 12}},
                                   {1176, 0, {:every, 12}},
                                   {1275, 0, :done},
                                   {678, 1000, {:every, 12}},
                                   {1500, 1000, {:every, 12}},
                                   {2466, 1000, {:every, 12}},
                                   {3576, 1000, {:every, 12}},
                                   {3775, 1000, :done}]
    end

    test "triggers for all windows" do
      assert Flow.from_enumerable(Stream.concat(1..100, Stream.timer(:infinity)), max_demand: 5, stages: 1)
             |> Flow.partition(window: double_ordered_window() |> Flow.Window.trigger_periodically(100, :millisecond),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.take(2) == [{1275, 0, :done},
                                 {3775, 1000, {:periodically, 100, :millisecond}}]
    end
  end

  defp double_unordered_window_without_lateness do
    Flow.Window.fixed(1, :second, fn
      x when x <= 40 -> 0
      x when x <= 80 -> 2_000
      x when x <= 100 -> 0 # Those events will be lost
    end)
  end

  # With one stage, termination happens when one stage is done.
  describe "double unordered windows without lateness with one stage" do
    test "reduces per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_without_lateness(), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [2630, 0, 2420]
    end

    test "triggers per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_without_lateness() |> Flow.Window.trigger_every(12), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.to_list() == [{78, 0, {:every, 12}},
                                   {300, 0, {:every, 12}},
                                   {666, 0, {:every, 12}},
                                   {558, 2000, {:every, 12}},
                                   {1260, 2000, {:every, 12}},
                                   {2106, 2000, {:every, 12}},
                                   {1496, 0, {:every, 12}},
                                   {2630, 0, {:every, 12}},
                                   {2630, 0, :done},
                                   {0, 1000, :done},
                                   {2420, 2000, :done}]
    end

    test "reduces per window with small demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_without_lateness(),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [820, 0, 2420]
    end

    test "triggers per window with small demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_without_lateness() |> Flow.Window.trigger_every(12),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.to_list() == [{78, 0, {:every, 12}},
                                   {300, 0, {:every, 12}},
                                   {666, 0, {:every, 12}},
                                   {820, 0, :done},
                                   {0, 1000, :done},
                                   {558, 2000, {:every, 12}},
                                   {1260, 2000, {:every, 12}},
                                   {2106, 2000, {:every, 12}},
                                   {2420, 2000, :done}]
    end

    test "triggers for all windows" do
      assert Flow.from_enumerable(Stream.concat(1..100, Stream.timer(:infinity)), max_demand: 5, stages: 1)
             |> Flow.partition(window: double_unordered_window_without_lateness() |> Flow.Window.trigger_periodically(100, :millisecond),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.take(3) == [{820, 0, :done},
                                 {0, 1000, :done},
                                 {2420, 2000, {:periodically, 100, :millisecond}}]
    end
  end

  # With two stages, termination is only guaranteed once both stages are done.
  describe "double unordered windows without lateness with two stages" do
    test "reduces per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 2)
             |> Flow.partition(window: double_unordered_window_without_lateness(), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [2630, 0, 2420]
    end

    test "triggers per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 2)
             |> Flow.partition(window: double_unordered_window_without_lateness() |> Flow.Window.trigger_every(12),
                               stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.to_list() == [{78, 0, {:every, 12}},
                                   {300, 0, {:every, 12}},
                                   {666, 0, {:every, 12}},
                                   {558, 2000, {:every, 12}},
                                   {1260, 2000, {:every, 12}},
                                   {2106, 2000, {:every, 12}},
                                   {1496, 0, {:every, 12}},
                                   {2630, 0, {:every, 12}},
                                   {2630, 0, :done},
                                   {0, 1000, :done},
                                   {2420, 2000, :done}]
    end

    test "reduces per window with small demand" do
      # We were not suppose to receive all data but,
      # because we have two stages, we are only done
      # once both stages are done, so we end-up consuming
      # late events while the other producer is open.
      assert Flow.from_enumerable(1..100, stages: 2)
             |> Flow.map(& &1)
             |> Flow.partition(window: double_unordered_window_without_lateness(),
                               stages: 1, max_demand: 100)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [2630, 0, 2420]
    end

    test "triggers per window with small demand" do
      assert Flow.from_enumerable(1..100, stages: 2)
             |> Flow.map(& &1)
             |> Flow.partition(window: double_unordered_window_without_lateness() |> Flow.Window.trigger_every(12),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.to_list() == [{78, 0, {:every, 12}},
                                   {300, 0, {:every, 12}},
                                   {666, 0, {:every, 12}},
                                   {558, 2000, {:every, 12}},
                                   {1260, 2000, {:every, 12}},
                                   {2106, 2000, {:every, 12}},
                                   {1496, 0, {:every, 12}},
                                   {2630, 0, {:every, 12}},
                                   {2630, 0, :done},
                                   {0, 1000, :done},
                                   {2420, 2000, :done}]
    end
  end

  defp double_unordered_window_with_lateness(keep_or_reset \\ :keep) do
    Flow.Window.fixed(1, :second, fn
      x when x <= 40 -> 0
      x when x <= 80 -> 2_000
      x when x <= 100 -> 0 # Those events won't be lost due to lateness
    end) |> Flow.Window.allowed_lateness(1, :hour, keep_or_reset)
  end

  # With one stage, termination happens when one stage is done.
  describe "double unordered windows with lateness with one stage" do
    test "reduces per window with large demand and keep buffer" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_with_lateness(:keep), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [2630, 0, 2630, 0, 2420]
    end

  test "reduces per window with large demand and reset buffer" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_with_lateness(:reset), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() ==  [2630, 0, 0, 0, 2420]
    end

    test "triggers per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_with_lateness() |> Flow.Window.trigger_every(12), stages: 1)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.to_list() == [{78, 0, {:every, 12}},
                                   {300, 0, {:every, 12}},
                                   {666, 0, {:every, 12}},
                                   {558, 2000, {:every, 12}},
                                   {1260, 2000, {:every, 12}},
                                   {2106, 2000, {:every, 12}},
                                   {1496, 0, {:every, 12}},
                                   {2630, 0, {:every, 12}},
                                   {2630, 0, :watermark},
                                   {0, 1000, :watermark},
                                   {2630, 0, :done},
                                   {0, 1000, :done},
                                   {2420, 2000, :done}]
    end

    test "reduces per window with small demand and keep buffer" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_with_lateness(),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [820, 0, 2630, 0, 2420]
    end

    test "reduces per window with small demand and reset buffer" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_with_lateness(:reset),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.emit(:state)
             |> Enum.to_list() == [820, 0, 1810, 0, 2420]
    end

    test "triggers per window with small demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_unordered_window_with_lateness() |> Flow.Window.trigger_every(12),
                               stages: 1, max_demand: 5, min_demand: 0)
             |> Flow.reduce(fn -> 0 end, & &1 + &2)
             |> Flow.map_state(fn state, _, {:fixed, fixed, trigger} -> [{state, fixed, trigger}] end)
             |> Enum.to_list() == [{78, 0, {:every, 12}},
                                   {300, 0, {:every, 12}},
                                   {666, 0, {:every, 12}},
                                   {820, 0, :watermark},
                                   {0, 1000, :watermark},
                                   {558, 2000, {:every, 12}},
                                   {1260, 2000, {:every, 12}},
                                   {2106, 2000, {:every, 12}},
                                   {1496, 0, {:every, 12}},
                                   {2630, 0, {:every, 12}},
                                   {2630, 0, :done},
                                   {0, 1000, :done},
                                   {2420, 2000, :done}]
    end
  end
end
