defmodule Flow.Window.CountTest do
  use ExUnit.Case, async: true

  defp single_window do
    Flow.Window.count(1000)
  end

  describe "single window" do
    test "with multiple mappers and reducers" do
      assert Flow.from_enumerable(1..100, stages: 4, max_demand: 5)
             |> Flow.map(& &1)
             |> Flow.partition(window: single_window(), stages: 4)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.sum() == 5050
    end

    test "trigger keep with large demand" do
      partition_opts = [window: single_window() |> Flow.Window.trigger_every(10), stages: 1]

      assert Flow.from_enumerable(1..100)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]
    end

    test "trigger keep with small demand" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_every(10),
        stages: 1,
        max_demand: 5
      ]

      assert Flow.from_enumerable(1..100)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]
    end

    test "trigger discard with large demand" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_every(10),
        stages: 1
      ]

      assert Flow.from_enumerable(1..100)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.on_trigger(&{[&1], 0})
             |> Enum.to_list() == [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]
    end

    test "trigger discard with small demand" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_every(10),
        stages: 1,
        max_demand: 5
      ]

      assert Flow.from_enumerable(1..100)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.on_trigger(&{[&1], 0})
             |> Enum.to_list() == [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]
    end

    test "trigger ordering" do
      window =
        Flow.Window.trigger(single_window(), fn -> true end, fn events, true ->
          {:cont, Enum.all?(events, &(rem(&1, 2) == 0))}
        end)

      assert Flow.from_enumerable(1..10)
             |> Flow.partition(window: window, stages: 1)
             |> Flow.map(&(&1 + 1))
             |> Flow.map(&(&1 * 2))
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.sort() == [130]
    end

    test "trigger names" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_every(10),
        stages: 1
      ]

      events =
        Flow.from_enumerable(1..100)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.on_trigger(fn state, _, {:count, 0, trigger} -> {[{trigger, state}], 0} end)
        |> Enum.sort()

      assert events == [
               {:done, 0},
               {{:every, 10}, 55},
               {{:every, 10}, 155},
               {{:every, 10}, 255},
               {{:every, 10}, 355},
               {{:every, 10}, 455},
               {{:every, 10}, 555},
               {{:every, 10}, 655},
               {{:every, 10}, 755},
               {{:every, 10}, 855},
               {{:every, 10}, 955}
             ]
    end

    test "trigger based on intervals" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_periodically(100, :millisecond),
        stages: 1,
        max_demand: 10
      ]

      assert Stream.concat(1..10, Stream.timer(60_000))
             |> Flow.from_enumerable(max_demand: 5, stages: 2)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.on_trigger(&{[&1 * 2], &1})
             |> Enum.take(1) == [110]
    end

    test "trigger based on timers" do
      reduce_fun = fn ->
        Process.send_after(self(), {:trigger, :sample}, 200)
        0
      end

      assert Stream.concat(1..10, Stream.timer(60_000))
             |> Flow.from_enumerable(max_demand: 5, stages: 2)
             |> Flow.partition(stages: 1, max_demand: 10, window: single_window())
             |> Flow.reduce(reduce_fun, &(&1 + &2))
             |> Flow.on_trigger(&{[{&1 * 2, &2, &3}], reduce_fun.()})
             |> Enum.take(1) == [{110, {0, 1}, {:count, 0, :sample}}]
    end
  end

  defp double_ordered_window do
    Flow.Window.count(50)
  end

  describe "double ordered windows" do
    test "reduces per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_ordered_window(), stages: 1)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [1275, 3775, 0]
    end

    test "triggers per window with large demand" do
      partition_opts = [
        window: double_ordered_window() |> Flow.Window.trigger_every(12),
        stages: 1
      ]

      events =
        Flow.from_enumerable(1..100, stages: 1)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.on_trigger(fn state, _, {:count, count, trigger} ->
          {[{state, count, trigger}], state}
        end)
        |> Enum.to_list()

      assert events == [
               {78, 0, {:every, 12}},
               {300, 0, {:every, 12}},
               {666, 0, {:every, 12}},
               {1176, 0, {:every, 12}},
               {1275, 0, :done},
               {678, 1, {:every, 12}},
               {1500, 1, {:every, 12}},
               {2466, 1, {:every, 12}},
               {3576, 1, {:every, 12}},
               {3775, 1, :done},
               {0, 2, :done}
             ]
    end

    test "reduces per window with small demand" do
      partition_opts = [window: double_ordered_window(), stages: 1, max_demand: 5, min_demand: 0]

      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [1275, 3775, 0]
    end

    test "triggers per window with small demand" do
      partition_opts = [
        window: double_ordered_window() |> Flow.Window.trigger_every(12),
        stages: 1,
        max_demand: 5,
        min_demand: 0
      ]

      events =
        Flow.from_enumerable(1..100, stages: 1)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.on_trigger(fn state, _, {:count, count, trigger} ->
          {[{state, count, trigger}], state}
        end)
        |> Enum.to_list()

      assert events == [
               {78, 0, {:every, 12}},
               {300, 0, {:every, 12}},
               {666, 0, {:every, 12}},
               {1176, 0, {:every, 12}},
               {1275, 0, :done},
               {678, 1, {:every, 12}},
               {1500, 1, {:every, 12}},
               {2466, 1, {:every, 12}},
               {3576, 1, {:every, 12}},
               {3775, 1, :done},
               {0, 2, :done}
             ]
    end
  end
end
