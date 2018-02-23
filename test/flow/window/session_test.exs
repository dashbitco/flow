defmodule Flow.Window.SessionTest do
  use ExUnit.Case, async: true

  defp single_window do
    Flow.Window.session(1, :second, fn x -> x end)
  end

  @tag :capture_log
  test "can't be departitioned" do
    assert catch_exit(
             Flow.from_enumerable(1..100, stages: 4, max_demand: 5)
             |> Flow.partition(window: single_window(), stages: 4, key: fn _ -> 0 end)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.departition(fn -> 0 end, &(&1 + &2), & &1)
             |> Enum.to_list()
           )
  end

  describe "single window" do
    test "with multiple mappers and reducers" do
      assert Flow.from_enumerable(1..100, stages: 4, max_demand: 5)
             |> Flow.map(& &1)
             |> Flow.partition(window: single_window(), stages: 4, key: fn _ -> 0 end)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.sum() == 5050
    end

    test "trigger keep with large demand" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_every(10),
        stages: 1,
        key: fn _ -> 0 end
      ]

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
        max_demand: 5,
        key: fn _ -> 0 end
      ]

      assert Flow.from_enumerable(1..100)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]
    end

    test "trigger discard with large demand" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_every(10, :reset),
        stages: 1,
        key: fn _ -> 0 end
      ]

      assert Flow.from_enumerable(1..100)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]
    end

    test "trigger discard with small demand" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_every(10, :reset),
        stages: 1,
        max_demand: 5,
        key: fn _ -> 0 end
      ]

      assert Flow.from_enumerable(1..100)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]
    end

    test "trigger ordering" do
      window =
        Flow.Window.trigger(single_window(), fn -> true end, fn events, true ->
          {:cont, Enum.all?(events, &(rem(&1, 2) == 0))}
        end)

      assert Flow.from_enumerable(1..10)
             |> Flow.partition(window: window, stages: 1, key: fn _ -> 0 end)
             |> Flow.map(&(&1 + 1))
             |> Flow.map(&(&1 * 2))
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.sort() == [130]
    end

    test "trigger names" do
      partition_opts = [
        window: single_window() |> Flow.Window.trigger_every(10, :reset),
        stages: 1,
        key: fn _ -> 0 end
      ]

      result =
        Flow.from_enumerable(1..100)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.map_state(fn state, _, {:session, {0, 1, _}, trigger} -> {trigger, state} end)
        |> Flow.emit(:state)
        |> Enum.sort()

      assert result == [
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
        max_demand: 10,
        key: fn _ -> 0 end
      ]

      assert Stream.concat(1..10, Stream.timer(:infinity))
             |> Flow.from_enumerable(max_demand: 5, stages: 2)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.map_state(&(&1 * 2))
             |> Flow.emit(:state)
             |> Enum.take(1) == [110]
    end

    test "trigger based on timers" do
      partition_opts = [stages: 1, max_demand: 10, window: single_window(), key: fn _ -> 0 end]

      reduce_fun = fn ->
        Process.send_after(self(), {:trigger, :reset, :sample}, 200)
        0
      end

      assert Stream.concat(1..10, Stream.timer(:infinity))
             |> Flow.from_enumerable(max_demand: 5, stages: 2)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(reduce_fun, &(&1 + &2))
             |> Flow.map_state(&{&1 * 2, &2, &3})
             |> Flow.emit(:state)
             |> Enum.take(1) == [{110, {0, 1}, {:session, {0, 1, 10}, :sample}}]
    end
  end

  defp double_ordered_window do
    Flow.Window.session(1, :second, fn
      x when x <= 50 -> 0 + x
      x when x <= 100 -> 1_000 + x
    end)
  end

  describe "double ordered windows with single key" do
    test "reduces per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_ordered_window(), stages: 1, key: fn _ -> 0 end)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [1275, 3775]
    end

    test "triggers per window with large demand" do
      partition_opts = [
        window: double_ordered_window() |> Flow.Window.trigger_every(12),
        stages: 1,
        key: fn _ -> 0 end
      ]

      result =
        Flow.from_enumerable(1..100, stages: 1)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.map_state(fn state, _, {:session, session, trigger} ->
          [{state, session, trigger}]
        end)
        |> Enum.to_list()

      assert result == [
               {78, {0, 1, 50}, {:every, 12}},
               {300, {0, 1, 50}, {:every, 12}},
               {666, {0, 1, 50}, {:every, 12}},
               {1176, {0, 1, 50}, {:every, 12}},
               {1275, {0, 1, 50}, :done},
               {678, {0, 1051, 1100}, {:every, 12}},
               {1500, {0, 1051, 1100}, {:every, 12}},
               {2466, {0, 1051, 1100}, {:every, 12}},
               {3576, {0, 1051, 1100}, {:every, 12}},
               {3775, {0, 1051, 1100}, :done}
             ]
    end

    test "reduces per window with small demand" do
      partition_opts = [
        window: double_ordered_window(),
        stages: 1,
        max_demand: 5,
        min_demand: 0,
        key: fn _ -> 0 end
      ]

      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [1275, 3775]
    end

    test "triggers per window with small demand" do
      partition_opts = [
        window: double_ordered_window() |> Flow.Window.trigger_every(12),
        stages: 1,
        max_demand: 5,
        min_demand: 0,
        key: fn _ -> 0 end
      ]

      result =
        Flow.from_enumerable(1..100, stages: 1)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.map_state(fn state, _, {:session, session, trigger} ->
          [{state, session, trigger}]
        end)
        |> Enum.to_list()

      assert result == [
               {78, {0, 1, 15}, {:every, 12}},
               {300, {0, 1, 25}, {:every, 12}},
               {666, {0, 1, 40}, {:every, 12}},
               {1176, {0, 1, 50}, {:every, 12}},
               {1275, {0, 1, 50}, :done},
               {678, {0, 1051, 1065}, {:every, 12}},
               {1500, {0, 1051, 1075}, {:every, 12}},
               {2466, {0, 1051, 1090}, {:every, 12}},
               {3576, {0, 1051, 1100}, {:every, 12}},
               {3775, {0, 1051, 1100}, :done}
             ]
    end

    test "triggers for all windows" do
      partition_opts = [
        window: double_ordered_window() |> Flow.Window.trigger_periodically(100, :millisecond),
        stages: 1,
        max_demand: 5,
        min_demand: 0,
        key: fn _ -> 0 end
      ]

      result =
        Stream.concat(1..100, Stream.timer(:infinity))
        |> Flow.from_enumerable(max_demand: 5, stages: 1)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.map_state(fn state, _, {:session, session, trigger} ->
          [{state, session, trigger}]
        end)
        |> Enum.take(2)

      assert result == [
               {1275, {0, 1, 50}, :done},
               {3775, {0, 1051, 1100}, {:periodically, 100, :millisecond}}
             ]
    end
  end

  describe "double ordered windows with multiple keys" do
    test "reduces per window with large demand" do
      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(window: double_ordered_window(), stages: 1, key: &rem(&1, 2))
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [625, 650, 1900, 1875]
    end

    test "triggers per window with large demand" do
      partition_opts = [
        window: double_ordered_window() |> Flow.Window.trigger_every(12),
        stages: 1,
        key: &rem(&1, 2)
      ]

      result =
        Flow.from_enumerable(1..100, stages: 1)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.map_state(fn state, _, {:session, session, trigger} ->
          [{state, session, trigger}]
        end)
        |> Enum.sort()

      assert result == [
               {144, {1, 1, 49}, {:every, 12}},
               {156, {0, 2, 50}, {:every, 12}},
               {576, {1, 1, 49}, {:every, 12}},
               {600, {0, 2, 50}, {:every, 12}},
               {625, {1, 1, 49}, :done},
               {650, {0, 2, 50}, :done},
               {744, {1, 1051, 1099}, {:every, 12}},
               {756, {0, 1052, 1100}, {:every, 12}},
               {1776, {1, 1051, 1099}, {:every, 12}},
               {1800, {0, 1052, 1100}, {:every, 12}},
               {1875, {1, 1051, 1099}, :done},
               {1900, {0, 1052, 1100}, :done}
             ]
    end

    test "reduces per window with small demand" do
      partition_opts = [
        window: double_ordered_window(),
        stages: 1,
        max_demand: 5,
        min_demand: 0,
        key: &rem(&1, 2)
      ]

      assert Flow.from_enumerable(1..100, stages: 1)
             |> Flow.partition(partition_opts)
             |> Flow.reduce(fn -> 0 end, &(&1 + &2))
             |> Flow.emit(:state)
             |> Enum.to_list() == [625, 650, 1900, 1875]
    end

    test "triggers per window with small demand" do
      partition_opts = [
        window: double_ordered_window() |> Flow.Window.trigger_every(12),
        stages: 1,
        max_demand: 5,
        min_demand: 0,
        key: &rem(&1, 2)
      ]

      result =
        Flow.from_enumerable(1..100, stages: 1)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.map_state(fn state, _, {:session, session, trigger} ->
          [{state, session, trigger}]
        end)
        |> Enum.sort()

      assert result == [
               {144, {1, 1, 25}, {:every, 12}},
               {156, {0, 2, 24}, {:every, 12}},
               {576, {1, 1, 49}, {:every, 12}},
               {600, {0, 2, 50}, {:every, 12}},
               {625, {1, 1, 49}, :done},
               {650, {0, 2, 50}, :done},
               {744, {1, 1051, 1075}, {:every, 12}},
               {756, {0, 1052, 1074}, {:every, 12}},
               {1776, {1, 1051, 1099}, {:every, 12}},
               {1800, {0, 1052, 1100}, {:every, 12}},
               {1875, {1, 1051, 1099}, :done},
               {1900, {0, 1052, 1100}, :done}
             ]
    end

    test "triggers for all windows" do
      partition_opts = [
        window: double_ordered_window() |> Flow.Window.trigger_periodically(100, :millisecond),
        stages: 1,
        max_demand: 5,
        min_demand: 0,
        key: &rem(&1, 2)
      ]

      result =
        Stream.concat(1..100, Stream.timer(:infinity))
        |> Flow.from_enumerable(max_demand: 5, stages: 1)
        |> Flow.partition(partition_opts)
        |> Flow.reduce(fn -> 0 end, &(&1 + &2))
        |> Flow.map_state(fn state, _, {:session, session, trigger} ->
          [{state, session, trigger}]
        end)
        |> Enum.take(4)

      assert result == [
               {625, {1, 1, 49}, :done},
               {650, {0, 2, 50}, :done},
               {1900, {0, 1052, 1100}, {:periodically, 100, :millisecond}},
               {1875, {1, 1051, 1099}, {:periodically, 100, :millisecond}}
             ]
    end
  end
end
