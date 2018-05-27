defmodule Flow.Window.GlobalTest do
  use ExUnit.Case, async: true

  test "trigger keep with large demand" do
    partition_opts = [window: Flow.Window.global() |> Flow.Window.trigger_every(10), stages: 1]

    assert Flow.from_enumerable(1..100)
           |> Flow.partition(partition_opts)
           |> Flow.reduce(fn -> 0 end, &(&1 + &2))
           |> Flow.emit(:state)
           |> Enum.to_list() == [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]
  end

  test "trigger keep with small demand" do
    partition_opts = [
      window: Flow.Window.global() |> Flow.Window.trigger_every(10),
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
      window: Flow.Window.global() |> Flow.Window.trigger_every(10),
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
      window: Flow.Window.global() |> Flow.Window.trigger_every(10),
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
      Flow.Window.trigger(Flow.Window.global(), fn -> true end, fn events, true ->
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
      window: Flow.Window.global() |> Flow.Window.trigger_every(10),
      stages: 1
    ]

    events =
      Flow.from_enumerable(1..100)
      |> Flow.partition(partition_opts)
      |> Flow.reduce(fn -> 0 end, &(&1 + &2))
      |> Flow.on_trigger(fn state, _, {:global, :global, trigger} -> {[{trigger, state}], 0} end)
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
      window: Flow.Window.global() |> Flow.Window.trigger_periodically(100, :millisecond),
      stages: 1,
      max_demand: 10
    ]

    assert Flow.from_enumerable(Stream.concat(1..10, Stream.timer(:infinity)), max_demand: 5)
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

    assert Stream.concat(1..10, Stream.timer(:infinity))
           |> Flow.from_enumerable(max_demand: 5, stages: 2)
           |> Flow.partition(stages: 1, max_demand: 10)
           |> Flow.reduce(reduce_fun, &(&1 + &2))
           |> Flow.on_trigger(&{[{&1 * 2, &2, &3}], reduce_fun.()})
           |> Enum.take(1) == [{110, {0, 1}, {:global, :global, :sample}}]
  end
end
