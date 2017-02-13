defmodule Flow.Window.Test do
  use ExUnit.Case, async: true
  doctest Flow.Window

  test "periodic triggers" do
    assert Flow.Window.global
           |> Flow.Window.trigger_periodically(10, :second, :keep)
           |> Map.fetch!(:periodically) ==
           [{10000, :keep, {:periodically, 10, :second}}]

    assert Flow.Window.global
           |> Flow.Window.trigger_periodically(10, :minute, :keep)
           |> Map.fetch!(:periodically) ==
           [{600000, :keep, {:periodically, 10, :minute}}]

    assert Flow.Window.global
           |> Flow.Window.trigger_periodically(10, :hour, :keep)
           |> Map.fetch!(:periodically) ==
           [{36000000, :keep, {:periodically, 10, :hour}}]
  end

  describe "custom trigger w/ :cont, emitted events and no emitted events" do
    setup do
      flow = fn(window, parent) ->
        Flow.from_enumerables([[:a, :b, :c], [:a, :b, :c]], stages: 1)
        |> Flow.partition(window: window, stages: 1)
        |> Flow.reduce(fn -> [] end, & [&1 | &2])
        |> Flow.each_state(&send(parent, Enum.sort(&1)))
        |> Flow.start_link
      end

      moving_event_trigger = fn(window, count, emitted, keep_or_reset) ->
        name = {:moving_event_trigger, count}
        Flow.Window.trigger(window, fn -> [] end, fn events, acc ->
          new_acc = acc ++ events

          if length(new_acc) >= count do
            pre = Enum.take(new_acc, count)
            pos = Enum.drop(new_acc, 1)
            {:trigger, name, pre, keep_or_reset, pos, []}
          else
            {:cont, emitted, new_acc}
          end
        end)
      end

      [flow: flow, trigger: moving_event_trigger]
    end

    @tag :custom_trigger
    test "skip on :cont w/ reset", context do

      window = Flow.Window.global |> context[:trigger].(2, [], :reset)
      {:ok, pid} = context[:flow].(window, self())

      assert_receive [:a, :b]
      assert_receive [:b, :c]
      assert_receive [:a, :c]
      assert_receive [:a, :b]
      assert_receive [:b, :c]
      refute_received [:a]
      refute_received [:c]

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, _}
    end

    @tag :custom_trigger
    test "emit on :cont w/ reset", context do

      window = Flow.Window.global |> context[:trigger].(2, [:elixir], :reset)
      {:ok, pid} = context[:flow].(window, self())

      assert_receive [:a, :b]
      assert_receive [:b, :c]
      assert_receive [:a, :c, :elixir]
      assert_receive [:a, :b]
      assert_receive [:b, :c]
      refute_received [:a, :c]
      refute_received [:a]
      refute_received [:c]

      ref = Process.monitor(pid)
      assert_receive {:DOWN, ^ref, _, _, _}
    end

  end

end

