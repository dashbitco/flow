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
end
