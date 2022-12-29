defmodule Flow.Window do
  @moduledoc """
  Splits a flow into windows that are materialized at certain triggers.

  Windows allow developers to split data so we can understand incoming
  data as time progresses. Once a window is created, we can specify
  triggers that allow us to customize when the data accumulated on every
  window is materialized.

  Windows must be created by calling one of the window type functions.
  The supported window types are as follows:

    * Global windows - that's the default window which means all data
      belongs to one single window. In other words, the data is not
      split in any way. The window finishes when all producers notify
      there is no more data

    * Fixed windows - splits incoming events into periodic, non-
      overlapping windows based on event times. In other words, a given
      event belongs to a single window. If data arrives late, a configured
      lateness can be specified.

    * Periodic windows - splits incoming events into periodic, non-
      overlapping windows based on processing times. Similar to fixed
      windows, a given event belongs to a single window.

    * Count windows - splits incoming events based on a count.
      Similar to fixed windows, a given event belongs to a single
      window.

  Other common window types can be expressed with Flow functions:

    * Session windows - splits incoming events into unique windows
      which is grouped until there is a configured gap between event
      times. Sessions are useful for data that is irregularly
      distributed with respect to time.

  We discuss all types and include examples below. In the first section,
  "Global windows", we build the basic intuition about windows and triggers
  as well as discuss the distinction between "Event time and processing time".
  Then we explore "Fixed windows" and the concept of lateness before moving
  on to other window types.

  ## Global windows

  By default, all events belong to the global window. The global window
  is automatically attached to a partition if no window is specified.
  The flow below:

      Flow.from_stages([some_producer])
      |> Flow.partition()
      |> Flow.reduce(fn -> 0 end, & &1 + 2)

  is equivalent to:

      Flow.from_stages([some_producer])
      |> Flow.partition(window: Flow.Window.global())
      |> Flow.reduce(fn -> 0 end, & &1 + 2)

  Even though the global window does not split the data in any way, it
  already provides conveniences for working with both bounded (finite)
  and unbounded (infinite) via triggers.

  For example, the flow below uses a global window with a count-based
  trigger to emit the values being summed as we sum them:

      iex> window = Flow.Window.global() |> Flow.Window.trigger_every(10)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
      iex> flow |> Flow.reduce(fn -> 0 end, &(&1 + &2)) |> Flow.emit(:state) |> Enum.to_list()
      [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]

  Let's explore the types of triggers available next.

  ### Triggers

  Triggers allow us to check point the data processed so far. There
  are different triggers we can use:

    * Event count triggers - compute state operations every X events

    * Processing time triggers - compute state operations every X time
      units for every stage

    * Punctuation - hand-written triggers based on the data

  Flow supports the triggers above via the `trigger_every/2`,
  `trigger_periodically/3` and `trigger/3` respectively.

  Once a trigger is emitted, the `Flow.reduce/3` step halts and invokes
  the `Flow.on_trigger/2` callback, allowing you to emit events and change
  the reducer accumulator.

  ### Event time and processing time

  Before we move to other window types, it is important to discuss
  the distinction between event time and processing time. In particular,
  triggers created with the `trigger_periodically/3` function are
  intrinsically inaccurate and therefore should not be used to split the
  data. For example, if you are measuring the frequency that events arrive,
  using the event time will always yield the same result, while processing
  time will be vulnerable to fluctuations if, for instance, an external
  factor causes events to processed slower or faster than usual.

  Furthermore, periodic triggers are established per partition and are
  message-based, which means partitions will emit the triggers at different
  times and possibly with delays based on the partition message queue size.
  However, it is exactly this lack of precision which makes them efficient
  for checkpointing data.

  Flow provides other window types, such as fixed windows, exactly to address
  the issues with processing time. Such windows use the event time which is
  based on the data itself. When working with event time, we can assign the
  data into proper windows even when late or out of order. Such windows can
  be used to gather time-based insight from the data (for example, the most
  popular hashtags in the last 10 minutes) as well as for checkpointing data.

  ## Fixed windows (event time)

  Fixed windows group the data based on the event times. Regardless if
  the data is bounded or not, fixed windows give us time-based insight
  about the data.

  Fixed windows are created via the `fixed/3` function which specified
  the duration of the window and a function that retrieves the event time
  from each event:

      Flow.Window.fixed(1, :hour, fn {word, timestamp} -> timestamp end)

  Let's see an example that will use the window above to count the frequency
  of words based on windows that are 1 hour long. The timestamps used by
  Flow are integers in milliseconds. For now, we will also set the concurrency
  down 1 and max demand down to 5 as it is simpler to reason about the results:

      iex> data = [{"elixir", 0}, {"elixir", 1_000}, {"erlang", 60_000},
      ...>         {"concurrency", 3_200_000}, {"elixir", 4_000_000},
      ...>         {"erlang", 5_000_000}, {"erlang", 6_000_000}]
      iex> window = Flow.Window.fixed(1, :hour, fn {_word, timestamp} -> timestamp end)
      iex> flow = Flow.from_enumerable(data, max_demand: 5, stages: 1)
      iex> flow = Flow.partition(flow, window: window, stages: 1)
      iex> flow = Flow.reduce(flow, fn -> %{} end, fn {word, _}, acc ->
      ...>   Map.update(acc, word, 1, & &1 + 1)
      ...> end)
      iex> flow |> Flow.emit(:state) |> Enum.to_list
      [%{"elixir" => 2, "erlang" => 1, "concurrency" => 1},
       %{"elixir" => 1, "erlang" => 2}]

  Since the data has been broken in two windows, the first four events belong
  to the same window while the last 3 belongs to the second one. Notice that
  `Flow.reduce/3` is executed per window and that each event belongs to a single
  window exclusively.

  Similar to global windows, fixed windows can also have triggers, allowing
  us to checkpoint the data as the computation happens.

  ### Data ordering, watermarks and lateness

  When working with event time, Flow assumes by default that events are time
  ordered. This means that, when we move from one window to another, like
  when we received the entry `{"elixir", 4_000_000}` in the example above,
  we assume the previous window has been completed.

  Let's change the events above to be out of order and move the first event
  to the end of the dataset and see what happens:

      iex> data = [{"elixir", 1_000}, {"erlang", 60_000},
      ...>         {"concurrency", 3_200_000}, {"elixir", 4_000_000},
      ...>         {"erlang", 5_000_000}, {"erlang", 6_000_000}, {"elixir", 0}]
      iex> window = Flow.Window.fixed(1, :hour, fn {_word, timestamp} -> timestamp end)
      iex> flow = Flow.from_enumerable(data) |> Flow.partition(window: window, stages: 1, max_demand: 5)
      iex> flow = Flow.reduce(flow, fn -> %{} end, fn {word, _}, acc ->
      ...>   Map.update(acc, word, 1, & &1 + 1)
      ...> end)
      iex> flow |> Flow.emit(:state) |> Enum.to_list
      [%{"elixir" => 1, "erlang" => 1, "concurrency" => 1},
       %{"elixir" => 1, "erlang" => 2}]

  Notice that now the first map did not count the "elixir" word twice.
  Since the event arrived late, it was marked as lost. However, in many
  flows we actually expect data to arrive late or out of order, especially
  when talking about concurrent data processing.

  Luckily, event time windows include the concept of lateness, which is a
  processing time base period we would wait to receive late events.
  Let's change the example above once more but now change the window
  to also call `allowed_lateness/3`:

      iex> data = [{"elixir", 1_000}, {"erlang", 60_000},
      ...>         {"concurrency", 3_200_000}, {"elixir", 4_000_000},
      ...>         {"erlang", 5_000_000}, {"erlang", 6_000_000}, {"elixir", 0}]
      iex> window = Flow.Window.fixed(1, :hour, fn {_word, timestamp} -> timestamp end)
      iex> window = Flow.Window.allowed_lateness(window, 5, :minute)
      iex> flow = Flow.from_enumerable(data) |> Flow.partition(window: window, stages: 1, max_demand: 5)
      iex> flow = Flow.reduce(flow, fn -> %{} end, fn {word, _}, acc ->
      ...>   Map.update(acc, word, 1, & &1 + 1)
      ...> end)
      iex> flow |> Flow.emit(:state) |> Enum.to_list
      [%{"concurrency" => 1, "elixir" => 1, "erlang" => 1},
       %{"concurrency" => 1, "elixir" => 2, "erlang" => 1},
       %{"elixir" => 1, "erlang" => 2}]

  Now that we allow late events, we can see the first window emitted
  twice. Instead of the window being marked as done when 1 hour passes,
  we say it emits a **watermark trigger**. The window will be effectively
  done only after the allowed lateness period. If desired, we can use
  `Flow.on_trigger/2` to get more information about each particular window
  and its trigger. Replace the last line above by the following:

      flow
      |> Flow.on_trigger(fn state, _index, trigger -> {[{state, trigger}], state} end)
      |> Enum.to_list()

  The trigger parameter will include the type of window, the current
  window and what caused the window to be emitted (`:watermark` or
  `:done`).

  Note that all stages must receive an event that is outside of a specific
  window before that window is considered complete. In other words if there are
  multiple stages in the partition preceding a reduce operation that has
  a window, the reduce step won't release a window until it has seen an event
  that is outside of that window from all processes that it receives data from.
  This could have an effect on how long events are delayed in the reduce step.

  ## Periodic windows (processing time)

  Periodic windows are similar to fixed windows except triggers are
  emitted based on processing time instead of event time. Remember that
  relying on periodic windows or triggers is intrinsically inaccurate and
  should not be used to split the data, only as a checkpointing device.

  Periodic windows are also similar to global windows that use
  `trigger_periodically/2` to emit events periodically. The difference is
  that periodic windows emit a window in a given interval while a trigger
  emits a trigger. This behaviour may affect functions such as `Flow.departition/4`,
  which calls the `merge` callback per trigger but the `done` callback per
  window. Unless you are relying on functions such as `Flow.departition/4`,
  there is no distinction between periodic windows and global windows with
  periodic triggers.

  ## Count windows (event count)

  Count windows are simpler versions of fixed windows where windows are split
  apart by event count. Since it is not timed-based, it does not provide the
  concept of lateness.

      iex> window = Flow.Window.count(10)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
      iex> flow |> Flow.reduce(fn -> 0 end, &(&1 + &2)) |> Flow.emit(:state) |> Enum.to_list()
      [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]

  Count windows are also similar to global windows that use `trigger_every/2`
  to emit events per count. The difference is that count windows emit a
  window per event count while a trigger belongs to a window. This behaviour
  may affect functions such as `Flow.departition/4`, which calls the `merge`
  callback per trigger but the `done` callback per window.  Unless you are
  relying on functions such as `Flow.departition/4`, there is no distinction
  between count windows and global windows with count triggers.

  ## Session windows (gap between events)

  Session windows allow events to accumulate until a configured time gap
  between events occurs. This allows for grouping events that occurred close to
  each other, while allowing the length of the window to vary. Flow does not
  provide a dedicated Session window type, but it can be constructed using
  `emit_and_reduce/3` and `on_trigger/2`.

      iex> data = [
      ...>   {"elixir", 2_000_000},
      ...>   {"erlang", 3_100_000},
      ...>   {"elixir", 3_200_000},
      ...>   {"erlang", 4_000_000},
      ...>   {"elixir", 4_100_000},
      ...>   {"erlang", 4_150_000}
      ...> ]
      iex> max_gap_between_events = 1_000_000
      iex> flow = Flow.from_enumerable(data) |> Flow.partition(key: fn {k, _} -> k end, stages: 1)
      iex> flow =
      ...>   Flow.emit_and_reduce(flow, fn -> %{} end, fn {word, time}, acc ->
      ...>     {count, previous_time} = Map.get(acc, word, {1, time})
      ...>
      ...>     if time - previous_time > max_gap_between_events do
      ...>       {[{word, {count, previous_time}}], Map.put(acc, word, {1, time})}
      ...>     else
      ...>       {[], Map.update(acc, word, {1, time}, fn {count, _} -> {count + 1, time} end)}
      ...>     end
      ...>   end)
      iex> flow = Flow.on_trigger(flow, fn acc -> {Enum.to_list(acc), :unused} end)
      iex> Enum.to_list(flow)
      [{"elixir", {1, 2000000}}, {"elixir", {2, 4100000}}, {"erlang", {3, 4150000}}]
  """

  @type t :: %{
          required(:trigger) => {fun(), fun()} | nil,
          required(:periodically) => [trigger],
          optional(atom()) => term()
        }

  @typedoc "The supported window types."
  @type type :: :global | :fixed | :periodic | :count | any()

  @typedoc """
  A function that returns the event time to window by.

  It must return an integer representing the time in milliseconds.
  Flow does not care if the integer is using the UNIX epoch,
  Gregorian epoch or any other as long as it is consistent.
  """
  @type by :: (term -> non_neg_integer)

  @typedoc """
  The window identifier.

  It is `:global` for `:global` windows or an integer for fixed windows.
  """
  @type id :: :global | non_neg_integer()

  @typedoc """
  The supported time units for fixed and periodic windows.
  """
  @type time_unit :: :millisecond | :second | :minute | :hour

  @typedoc "The name of the trigger."
  @type trigger :: term

  @doc """
  Returns a global window.

  Global window triggers have the shape of `{:global, :global, trigger_name}`.

  See the section on "Global windows" in the module documentation for examples.
  """
  @spec global :: t
  def global do
    %Flow.Window.Global{}
  end

  @doc """
  Returns a count-based window of every `count` elements.

  `count` must be a positive integer.

  Count window triggers have the shape of `{:count, window, trigger_name}`,
  where `window` is an incrementing integer identifying the window.

  See the section on "Count windows" in the module documentation for examples.
  """
  @spec count(pos_integer) :: t
  def count(count) when is_integer(count) and count > 0 do
    %Flow.Window.Count{count: count}
  end

  @doc """
  Returns a period-based window of every `count` `unit`.

  `count` is a positive integer and `unit` is one of `:millisecond`,
  `:second`, `:minute`, or `:hour`. Remember periodic triggers are established
  per partition and are message-based, which means partitions will emit the
  triggers at different times and possibly with delays based on the partition
  message queue size.

  Periodic window triggers have the shape of `{:periodic, window, trigger_name}`,
  where `window` is an incrementing integer identifying the window.

  See the section on "Periodic windows" in the module documentation for examples.
  """
  @spec periodic(pos_integer, time_unit) :: t
  def periodic(count, unit) when is_integer(count) and count > 0 do
    %Flow.Window.Periodic{duration: to_ms(count, unit)}
  end

  @doc """
  Returns a fixed window of duration `count` `unit` where the
  event time is calculated by the given function `by`.

  `count` is a positive integer and `unit` is one of `:millisecond`,
  `:second`, `:minute`, or `:hour`.

  Fixed window triggers have the shape of `{:fixed, window, trigger_name}`,
  where `window` is an integer that represents the beginning timestamp
  for the current window.

  If `allowed_lateness/3` is used with fixed windows, the window will
  first emit a `{:fixed, window, :watermark}` trigger when the window
  terminates and emit `{:fixed, window, :done}` only after the
  `allowed_lateness/3` duration has passed.

  See the section on "Fixed windows" in the module documentation for examples.
  """
  @spec fixed(pos_integer, time_unit, (t -> pos_integer)) :: t
  def fixed(count, unit, by) when is_integer(count) and count > 0 and is_function(by, 1) do
    %Flow.Window.Fixed{duration: to_ms(count, unit), by: by}
  end

  @doc """
  Sets a duration, in processing time, of how long we will
  wait for late events for a given window.

  If allowed lateness is configured, once the window is finished,
  it won't trigger a `:done` event but instead emit a `:watermark`.
  The window will be done only when the allowed lateness time expires,
  effectively emitting the `:done` trigger.

  `count` is a positive number. The `unit` may be a time unit
  (`:millisecond`, `:second`, `:minute`, or `:hour`).
  """
  @spec allowed_lateness(t, pos_integer, time_unit) :: t
  def allowed_lateness(window, count, unit)

  def allowed_lateness(%{lateness: _} = window, count, unit) do
    %{window | lateness: to_ms(count, unit)}
  end

  def allowed_lateness(window, _, _) do
    raise ArgumentError, "allowed_lateness/3 not supported for window type #{inspect(window)}"
  end

  @doc """
  Calculates when to emit a trigger.

  Triggers are calculated per window and are used to temporarily
  halt the window accumulation, typically done with `Flow.reduce/3`,
  allowing the next operations to execute before accumulation is
  resumed.

  This function expects the trigger accumulator function, which will
  be invoked at the beginning of every window, and a trigger function
  that receives the current batch of events and its own accumulator.
  The trigger function must return one of the three values:

    * `{:cont, acc}` - the reduce operation should continue as usual.
      `acc` is the trigger state.

    * `{:cont, events, acc}` - the reduce operation should continue, but
      only with the events you want to emit as part of the next state.
      `acc` is the trigger state.

    * `{:trigger, name, pre, pos, acc}` - where `name` is the trigger `name`,
      `pre` are the events to be consumed before the trigger, `pos` controls
      events to be processed after the trigger with the `acc` as the new trigger
      accumulator.

  We recommend looking at the implementation of `trigger_every/2` as
  an example of a custom trigger.
  """
  @spec trigger(t, (() -> acc), trigger_fun) :: t
        when trigger_fun: ([event], acc -> trigger_fun_return),
             trigger_fun_return: cont_tuple | cont_tuple_with_emitted_events | trigger_tuple,
             cont_tuple: {:cont, acc},
             cont_tuple_with_emitted_events: {:cont, [event], acc},
             trigger_tuple: {:trigger, trigger(), pre, pos, acc},
             pre: [event],
             pos: [event],
             acc: term(),
             event: term()
  def trigger(window, acc_fun, trigger_fun) do
    if is_function(acc_fun, 0) do
      add_trigger(window, {acc_fun, trigger_fun})
    else
      raise ArgumentError,
            "Flow.Window.trigger/3 expects the accumulator to be given as a function"
    end
  end

  @doc """
  A trigger emitted every `count` elements in a window.

  The trigger will be named `{:every, count}`.

  ## Examples

  Below is an example that checkpoints the sum from 1 to 100, emitting
  a trigger with the state every 10 items. The extra 5050 value at the
  end is the trigger emitted because processing is done.

      iex> window = Flow.Window.global() |> Flow.Window.trigger_every(10)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
      iex> flow |> Flow.reduce(fn -> 0 end, &(&1 + &2)) |> Flow.emit(:state) |> Enum.to_list()
      [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]

  """
  @spec trigger_every(t, pos_integer) :: t
  def trigger_every(window, count) when is_integer(count) and count > 0 do
    name = {:every, count}

    trigger(window, fn -> count end, fn events, acc ->
      length = length(events)

      if length >= acc do
        {pre, pos} = Enum.split(events, acc)
        {:trigger, name, pre, pos, count}
      else
        {:cont, acc - length}
      end
    end)
  end

  @doc """
  Emits a trigger periodically every `count` `unit`.

  Such trigger will apply to every window that has changed since the last
  periodic trigger.

  `count` is a positive integer and `unit` is one of `:millisecond`,
  `:second`, `:minute`, or `:hour`. Remember periodic triggers are established
  per partition and are message-based, which means partitions will emit the
  triggers at different times and possibly with delays based on the partition
  message queue size.

  The trigger will be named `{:periodically, count, unit}`.

  ## Message-based triggers (timers)

  It is also possible to dispatch a trigger by sending a message to
  `self()` with the format of `{:trigger, name}`. This is useful for
  custom triggers and timers. One example is to send the message when
  building the accumulator for `Flow.reduce/3`.

  Similar to periodic triggers, message-based triggers will also be
  invoked to all windows that have changed since the last trigger.
  """
  @spec trigger_periodically(t, pos_integer, time_unit) :: t
  def trigger_periodically(%{periodically: periodically} = window, count, unit)
      when is_integer(count) and count > 0 do
    trigger = {to_ms(count, unit), {:periodically, count, unit}}
    %{window | periodically: [trigger | periodically]}
  end

  @spec to_ms(pos_integer(), time_unit()) :: pos_integer
  defp to_ms(count, :millisecond), do: count
  defp to_ms(count, :second), do: count * 1000
  defp to_ms(count, :minute), do: count * 1000 * 60
  defp to_ms(count, :hour), do: count * 1000 * 60 * 60

  defp to_ms(_count, unit) do
    raise ArgumentError,
          "unknown unit #{inspect(unit)} (expected :millisecond, :second, :minute or :hour)"
  end

  defp add_trigger(%{trigger: nil} = window, trigger) do
    %{window | trigger: trigger}
  end

  defp add_trigger(%{}, _trigger) do
    raise ArgumentError,
          "Flow.Window.trigger/3 or Flow.Window.trigger_every/2 " <>
            "can only be called once per window"
  end
end
