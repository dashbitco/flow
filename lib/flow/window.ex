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

      Flow.from_stage(some_producer)
      |> Flow.partition()
      |> Flow.reduce(fn -> 0 end, & &1 + 2)

  is equivalent to:

      Flow.from_stage(some_producer)
      |> Flow.partition(Flow.Window.global())
      |> Flow.reduce(fn -> 0 end, & &1 + 2)

  Even though the global window does not split the data in any way, it
  already provides conveniences for working with both bounded (finite)
  and unbounded (infinite) via triggers.

  For example, the flow below uses a global window with a count-based
  trigger to emit the values being summed as we sum them:

      iex> window = Flow.Window.global |> Flow.Window.trigger_every(10)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
      iex> flow |> Flow.reduce(fn -> 0 end, & &1 + &2) |> Flow.emit(:state) |> Enum.to_list()
      [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]

  Let's explore the types of triggers available next.

  ### Triggers

  Triggers allow us to check point the data processed so far. There
  are different triggers we can use:

    * Event count triggers - compute state operations every X events

    * Processing time triggers - compute state operations every X time
      units for every stage

    * Punctuation - hand-written triggers based on the data

  Flow supports the triggers above via the `trigger_every/3`,
  `trigger_periodically/4` and `trigger/3` respectively.

  Once a trigger is emitted, the `reduce/3` step halts and invokes
  the remaining steps for that flow such as `map_state/2` or any other
  call after `reduce/3`. Triggers are also named and the trigger names
  will be sent alongside the window name as third argument to the callback
  given to `map_state/2` and `each_state/2`.

  For every emitted trigger, developers have the choice of either
  resetting the reducer accumulator (`:reset`) or keeping it as is (`:keep`).
  The resetting option is useful when you are interested only on intermediate
  results, usually because another step is aggregating the data. Keeping the
  accumulator is the default and used to checkpoint the values while still
  working towards an end result.

  ### Event time and processing time

  Before we move to other window types, it is important to discuss
  the distinction between event time and processing time. In particular,
  triggers created with the `trigger_periodically/4` function are
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
  `reduce/3` is executed per window and that each event belongs to a single
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
  to also call `allowed_lateness/4`:

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
  `Flow.map_state/2` to get more information about each particular window
  and its trigger. Replace the last line above by the following:

      flow = flow |> Flow.map_state(fn state, _index, trigger -> {state, trigger} end)
      flow = flow |> Flow.emit(:state) |> Enum.to_list()

  The trigger parameter will include the type of window, the current
  window and what caused the window to be emitted (`:watermark` or
  `:done`).

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
      iex> flow |> Flow.reduce(fn -> 0 end, & &1 + &2) |> Flow.emit(:state) |> Enum.to_list()
      [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]

  Count windows are also similar to global windows that use `trigger_every/2`
  to emit events per count. The difference is that count windows emit a
  window per event count while a trigger belongs to a window. This behaviour
  may affect functions such as `Flow.departition/4`, which calls the `merge`
  callback per trigger but the `done` callback per window.  Unless you are
  relying on functions such as `Flow.departition/4`, there is no distinction
  between count windows and global windows with count triggers.

  ## Session windows (event time)

  Session windows are useful for data that is irregularly distributed with
  respect to time. For example, GPS data contains moments of user activity
  with long periods of user inactivity. Sessions allows us to group these
  events together until there is a time gap between them.

  Session windows by definition belong to a single key. Therefore, the :key
  option must be given to the partition alongside the window option. For
  instance, in case of GPS data, the key would be the `device_id` or the
  `user_id`.

  To build on this example, imagine we want to calculate the distance
  travelled by a user on certain trips based on GPS data. Let's assume the
  movement happens on a one-dimensional line for simplicity. Our server
  will receive streaming data from different users in the shape of:

      {user_id, position, time_in_seconds}

  Our code is going to calculate the location per user per trip based on
  time inactivity:

      iex> data = [{1, 32, 0}, {1, 35, 60}, {1, 40, 120},       # user 1 - trip 1
      ...>         {2, 45, 60}, {2, 43, 70}, {2, 47, 200},      # user 2 - trip 1
      ...>         {1, 40, 3600}, {1, 43, 3700}, {1, 50, 4000}] # user 1 - trip 2
      iex> key = fn {user_id, _position, _time} -> user_id end  # Partition per user
      iex> window = Flow.Window.session(20, :minute, fn {_user_id, _position, time} -> time * 1000 end)
      iex> flow = Flow.from_enumerable(data) |> Flow.partition(key: key, window: window)
      iex> flow = Flow.reduce(flow, fn -> :empty end, fn
      ...>   {_, pos, _}, :empty -> {pos, 0} # initial point and distance
      ...>   {_, pos, _}, {last, distance} -> {pos, abs(pos - last) + distance}
      ...> end)
      iex> flow = Flow.map_state(flow, fn {_, distance}, _partition, {:session, {user_id, start, last}, :done} ->
      ...>   {user_id, distance, div(last - start, 1000)} # user_id travelled total in last - start seconds
      ...> end)
      iex> flow |> Flow.emit(:state) |> Enum.sort()
      [{1, 8, 120}, {1, 10, 400}, {2, 6, 140}]
  """

  @type t :: %{required(:trigger) => {fun(), fun()} | nil,
               required(:periodically) => []}

  @typedoc "The supported window types."
  @type type :: :global | :fixed | :session | :periodic | :count | any()

  @typedoc """
  A function that retrieves the field to window by.

  It must be an integer representing the time in milliseconds.
  Flow does not care if the integer is using the UNIX epoch,
  Gregorian epoch or any other as long as it is consistent.
  """
  @type by :: (term -> non_neg_integer)

  @typedoc """
  The window identifier.

  It is `:global` for `:global` windows. An integer for fixed
  windows and a custom value for session windows.
  """
  @type id :: :global | non_neg_integer() | term()

  @typedoc "The name of the trigger."
  @type trigger :: term

  @typedoc "The operation to perform on the accumulator."
  @type accumulator :: :keep | :reset

  @trigger_operation [:keep, :reset]

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
  Returns a periodic-based window on every `count` `unit`.

  `count` is a positive integer and `unit` is one of `:millisecond`,
  `:second`, `:minute`, `:hour`. Remember periodic triggers are established
  per partition and are message-based, which means partitions will emit the
  triggers at different times and possibly with delays based on the partition
  message queue size.

  Periodic window triggers have the shape of `{:periodic, window, trigger_name}`,
  where `window` is an incrementing integer identifying the window.

  See the section on "Periodic windows" in the module documentation for examples.
  """
  @spec periodic(pos_integer, System.time_unit) :: t
  def periodic(count, unit) when is_integer(count) and count > 0 do
    %Flow.Window.Periodic{duration: to_ms(count, unit)}
  end

  @doc """
  Returns a fixed window of duration `count` `unit` where the
  event time is calculated by the given function `by`.

  `count` is a positive integer and `unit` is one of `:millisecond`,
  `:second`, `:minute`, `:hour`.

  Fixed window triggers have the shape of `{:fixed, window, trigger_name}`,
  where `window` is an integer that represents the beginning timestamp
  for the current window.

  If `allowed_lateness/4` is used with fixed windows, the window will
  first emit a `{:fixed, window, :watermark}` trigger when the window
  terminates and emit `{:fixed, window, :done}` only after the
  `allowed_lateness/4` duration has passed.

  See the section on "Fixed windows" in the module documentation for examples.
  """
  @spec fixed(pos_integer, System.time_unit, (t -> pos_integer)) :: t
  def fixed(count, unit, by) when is_integer(count) and count > 0 and is_function(by, 1) do
    %Flow.Window.Fixed{duration: to_ms(count, unit), by: by}
  end

  @doc """
  Returns a session window that works on gaps given by `count` `unit` and
  the event time is calculated by the given function `by`.

  `count` is a positive integer and `unit` is one of `:millisecond`,
  `:second`, `:minute`, `:hour`.

  Session window triggers have the shape of
  `{:session, {key, first_time, last_time}, trigger_name}`, where `key`
  is the window key, the `first_time` in the session and the `last_time`
  on the session thus far.

  See the section on "Session windows" in the module documentation for examples.
  """
  @spec session(pos_integer, System.time_unit, (t -> pos_integer)) :: t
  def session(count, unit, by) when is_integer(count) and count > 0 and is_function(by, 1) do
    %Flow.Window.Session{gap: to_ms(count, unit), by: by}
  end

  @doc """
  Sets a duration, in processing time, of how long we will
  wait for late events for a given window.

  If allowed lateness is configured, once the window is finished,
  it won't trigger a `:done` event but instead emit a `:watermark`.
  The `keep_or_reset` option can configure if the state should be
  kept or reset when the watermark is triggered. The window will
  be done only when the allowed lateness time expires, effectively
  emitting the `:done` trigger.

  `count` is a positive number. The `unit` may be a time unit
  (`:second`, `:millisecond`, `:second`, `:minute` and `:hour`).
  """
  @spec allowed_lateness(t, pos_integer, System.time_unit, :keep | :reset) :: t
  def allowed_lateness(window, count, unit, keep_or_reset \\ :keep)

  def allowed_lateness(%{lateness: _} = window, count, unit, keep_or_reset) do
    %{window | lateness: {to_ms(count, unit), keep_or_reset}}
  end
  def allowed_lateness(window, _, _, _) do
    raise ArgumentError, "allowed_lateness/4 not supported for window type #{inspect window}"
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
  The trigger function must return one of the two values:

    * `{:cont, acc}` - the reduce operation should continue as usual.
       `acc` is the trigger state.

    * `{:cont, events, acc}` - the reduce operation should continue, but
       only with the events you want to emit as part of the next state.
       `acc` is the trigger state.

    * `{:trigger, name, pre, operation, pos, acc}` - where `name` is the
      trigger `name`, `pre` are the events to be consumed before the trigger,
      the `operation` configures the stage should `:keep` the reduce accumulator
      or `:reset` it. `pos` controls events to be processed after the trigger
      with the `acc` as the new trigger accumulator.

  We recommend looking at the implementation of `trigger_every/3` as
  an example of a custom trigger.
  """
  @spec trigger(t, (() -> acc), ([event], acc -> cont_tuple |
                                                 cont_tuple_with_emitted_events |
                                                 trigger_tuple)) :: t
        when cont_tuple: {:cont, acc},
             cont_tuple_with_emitted_events: {:cont, [event], acc},
             trigger_tuple: {:trigger, trigger(), pre, accumulator(), pos, acc},
             pre: [event], pos: [event], acc: term(), event: term()
  def trigger(window, acc_fun, trigger_fun) do
    if is_function(acc_fun, 0) do
      add_trigger(window, {acc_fun, trigger_fun})
    else
      raise ArgumentError, "Flow.Window.trigger/3 expects the accumulator to be given as a function"
    end
  end

  @doc """
  A trigger emitted every `count` elements in a window.

  The `keep_or_reset` argument must be one of `:keep` or `:reset`.
  If `:keep`, the state accumulated so far on `reduce/3` will be kept,
  otherwise discarded.

  The trigger will be named `{:every, count}`.

  ## Examples

  Below is an example that checkpoints the sum from 1 to 100, emitting
  a trigger with the state every 10 items. The extra 5050 value at the
  end is the trigger emitted because processing is done.

      iex> window = Flow.Window.global |> Flow.Window.trigger_every(10)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
      iex> flow |> Flow.reduce(fn -> 0 end, & &1 + &2) |> Flow.emit(:state) |> Enum.to_list()
      [55, 210, 465, 820, 1275, 1830, 2485, 3240, 4095, 5050, 5050]

  Now let's see an example similar to above except we reset the counter
  on every trigger. At the end, the sum of all values is still 5050:

      iex> window = Flow.Window.global |> Flow.Window.trigger_every(10, :reset)
      iex> flow = Flow.from_enumerable(1..100) |> Flow.partition(window: window, stages: 1)
      iex> flow |> Flow.reduce(fn -> 0 end, & &1 + &2) |> Flow.emit(:state) |> Enum.to_list()
      [55, 155, 255, 355, 455, 555, 655, 755, 855, 955, 0]

  """
  @spec trigger_every(t, pos_integer, :keep | :reset) :: t
  def trigger_every(window, count, keep_or_reset \\ :keep)
      when is_integer(count) and count > 0 and keep_or_reset in @trigger_operation do
    name = {:every, count}

    trigger(window, fn -> count end, fn events, acc ->
      length = length(events)
      if length(events) >= acc do
        {pre, pos} = Enum.split(events, acc)
        {:trigger, name, pre, keep_or_reset, pos, count}
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
  `:second`, `:minute`, `:hour`. Remember periodic triggers are established
  per partition and are message-based, which means partitions will emit the
  triggers at different times and possibly with delays based on the partition
  message queue size.

  The `keep_or_reset` argument must be one of `:keep` or `:reset`. If
  `:keep`, the state accumulate so far on `reduce/3` will be kept, otherwise
  discarded.

  The trigger will be named `{:periodically, count, unit}`.

  ## Message-based triggers (timers)

  It is also possible to dispatch a trigger by sending a message to
  `self()` with the format of `{:trigger, :keep | :reset, name}`.
  This is useful for custom triggers and timers. One example is to
  send the message when building the accumulator for `reduce/3`.
  If `:reset` is used, every time the accumulator is rebuilt, a new
  message will be sent. If `:keep` is used and a new timer is necessary,
  then `each_state/2` can be called after `reduce/3` to resend it.

  Similar to periodic triggers, message-based triggers will also be
  invoked to all windows that have changed since the last trigger.
  """
  @spec trigger_periodically(t, pos_integer, System.time_unit, :keep | :reset) :: t
  def trigger_periodically(%{periodically: periodically} = window,
                           count, unit, keep_or_reset \\ :keep)
      when is_integer(count) and count > 0 do
    periodically = [{to_ms(count, unit), keep_or_reset, {:periodically, count, unit}} | periodically]
    %{window | periodically: periodically}
  end

  defp to_ms(count, :millisecond), do: count
  defp to_ms(count, :second), do: count * 1000
  defp to_ms(count, :minute), do: count * 1000 * 60
  defp to_ms(count, :hour), do: count * 1000 * 60 * 60
  defp to_ms(_count, unit), do: raise ArgumentError, "unknown unit #{inspect unit} (expected :millisecond, :second, :minute or :hour)"

  defp add_trigger(%{trigger: nil} = window, trigger) do
    %{window | trigger: trigger}
  end
  defp add_trigger(%{}, _trigger) do
    raise ArgumentError, "Flow.Window.trigger/3 or Flow.Window.trigger_every/3 " <>
                         "can only be called once per window"
  end
end
