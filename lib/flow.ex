defmodule Flow do
  @moduledoc ~S"""
  Computational flows with stages.

  `Flow` allows developers to express computations
  on collections, similar to the `Enum` and `Stream` modules,
  although computations will be executed in parallel using
  multiple `GenStage`s.

  Flow was also designed to work with both bounded (finite)
  and unbounded (infinite) data. By default, Flow will work
  with batches of 500 items. This means Flow will only show
  improvements when working with larger collections. However,
  for certain cases, such as IO-bound flows, a smaller batch size
  can be configured through the `:min_demand` and `:max_demand`
  options supported by `from_enumerable/2`, `from_stages/2`
  and `partition/2`.

  Flow also provides the concepts of "windows" and "triggers",
  which allow developers to split the data into arbitrary
  windows according to event time. Triggers allow computations
  to be materialized at different intervals, allowing developers
  to peek at results as they are computed.

  This README will cover the main constructs and concepts behind
  Flow, with examples. There is also a presentation about GenStage
  and Flow from José Valim at ElixirConf 2016, which covers
  data processing concepts for those unfamilar with the domain:
  <https://youtu.be/srtMWzyqdp8?t=244>

  ## Example

  As an example, let's implement the classic word counting
  algorithm using flow. The word counting program will receive
  one file and count how many times each word appears in the
  document. Using the `Enum` module it could be implemented
  as follows:

      File.stream!("path/to/some/file")
      |> Enum.flat_map(&String.split(&1, " "))
      |> Enum.reduce(%{}, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  Unfortunately, the implementation above is not very efficient,
  as `Enum.flat_map/2` will build a list with all the words in
  the document before reducing it. If the document is, for example,
  2GB, we will load 2GB of data into memory.

  We can improve the solution above by using the `Stream` module:

      File.stream!("path/to/some/file")
      |> Stream.flat_map(&String.split(&1, " "))
      |> Enum.reduce(%{}, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  Now instead of loading the whole set into memory, we will only
  keep the current line in memory while we process it. While this
  allows us to process the whole data set efficiently, it does
  not leverage concurrency. Flow solves that:

      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.partition()
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  To convert from Stream to Flow, we have made two changes:

    1. We have replaced the calls to `Stream` with `Flow`
    2. We call `partition/1` so words are properly partitioned between stages

  The example above will use all available cores and will
  keep an ongoing flow of data instead of traversing them
  line by line. Once all data is computed, it is sent to the
  process which invoked `Enum.to_list/1`.

  While we gain concurrency by using Flow, many of the benefits
  of Flow are in partitioning the data. We will discuss
  the need for data partitioning next.

  ## Partitioning

  To understand the need to partition the data, let's change the
  example above and remove the partition call:

      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  This will execute the `flat_map` and `reduce` operations in parallel
  inside multiple stages. When running on a machine with two cores:

       [file stream]  # Flow.from_enumerable/1 (producer)
          |    |
        [M1]  [M2]    # Flow.flat_map/2 + Flow.reduce/3 (consumer)

  Now imagine that the `M1` and `M2` stages above receive the
  following lines:

      M1 - "roses are red"
      M2 - "violets are blue"

  `flat_map/2` will break them into:

      M1 - ["roses", "are", "red"]
      M2 - ["violets", "are", "blue"]

  Then `reduce/3` will result in each stage having the following state:

      M1 - %{"roses" => 1, "are" => 1, "red" => 1}
      M2 - %{"violets" => 1, "are" => 1, "blue" => 1}

  Which is converted to the list (in no particular order):

      [{"roses", 1},
       {"are", 1},
       {"red", 1},
       {"violets", 1},
       {"are", 1},
       {"blue", 1}]

  Although both stages have performed word counting, we have words
  like "are" that appear on both stages. This means we would need
  to perform yet another pass on the data merging the duplicated
  words across stages. This step would have to run on a single process,
  which would limit our ability to run concurrently.

  Remember that events are batched, so for small files, there is a chance
  all lines will be set to the same stage (M1 or M2) and you won't be
  able to replicate the issue. If you want to emulate this, either to
  follow along or in your test suites, you may set `:max_demand` to 1
  when reading from the stream, so that the code looks like this:

      File.stream!("path/to/some/file")
      |> Flow.from_enumerable(max_demand: 1)
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  Partitioning solves this by introducing a new set of stages and
  making sure the same word is always mapped to the same stage
  with the help of a hash function. Let's introduce the call to
  `partition/1` back:

      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.partition()
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  Now we will have the following topology:

       [file stream]  # Flow.from_enumerable/1 (producer)
          |    |
        [M1]  [M2]    # Flow.flat_map/2 (producer-consumer)
          |\  /|
          | \/ |
          |/ \ |
        [R1]  [R2]    # Flow.reduce/3 (consumer)

  If the `M1` and `M2` stages receive the same lines and break
  them into words as before:

      M1 - ["roses", "are", "red"]
      M2 - ["violets", "are", "blue"]

  Now, any given word will be consistently routed to `R1` or `R2`
  regardless of its origin. The default hashing function will route
  them like this:

      R1 - ["roses", "are", "red", "are"]
      R2 - ["violets", "blue"]

  Resulting in the reduced state of:

      R1 - %{"roses" => 1, "are" => 2, "red" => 1}
      R2 - %{"violets" => 1, "blue" => 1}

  Which is converted to the list (in no particular order):

      [{"roses", 1},
       {"are", 2},
       {"red", 1},
       {"violets", 1},
       {"blue", 1}]

  Each stage has a distinct subset of the data so we know
  that we don't need to merge the data later on, because a given
  word is guaranteed to have only been routed to one stage.

  Partitioning the data is a very useful technique. For example,
  if we wanted to count the number of unique elements in a dataset,
  we could perform such a count in each partition and then sum
  their results, as the partitioning guarantees the data in
  each partition won't overlap. A unique element would never
  be counted twice.

  The topology above alongside partitioning is very common in
  the MapReduce programming model which we will briefly discuss
  next.

  ### MapReduce

  The MapReduce programming model forces us to break our computations
  in two stages: map and reduce. The map stage is often quite easy to
  parallelize because events are processed individually and in isolation.
  The reduce stages need to group the data either partially or completely.

  In the example above, the stages executing `flat_map/2` are the
  mapper stages. Because the `flat_map/2` function works line by line,
  we can have two, four, eight or more mapper processes that will
  break line by line into words without any need for coordination.

  However, the reducing stage is a bit more complicated. Reducer
  stages typically aggregate some result based on their inputs, such
  as how many times a word has appeared. This implies reducer
  computations need to traverse the whole data set and, in order
  to do so in parallel, we partition the data into distinct
  datasets.

  The goal of the `reduce/3` operation is to accumulate a value
  which then becomes the partition state. Any operation that
  happens after `reduce/3` works on the whole state and is only
  executed after all the data for a partition is collected.

  While this approach works well for bounded (finite) data, it
  is quite limited for unbounded (infinite) data. After all, if
  the reduce operation needs to traverse the whole partition to
  complete, how can we do so if the data never finishes?

  To answer this question, we need to talk about data completion,
  windows and triggers.

  ## Data completion, windows and triggers

  By default, Flow uses `GenStage`'s notification system to notify
  stages when a producer has emitted all events. This is done
  automatically by Flow when using `from_enumerable/2`. Custom
  producers can also send such notifications by calling
  `GenStage.async_notify/2` from themselves:

      # In the case where all the data is done
      GenStage.async_notify(self(), {:producer, :done})

      # In the case where the producer halted due to an external factor
      GenStage.async_notify(self(), {:producer, :halt})

  However, when working with an unbounded stream of data, there is
  no such thing as data completion. So when can we consider a reduce
  function to be "completed"?

  To handle such cases, Flow provides windows and triggers. Windows
  allow us to split the data based on the event time while triggers
  tells us when to write the results we have computed so far. By
  introducing windows, we no longer think about events being partitioned
  across stages. Instead each event belongs to a window and the window
  is partitioned across the stages.

  By default, all events belong to the same window (called the global
  window), which is partitioned across stages. However, different
  windowing strategies can be used by building a `Flow.Window`
  and passing it to the `Flow.partition/2` function.

  Once a window is specified, we can create triggers that tell us
  when to checkpoint the data, allowing us to report our progress
  while the data streams through the system, regardless of whether
  the data is bounded or unbounded.

  Windows and triggers effectively control how the `reduce/3` function
  works. `reduce/3` is invoked per window, while a trigger configures
  when `reduce/3` halts so we can checkpoint the data before resuming
  the computation with an old or new accumulator. See `Flow.Window`
  for a complete introduction to windows and triggers.

  ## Supervisable flows

  In the examples so far we have started a flow dynamically
  and consumed it using `Enum.to_list/1`. Unfortunately calling
  a function from `Enum` will cause the whole computed dataset
  to be sent to a single process.

  In many situations, this is either too expensive or completely
  undesirable. For example, in data-processing pipelines, it is
  common to receive data continuously from external sources. At
  the end, this data is written to disk or another storage mechanism
  after being processed, rather than being sent to a single process.

  Flow allows computations to be started as a group of processes
  which may run indefinitely. This can be done by starting
  the flow as part of a supervision tree using `Flow.start_link/2`.
  `Flow.into_stages/3` can also be used to start the flow as a
  linked process which will send the events to the given consumers.

  ## Performance discussions

  In this section we will discuss points related to performance
  with flows.

  ### Know your code

  There are many optimizations we could perform in the flow above
  that are not necessarily related to flows themselves. Let's rewrite
  the flow using some of them:

      # The parent process which will own the table
      parent = self()

      # Let's compile common patterns for performance
      empty_space = :binary.compile_pattern(" ") # BINARY

      File.stream!("path/to/some/file", read_ahead: 100_000) # READ_AHEAD
      |> Flow.from_enumerable()
      |> Flow.flat_map(&String.split(&1, empty_space)) # BINARY
      |> Flow.partition()
      |> Flow.reduce(fn -> :ets.new(:words, []) end, fn word, ets -> # ETS
        :ets.update_counter(ets, word, {2, 1}, {word, 0})
        ets
      end)
      |> Flow.map_state(fn ets ->         # ETS
        :ets.give_away(ets, parent, [])
        [ets]
      end)
      |> Enum.to_list()

  We have performed three optimizations:

    * BINARY - the first optimization is to compile the pattern we use
      to split the string on

    * READ_AHEAD - the second optimization is to use the `:read_ahead`
      option for file streams allowing us to do fewer IO operations by
      reading large chunks of data at once

    * ETS - the third stores the data in a ETS table and uses its counter
      operations. For counters and a large dataset this provides a great
      performance benefit as it generates less garbage. At the end, we
      call `map_state/2` to transfer the ETS table to the parent process
      and wrap the table in a list so we can access it on `Enum.to_list/1`.
      This step is not strictly required. For example, one could write the
      table to disk with `:ets.tab2file/2` at the end of the computation

  ### Configuration (demand and the number of stages)

  `from_enumerable/2`, `from_stages/2` and `partition/3` allow a set of
  options to configure how flows work. In particular, we recommend that
  developers play with the `:min_demand` and `:max_demand` options, which
  control the amount of data sent between stages. The difference between
  `max_demand` and `min_demand` works as the batch size when the producer
  is full. If the producer has fewer events than requested by consumers,
  it usually sends the remaining events available.

  If stages perform IO, it may also be worth increasing
  the number of stages. The default value is `System.schedulers_online/0`,
  which is a good default if the stages are CPU bound, but if stages
  are waiting on external resources or other processes, increasing the
  number of stages may be helpful.

  ### Avoid single sources

  In the examples so far we have used a single file as our data
  source. In practice such single sources should be avoided as they
  could end up being the bottleneck of our whole computation.

  In the file stream case above, instead of having one single
  large file, it is preferable to break the file into smaller
  ones:

      streams = for file <- File.ls!("dir/with/files") do
        File.stream!("dir/with/files/#{file}", read_ahead: 100_000)
      end

      streams
      |> Flow.from_enumerables()
      |> Flow.flat_map(&String.split(&1, " "))
      |> Flow.partition()
      |> Flow.reduce(fn -> %{} end, fn word, acc ->
        Map.update(acc, word, 1, & &1 + 1)
      end)
      |> Enum.to_list()

  Instead of calling `from_enumerable/1`, we now called
  `from_enumerables/1` which expects a list of enumerables to
  be used as source. Notice every stream also uses the `:read_ahead`
  option which tells Elixir to buffer file data in memory to
  avoid multiple IO lookups.

  If the number of enumerables is equal to or greater than the number of
  cores, Flow will automatically fuse the enumerables with the mapper
  logic. For example, if three file streams are given as enumerables
  to a machine with two cores, we will have the following topology:

      [F1][F2][F3]  # file stream
      [M1][M2][M3]  # Flow.flat_map/2 (producer)
        |\ /\ /|
        | /\/\ |
        |//  \\|
        [R1][R2]    # Flow.reduce/3 (consumer)

  """

  defstruct producers: nil, window: nil, options: [], operations: []
  @type t :: %Flow{producers: producers, operations: [operation],
                   options: keyword(), window: Flow.Window.t}

  @typep producers :: nil |
                      {:stages, GenStage.stage | [GenStage.stage]} |
                      {:enumerables, Enumerable.t} |
                      {:join, t, t, fun(), fun(), fun()} |
                      {:departition, t, fun(), fun(), fun()} |
                      {:flows, [t]}

  @typep operation :: {:mapper, atom(), [term()]} |
                      {:partition, keyword()} |
                      {:map_state, fun()} |
                      {:reduce, fun(), fun()} |
                      {:uniq, fun()}


  ## Building

  @doc """
  Starts a flow with the given enumerable as the producer.

  Calling this function is equivalent to:

      Flow.from_enumerables([enumerable], options)

  The enumerable is consumed in batches, retrieving `max_demand`
  items the first time and then `max_demand - min_demand` the
  next times. Therefore, for streams that cannot produce items
  that fast, it is recommended to pass a lower `:max_demand`
  value as an option.

  It is also expected the enumerable is able to produce the whole
  batch on demand or terminate. If the enumerable is a blocking one,
  for example, because it needs to wait for data from another source,
  it will block until the current batch is fully filled. GenStage and
  Flow were created exactly to address such issue. So if you have a
  blocking enumerable that you want to use in your Flow, then it must
  be implemented with GenStage and integrated with `from_stages/2`.

  ## Examples

      "some/file"
      |> File.stream!(read_ahead: 100_000)
      |> Flow.from_enumerable()

      some_network_based_stream()
      |> Flow.from_enumerable(max_demand: 20)

  """
  @spec from_enumerable(Enumerable.t, keyword) :: t
  def from_enumerable(enumerable, options \\ [])

  def from_enumerable(%Flow{}, _options) do
    raise ArgumentError, "passing a Flow to Flow.from_enumerable/2 is not supported. " <>
                         "Did you mean to use Flow.partition/2 or Flow.merge/2?"
  end

  def from_enumerable(enumerable, options) do
    from_enumerables([enumerable], options)
  end

  @doc """
  Starts a flow with the given enumerable as producer.

  The enumerable is consumed in batches, retrieving `max_demand`
  items the first time and then `max_demand - min_demand` the
  next times. Therefore, for streams that cannot produce items
  that fast, it is recommended to pass a lower `:max_demand`
  value as an option.

  See `GenStage.from_enumerable/2` for information and
  limitations on enumerable-based stages.

  ## Options

  These options configure the stages connected to producers before partitioning.

    * `:window` - a window to run the next stages in, see `Flow.Window`
    * `:stages` - the number of stages
    * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
    * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.

  ## Examples

      files = [File.stream!("some/file1", read_ahead: 100_000),
               File.stream!("some/file2", read_ahead: 100_000),
               File.stream!("some/file3", read_ahead: 100_000)]
      Flow.from_enumerables(files)
  """
  @spec from_enumerables([Enumerable.t], keyword) :: t
  def from_enumerables(enumerables, options \\ [])

  def from_enumerables([_ | _] = enumerables, options) do
    options = stages(options)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global)
    %Flow{producers: {:enumerables, enumerables}, options: options, window: window}
  end
  def from_enumerables(enumerables, _options) do
    raise ArgumentError, "from_enumerables/2 expects a non-empty list as argument, got: #{inspect enumerables}"
  end

  @doc """
  Starts a flow with the given stage as producer.

  Calling this function is equivalent to:

      Flow.from_stages([stage], options)

  See `from_stages/2` for more information.

  ## Examples

      Flow.from_stage(MyStage)

  """
  @spec from_stage(GenStage.stage, keyword) :: t
  def from_stage(stage, options \\ []) do
    from_stages([stage], options)
  end

  @doc """
  Starts a flow with the list of stages as producers.

  ## Options

  These options configure the stages connected to producers before partitioning.

    * `:window` - a window to run the next stages in, see `Flow.Window`
    * `:stages` - the number of stages
    * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
    * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.

  ## Examples

      stages = [pid1, pid2, pid3]
      Flow.from_stages(stages)

  ## Termination

  Producer stages can signal the flow that it has emitted all
  events by emitting a notification using `GenStage.async_notify/2`
  from themselves:

      # In the case all the data is done
      GenStage.async_notify(self(), {:producer, :done})

      # In the case the producer halted due to an external factor
      GenStage.async_notify(self(), {:producer, :halt})

  Your producer may also keep track of all consumers and automatically
  shut down when all consumers have exited.
  """
  @spec from_stages([GenStage.stage], keyword) :: t
  def from_stages(stages, options \\ [])

  def from_stages([_ | _] = stages, options) do
    options = stages(options)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global)
    %Flow{producers: {:stages, stages}, options: options, window: window}
  end
  def from_stages(stages, _options) do
    raise ArgumentError, "from_stages/2 expects a non-empty list as argument, got: #{inspect stages}"
  end

  @joins [:inner, :left_outer, :right_outer, :full_outer]

  @doc """
  Joins two bounded (finite) flows.

  It expects the `left` and `right` flow, the `left_key` and
  `right_key` to calculate the key for both flows and the `join`
  function which is invoked whenever there is a match.

  A join creates a new partitioned flow that subscribes to the
  two flows given as arguments.  The newly created partitions
  will accumulate the data received from both flows until there
  is no more data. Therefore, this function is useful for merging
  finite flows. If used for merging infinite flows, you will
  eventually run out of memory due to the accumulated data. See
  `window_join/8` for applying a window to a join, allowing the
  join data to be reset per window.

  The join has 4 modes:

    * `:inner` - data will only be emitted when there is a match
      between the keys in left and right side
    * `:left_outer` - similar to `:inner` plus all items given
      in the left that did not have a match will be emitted at the
      end with `nil` for the right value
    * `:right_outer` - similar to `:inner` plus all items given
      in the right that did not have a match will be emitted at the
      end with `nil` for the left value
    * `:full_outer` - similar to `:inner` plus all items given
      in the left and right that did not have a match will be emitted
      at the end with `nil` for the right and left value respectively

  The joined partitions can be configured via `options` with the
  same values as shown on `from_enumerable/2` or `from_stages/2`.

  ## Examples

      iex> posts = [%{id: 1, title: "hello"}, %{id: 2, title: "world"}]
      iex> comments = [{1, "excellent"}, {1, "outstanding"},
      ...>             {2, "great follow up"}, {3, "unknown"}]
      iex> flow = Flow.bounded_join(:inner,
      ...>                          Flow.from_enumerable(posts),
      ...>                          Flow.from_enumerable(comments),
      ...>                          & &1.id, # left key
      ...>                          & elem(&1, 0), # right key
      ...>                          fn post, {_post_id, comment} -> Map.put(post, :comment, comment) end)
      iex> Enum.sort(flow)
      [%{id: 1, title: "hello", comment: "excellent"},
       %{id: 2, title: "world", comment: "great follow up"},
       %{id: 1, title: "hello", comment: "outstanding"}]

  """
  @spec bounded_join(:inner | :left_outer | :right_outer | :outer, t, t,
                     fun(), fun(), fun(), keyword()) :: t
  def bounded_join(mode, %Flow{} = left, %Flow{} = right,
                   left_key, right_key, join, options \\ [])
      when is_function(left_key, 1) and is_function(right_key, 1) and
           is_function(join, 2) and mode in @joins do
    window_join(mode, left, right, Flow.Window.global, left_key, right_key, join, options)
  end

  @doc """
  Joins two flows with the given window.

  It is similar to `bounded_join/7` with the addition a window
  can be given. The window function applies to elements of both
  left and right side in isolation (and not the joined value). A
  trigger will cause the join state to be cleared.

  ## Examples

  As an example, let's expand the example given in `bounded_join/7`
  and apply a window to it. The example in `bounded_join/7` returned
  3 results but in this example, because we will split the posts
  and comments in two different windows, we will get only two results
  as the later comment for `post_id=1` won't have a matching comment for
  its window:

      iex> posts = [%{id: 1, title: "hello", timestamp: 0}, %{id: 2, title: "world", timestamp: 1000}]
      iex> comments = [{1, "excellent", 0}, {1, "outstanding", 1000},
      ...>             {2, "great follow up", 1000}, {3, "unknown", 1000}]
      iex> window = Flow.Window.fixed(1, :second, fn
      ...>   {_, _, timestamp} -> timestamp
      ...>   %{timestamp: timestamp} -> timestamp
      ...> end)
      iex> flow = Flow.window_join(:inner,
      ...>                         Flow.from_enumerable(posts),
      ...>                         Flow.from_enumerable(comments),
      ...>                         window,
      ...>                         & &1.id, # left key
      ...>                         & elem(&1, 0), # right key
      ...>                         fn post, {_post_id, comment, _ts} -> Map.put(post, :comment, comment) end,
      ...>                         stages: 1, max_demand: 1)
      iex> Enum.sort(flow)
      [%{id: 1, title: "hello", comment: "excellent", timestamp: 0},
       %{id: 2, title: "world", comment: "great follow up", timestamp: 1000}]

  """
  @spec window_join(:inner | :left_outer | :right_outer | :outer, t, t, Flow.Window.t,
                    fun(), fun(), fun(), keyword()) :: t
  def window_join(mode, %Flow{} = left, %Flow{} = right, %{} = window,
                  left_key, right_key, join, options \\ [])
      when is_function(left_key, 1) and is_function(right_key, 1) and
           is_function(join, 2) and mode in @joins do
    options = stages(options)
    %Flow{producers: {:join, mode, left, right, left_key, right_key, join},
          options: options, window: window}
  end

  @doc """
  Runs a given flow.

  This runs the given flow as a stream for its side-effects. No
  items are sent from the flow to the current process.

  ## Examples

      iex> parent = self()
      iex> [1, 2, 3] |> Flow.from_enumerable() |> Flow.each(&send(parent, &1)) |> Flow.run()
      :ok
      iex> receive do
      ...>   1 -> :ok
      ...> end
      :ok

  """
  @spec run(t) :: :ok
  def run(flow) do
    [] = flow |> emit(:nothing) |> Enum.to_list()
    :ok
  end

  @doc """
  Starts and runs the flow as a separate process.

  See `into_stages/3` in case you want the flow to
  work as a producer for another series of stages.

  ## Options

    * `:dispatcher` - the dispatcher responsible for handling demands.
      Defaults to `GenStage.DemandDispatch`. May be either an atom or
      a tuple with the dispatcher and the dispatcher options

    * `:demand` - configures the demand on the flow producers to `:forward`
      or `:accumulate`. The default is `:forward`. See `GenStage.demand/2`
      for more information.

  """
  @spec start_link(t, keyword()) :: GenServer.on_start
  def start_link(flow, options \\ []) do
    Flow.Coordinator.start_link(emit(flow, :nothing), :consumer, [], options)
  end

  @doc """
  Starts and runs the flow as a separate process which
  will be a producer to the given `consumers`.

  It expects a list of consumers to subscribe to. Each element
  represents the consumer or a tuple with the consumer and the
  subscription options as defined in `GenStage.sync_subscribe/2`.

  The `pid` returned by this function identifies a coordinator
  process. While it is possible to send subscribe requests to
  the coordinator process, the coordinator process will simply
  redirect the subscription to the proper flow processes and
  cancel the initial subscription. This means it is recommended
  that late subscriptions use `cancel: :transient`. However
  keep in mind the consumer will continue running when producers
  exit with `:normal` or `:shutdown` reason. In order to properly
  shutdown the application, it is recommended for consumers to
  track subscriptions.

  ## Options

  This function receives the same options as `start_link/2`.
  """
  @spec into_stages(t, consumers, keyword()) :: GenServer.on_start when
        consumers: [GenStage.stage | {GenStage.stage, keyword()}]
  def into_stages(flow, consumers, options \\ []) do
    Flow.Coordinator.start_link(flow, :producer_consumer, consumers, options)
  end

  ## Mappers

  @doc """
  Applies the given function to each input without modifying it.

  ## Examples

      iex> parent = self()
      iex> [1, 2, 3] |> Flow.from_enumerable() |> Flow.each(&send(parent, &1)) |> Enum.sort()
      [1, 2, 3]
      iex> receive do
      ...>   1 -> :ok
      ...> end
      :ok

  """
  @spec each(t, (term -> term)) :: t
  def each(flow, each) when is_function(each, 1) do
    add_operation(flow, {:mapper, :each, [each]})
  end

  @doc """
  Applies the given function filtering each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.filter(& rem(&1, 2) == 0)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [2]

  """
  @spec filter(t, (term -> term)) :: t
  def filter(flow, filter) when is_function(filter, 1) do
    add_operation(flow, {:mapper, :filter, [filter]})
  end

  @doc """
  Applies the given function filtering and mapping each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.filter_map(& rem(&1, 2) == 0, & &1 * 2)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [4]

  """
  @spec filter_map(t, (term -> term), (term -> term)) :: t
  def filter_map(flow, filter, mapper) when is_function(filter, 1) and is_function(mapper, 1) do
    add_operation(flow, {:mapper, :filter_map, [filter, mapper]})
  end

  @doc """
  Applies the given function mapping each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.map(& &1 * 2)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [2, 4, 6]

      iex> flow = Flow.from_enumerables([[1, 2, 3], 1..3]) |> Flow.map(& &1 * 2)
      iex> Enum.sort(flow)
      [2, 2, 4, 4, 6, 6]

  """
  @spec map(t, (term -> term)) :: t
  def map(flow, mapper) when is_function(mapper, 1) do
    add_operation(flow, {:mapper, :map, [mapper]})
  end

  @doc """
  Maps over the given values in the stage state.

  It is expected the state to emit two-elements tuples,
  such as list, maps, etc.

  ## Examples

      iex> flow = Flow.from_enumerable([foo: 1, foo: 2, bar: 3, foo: 4, bar: 5], stages: 1)
      iex> flow |> Flow.group_by_key |> Flow.map_values(&Enum.sort/1) |> Enum.sort()
      [bar: [3, 5], foo: [1, 2, 4]]

  """
  def map_values(flow, value_fun) when is_function(value_fun) do
    map(flow, fn {key, value} -> {key, value_fun.(value)} end)
  end

  @doc """
  Applies the given function mapping each input in parallel and
  flattening the result, but only one level deep.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.flat_map(fn(x) -> [x, x * 2] end)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [1, 2, 2, 3, 4, 6]

  """
  @spec flat_map(t, (term -> Enumerable.t)) :: t
  def flat_map(flow, flat_mapper) when is_function(flat_mapper, 1) do
    add_operation(flow, {:mapper, :flat_map, [flat_mapper]})
  end

  @doc """
  Applies the given function rejecting each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.reject(& rem(&1, 2) == 0)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [1, 3]

  """
  @spec reject(t, (term -> term)) :: t
  def reject(flow, filter) when is_function(filter, 1) do
    add_operation(flow, {:mapper, :reject, [filter]})
  end

  ## Reducers

  @doc """
  Creates a new partition for the given flow with the given options

  Every time this function is called, a new partition
  is created. It is typically recommended to invoke it
  before a reducing function, such as `reduce/3`, so data
  belonging to the same partition can be kept together.

  ## Examples

      flow |> Flow.partition(window: Flow.Global.window)
      flow |> Flow.partition(stages: 4)

  ## Options

    * `:window` - a `Flow.Window` struct which controls how the
       reducing function behaves, see `Flow.Window` for more information.
    * `:stages` - the number of partitions (reducer stages)
    * `:key` - the key to use when partitioning. It is a function
      that receives a single argument (the event) and must return its key.
      To facilitate customization, `:key` also allows common values, such as
      `{:elem, integer}` and `{:key, atom}`, to calculate the hash based on a
      tuple or a map field. See the "Key shortcuts" section below.
    * `:hash` - the hashing function. By default a hashing function is built
      on the key but a custom one may be specified as described in
      `GenStage.PartitionDispatcher`
    * `:dispatcher` - by default, `partition/2` uses `GenStage.PartitionDispatcher`
      with the given hash function but any other dispatcher can be given
    * `:min_demand` - the minimum demand for this subscription
    * `:max_demand` - the maximum demand for this subscription

  ## Key shortcuts

  The following shortcuts can be given to the `:hash` option:

    * `{:elem, pos}` - apply the hash function to the element
      at position `pos` in the given tuple

    * `{:key, key}` - apply the hash function to the key of a given map

  """
  @spec partition(t, keyword()) :: t
  def partition(flow, options \\ []) when is_list(options) do
    merge([flow], options)
  end

  @doc """
  Reduces windows over multiple partitions into a single stage.

  Once `departition/5` is called, computations no longer
  happen concurrently until the data is once again partitioned.

  `departition/5` is typically invoked as the last step in a flow
  to merge the state from all previous partitions per window.

  It requires a flow and three functions as arguments as
  described:

    * the accumulator function - a zero-arity function that returns
      the initial accumulator. This function is invoked per window.
    * the merger function - a function that receives the state of
      a given partition and the accumulator and merges them together.
    * the done function - a function that receives the final accumulator.

  A set of options may also be given to customize with the `:window`,
  `:min_demand` and `:max_demand`.

  ## Examples

  For example, imagine we are counting words in a document. Each
  partition ends up with a map of words as keys and count as values.
  In the examples in the module documentation, we streamed those
  results to a single client using `Enum.to_list/1`. However, we
  could use `departition/5` to reduce the data over multiple stages
  returning one single map with all results:

      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.map(&String.split/1)
      |> Flow.partition()
      |> Flow.reduce(fn -> %{} end, fn event, acc -> Map.update(acc, event, 1, & &1 + 1) end)
      |> Flow.departition(&Map.new/0, &Map.merge/2, &(&1))
      |> Enum.to_list

  The departition function expects the initial accumulator, a function
  that merges the data, and a final function invoked when the computation
  is done.

  Departition also works with windows and triggers. A new accumulator
  is created per window and the merge function is invoked with the state
  every time a trigger is emitted in any of the partitions. This can be
  useful to compute the final state as computations happen instead of one
  time at the end. For example, we could change the flow above so each
  partition emits their whole intermediary state every 1000 items, merging
  it into the departition more frequently:

      File.stream!("path/to/some/file")
      |> Flow.from_enumerable()
      |> Flow.map(&String.split/1)
      |> Flow.partition(window: Flow.Window.global |> Flow.Window.trigger_every(1000, :reset))
      |> Flow.reduce(fn -> %{} end, fn event, acc -> Map.update(acc, event, 1, & &1 + 1) end)
      |> Flow.departition(&Map.new/0, &Map.merge(&1, &2, fn _, v1, v2 -> v1 + v2 end), &(&1))
      |> Enum.to_list

  Each approach is going to have different performance characteristics
  and it is important to measure to verify which one will be more efficient
  to the problem at hand.
  """
  def departition(%Flow{} = flow, acc_fun, merge_fun, done_fun, options \\ [])
      when is_function(acc_fun, 0) and is_function(merge_fun, 2) and
           (is_function(done_fun, 1) or is_function(done_fun, 2)) do
    unless has_reduce?(flow) do
      raise ArgumentError, "departition/5 must be called after a group_by/reduce operation"
    end

    done_fun =
      if is_function(done_fun, 1) do
        fn acc, _ -> done_fun.(acc) end
      else
        done_fun
      end

    flow = map_state(flow, fn state, {partition, _}, trigger ->
      [{state, partition, trigger}]
    end)

    {window, options} =
      options
      |> Keyword.put(:dispatcher, GenStage.DemandDispatcher)
      |> Keyword.put(:stages, 1)
      |> Keyword.pop(:window, Flow.Window.global)

    %Flow{producers: {:departition, flow, acc_fun, merge_fun, done_fun},
          options: options, window: window}
  end

  @doc """
  Merges the given flows into a new partition with the given
  window and options.

  Similar to `partition/2`, this function will partition
  the data, routing events with the same characteristics
  to the same partition.

  It accepts the same options and hash shortcuts as
  `partition/2`. See `partition/2` for more information.

  ## Examples

      Flow.merge([flow1, flow2], window: Flow.Global.window)
      Flow.merge([flow1, flow2], stages: 4)

  """
  @spec merge([t], keyword()) :: t
  def merge(flows, options \\ [])

  def merge([%Flow{} | _] = flows, options) when is_list(options) do
    options = stages(options)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global)
    %Flow{producers: {:flows, flows}, options: options, window: window}
  end
  def merge(other, options) when is_list(options) do
    raise ArgumentError, "Flow.merge/2 expects a non-empty list of flows as first argument, got: #{inspect other}"
  end

  defp stages(options) do
    case Keyword.fetch(options, :stages) do
      {:ok, _} ->
        options
      :error ->
        stages = System.schedulers_online()
        [stages: stages] ++ options
    end
  end

  @doc """
  Reduces the given values with the given accumulator.

  `acc_fun` is a function that receives no arguments and returns
  the actual accumulator. The `acc_fun` function is invoked per window
  whenever a new window starts. If a trigger is emitted and it is
  configured to reset the accumulator, the `acc_fun` function will
  be invoked once again.

  Reducing will accumulate data until a trigger is emitted
  or until a window completes. When that happens, the returned
  accumulator will be the new state of the stage and all functions
  after reduce will be invoked.

  ## Examples

      iex> flow = Flow.from_enumerable(["the quick brown fox"]) |> Flow.flat_map(fn word ->
      ...>    String.graphemes(word)
      ...> end)
      iex> flow = flow |> Flow.partition |> Flow.reduce(fn -> %{} end, fn grapheme, map ->
      ...>   Map.update(map, grapheme, 1, & &1 + 1)
      ...> end)
      iex> Enum.sort(flow)
      [{" ", 3}, {"b", 1}, {"c", 1}, {"e", 1}, {"f", 1},
       {"h", 1}, {"i", 1}, {"k", 1}, {"n", 1}, {"o", 2},
       {"q", 1}, {"r", 1}, {"t", 1}, {"u", 1}, {"w", 1},
       {"x", 1}]

  """
  @spec reduce(t, (() -> acc), (term, acc -> acc)) :: t when acc: term()
  def reduce(flow, acc_fun, reducer_fun) when is_function(reducer_fun, 2) do
    cond do
      has_reduce?(flow) ->
        raise ArgumentError, "cannot call group_by/reduce on a flow after another group_by/reduce operation " <>
                             "(it must be called only once per partition, consider using map_state/2 instead)"
      is_function(acc_fun, 0) ->
        add_operation(flow, {:reduce, acc_fun, reducer_fun})
      true ->
        raise ArgumentError, "Flow.reduce/3 expects the accumulator to be given as a function"
    end
  end

  @doc """
  Takes `n` events according to the sort function.

  This function allows developers to calculate the top `n` entries
  (or the bottom `n` entries) by performing most of the work
  concurrently.

  First `n` events are taken from every partition and then those `n`
  events from every partition are merged into a single partition. The
  final result is a flow with a single partition that will emit a list
  with the top `n` events. The sorting is given by the `sort_fun`.

  `take_sort/3` is built on top of departition, which means it will
  also take and sort entries across windows.

  ## Examples

  As an example, imagine you are processing a list of URLs and you want
  the list of the most accessed URLs.

      iex> urls = ~w(www.foo.com www.bar.com www.foo.com www.foo.com www.baz.com)
      iex> flow = urls |> Flow.from_enumerable |> Flow.partition()
      iex> flow = flow |> Flow.reduce(fn -> %{} end, fn url, map ->
      ...>   Map.update(map, url, 1, & &1 + 1)
      ...> end)
      iex> flow = flow |> Flow.take_sort(1, fn {_url_a, count_a}, {_url_b, count_b} ->
      ...>   count_b <= count_a
      ...> end)
      iex> Enum.to_list(flow)
      [[{"www.foo.com", 3}]]

  """
  def take_sort(flow, n, sort_fun \\ &<=/2) when is_integer(n) and n > 0 do
    unless has_reduce?(flow) do
      raise ArgumentError, "take_sort/3 must be called after a group_by/reduce operation"
    end

    flow
    |> map_state(& &1 |> Enum.sort(sort_fun) |> Enum.take(n))
    |> departition(fn -> [] end, &merge_sorted(&1, &2, n, sort_fun), fn x -> x end)
  end

  defp merge_sorted([], other, _, _), do: other
  defp merge_sorted(other, [], _, _), do: other
  defp merge_sorted(left, right, n, sort), do: merge_sorted(left, right, 0, n, sort)

  defp merge_sorted(_, _, count, count, _sort), do: []
  defp merge_sorted(lefties, [], count, n, _sort), do: Enum.take(lefties, n - count)
  defp merge_sorted([], righties, count, n, _sort), do: Enum.take(righties, n - count)

  defp merge_sorted([left | lefties], [right | righties], count, n, sort) do
    case sort.(left, right) do
      true ->
        [left | merge_sorted(lefties, [right | righties], count + 1, n, sort)]
      false ->
        [right | merge_sorted([left | lefties], righties, count + 1, n, sort)]
    end
  end

  @doc """
  Groups events with the given `key_fun`.

  This is a reduce operation that groups events into maps
  where the key is the key returned by `key_fun` and the
  value is a list of values in reverse order as returned by
  `value_fun`. The resulting map becomes the stage state.

  ## Examples

      iex> flow = Flow.from_enumerable(~w[the quick brown fox], stages: 1)
      iex> flow |> Flow.group_by(&String.length/1) |> Flow.emit(:state) |> Enum.to_list()
      [%{3 => ["fox", "the"], 5 => ["brown", "quick"]}]

  """
  @spec group_by(t, (term -> term), (term -> term)) :: t
  def group_by(flow, key_fun, value_fun \\ fn x -> x end)
      when is_function(key_fun, 1) and is_function(value_fun, 1) do
    reduce(flow, fn -> %{} end, fn entry, categories ->
      value = value_fun.(entry)
      Map.update(categories, key_fun.(entry), [value], &[value | &1])
    end)
  end

  @doc """
  Groups a series of `{key, value}` tuples by keys.

  This is a reduce operation that groups events into maps
  with the given key and a list of values with the given keys
  in reverse order. The resulting map becomes the stage state.

  ## Examples

      iex> flow = Flow.from_enumerable([foo: 1, foo: 2, bar: 3, foo: 4, bar: 5], stages: 1)
      iex> flow |> Flow.group_by_key |> Flow.emit(:state) |> Enum.to_list()
      [%{foo: [4, 2, 1], bar: [5, 3]}]

  """
  @spec group_by_key(t) :: t
  def group_by_key(flow) do
    reduce(flow, fn -> %{} end, fn {key, value}, acc ->
      Map.update(acc, key, [value], &[value | &1])
    end)
  end

  @doc """
  Only emit unique events.

  Calling this function is equivalent to:

      Flow.uniq_by(flow, & &1)

  See `uniq_by/2` for more information.
  """
  def uniq(flow) do
    uniq_by(flow, & &1)
  end

  @doc """
  Only emit events that are unique according to the `by` function.

  In order to verify if an item is unique or not, `uniq_by/2`
  must store the value computed by `by/1` into a set. This means
  that, when working with unbounded data, it is recommended to
  wrap `uniq_by/2` in a window otherwise the data set will grow
  forever, eventually using all memory available.

  Also keep in mind that `uniq_by/2` is applied per partition.
  Therefore, if the data is not uniquely divided per partition,
  it won't be able to calculate the unique items properly.

  ## Examples

  To get started, let's create a flow that emits only the first
  odd and even number for a range:

      iex> flow = Flow.from_enumerable(1..100)
      iex> flow = Flow.partition(flow, stages: 1)
      iex> flow |> Flow.uniq_by(&rem(&1, 2)) |> Enum.sort()
      [1, 2]

  Since we have used only one stage when partitioning, we
  correctly calculate `[1, 2]` for the given partition. Let's see
  what happens when we increase the number of stages in the partition:

      iex> flow = Flow.from_enumerable(1..100)
      iex> flow = Flow.partition(flow, stages: 4)
      iex> flow |> Flow.uniq_by(&rem(&1, 2)) |> Enum.sort()
      [1, 2, 3, 4, 10, 16, 23, 39]

  Now we got 8 numbers, one odd and one even *per partition*. If
  we want to compute the unique items per partition, we must properly
  hash the events into two distinct partitions, one for odd numbers
  and another for even numbers:

      iex> flow = Flow.from_enumerable(1..100)
      iex> flow = Flow.partition(flow, stages: 2, hash: fn event -> {event, rem(event, 2)} end)
      iex> flow |> Flow.uniq_by(&rem(&1, 2)) |> Enum.sort()
      [1, 2]
  """
  @spec uniq_by(t, (term -> term)) :: t
  def uniq_by(flow, by) when is_function(by, 1) do
    add_operation(flow, {:uniq, by})
  end

  @doc """
  Controls which values should be emitted from now.

  The argument can be either `:events`, `:state` or `:nothing`.
  This step must be called after the reduce operation and it will
  guarantee the state is a list that can be sent downstream.

  Most commonly `:events` is used and each partition will emit the events it has
  processed to the next stages. However, sometimes we want
  to emit counters or other data structures as a result of
  our computations. In such cases, the emit argument can be
  set to `:state`, to return the `:state` from `reduce/3`
  or `map_state/2` or even the processed collection as a whole. The
  argument value of `:nothing` is used by `run/1` and `start_link/2`.
  """
  @spec emit(t, :events | :state | :nothing) :: t | Enumerable.t
  def emit(flow, :events) do
    flow
  end
  def emit(flow, :state) do
    unless has_reduce?(flow) do
      raise ArgumentError, "emit/2 must be called after a group_by/reduce operation"
    end
    map_state(flow, fn acc, _, _ -> [acc] end)
  end
  def emit(%{operations: operations} = flow, :nothing) do
    case inject_to_nothing(operations) do
      :map_state -> map_state(flow, fn _, _, _ -> [] end)
      :reduce -> reduce(flow, fn -> [] end, fn _, acc -> acc end)
    end
  end
  def emit(_, emit) do
    raise ArgumentError, "unknown option for emit: #{inspect emit}"
  end

  defp inject_to_nothing([{:reduce, _, _} | _]), do: :map_state
  defp inject_to_nothing([_ | ops]), do: inject_to_nothing(ops)
  defp inject_to_nothing([]), do: :reduce

  @doc """
  Applies the given function over the window state.

  This function must be called after `reduce/3` as it maps over
  the state accumulated by `reduce/3`. `map_state/2` is invoked
  per window on every stage whenever there is a trigger: this
  gives us an understanding of the window data while leveraging
  the parallelism between stages.

  ## The mapper function

  The `mapper` function may have arity 1, 2 or 3.

  The first argument is the state.

  The second argument is optional and contains the partition index.
  The partition index is a two-element tuple identifying the current
  partition and the total number of partitions as the second element. For
  example, for a partition with 4 stages, the partition index will be
  the values `{0, 4}`, `{1, 4}`, `{2, 4}` and `{3, 4}`.

  The third argument is optional and contains the window-trigger information.
  This information is a three-element tuple containing the window name,
  the window identifier, and the trigger name. For example, a global window
  created with `Flow.Window.global/0` will emit on termination:

      {:global, :global, :done}

  A `Flow.Window.global/0` window with a count trigger created with
  `Flow.Window.trigger_every/2` will also emit:

      {:global, :global, {:every, 20}}

  A `Flow.Window.fixed/3` window will emit on done:

      {:fixed, window, :done}

  Where `window` is an integer identifying the timestamp for the window
  being triggered.

  The value returned by the `mapper` function is passed forward to the
  upcoming flow functions.

  ## Examples

  We can use `map_state/2` to transform the collection after
  processing. For example, if we want to count the amount of
  unique letters in a sentence, we can partition the data,
  then reduce over the unique entries and finally return the
  size of each stage, summing it all:

      iex> flow = Flow.from_enumerable(["the quick brown fox"]) |> Flow.flat_map(fn word ->
      ...>    String.graphemes(word)
      ...> end)
      iex> flow = Flow.partition(flow)
      iex> flow = Flow.reduce(flow, fn -> %{} end, &Map.put(&2, &1, true))
      iex> flow |> Flow.map_state(fn map -> map_size(map) end) |> Flow.emit(:state) |> Enum.sum()
      16

  """
  @spec map_state(t, (term -> term) |
                     (term, term -> term) |
                     (term, term, {Flow.Window.type, Flow.Window.id, Flow.Window.trigger} -> term)) :: t
  def map_state(flow, mapper) when is_function(mapper, 3) do
    do_map_state(flow, mapper)
  end
  def map_state(flow, mapper) when is_function(mapper, 2) do
    do_map_state(flow, fn acc, index, _ -> mapper.(acc, index) end)
  end
  def map_state(flow, mapper) when is_function(mapper, 1) do
    do_map_state(flow, fn acc, _, _ -> mapper.(acc) end)
  end
  defp do_map_state(flow, mapper) do
    unless has_reduce?(flow) do
      raise ArgumentError, "map_state/2 must be called after a group_by/reduce operation"
    end
    add_operation(flow, {:map_state, mapper})
  end

  @doc """
  Applies the given function over the stage state without changing its value.

  It is similar to `map_state/2` except that the value returned by `mapper`
  is ignored.

      iex> parent = self()
      iex> flow = Flow.from_enumerable(["the quick brown fox"]) |> Flow.flat_map(fn word ->
      ...>    String.graphemes(word)
      ...> end)
      iex> flow = flow |> Flow.partition(stages: 2) |> Flow.reduce(fn -> %{} end, &Map.put(&2, &1, true))
      iex> flow = flow |> Flow.each_state(fn map -> send(parent, map_size(map)) end)
      iex> Flow.run(flow)
      iex> receive do
      ...>   6 -> :ok
      ...> end
      :ok
      iex> receive do
      ...>   10 -> :ok
      ...> end
      :ok

  """
  @spec each_state(t, (term -> term) |
                      (term, term -> term) |
                      (term, term, {Flow.Window.type, Flow.Window.id, Flow.Window.trigger} -> term)) :: t
  def each_state(flow, mapper) when is_function(mapper, 3) do
    do_each_state(flow, fn acc, index, trigger -> mapper.(acc, index, trigger); acc end)
  end
  def each_state(flow, mapper) when is_function(mapper, 2) do
    do_each_state(flow, fn acc, index, _ -> mapper.(acc, index); acc end)
  end
  def each_state(flow, mapper) when is_function(mapper, 1) do
    do_each_state(flow, fn acc, _, _ -> mapper.(acc); acc end)
  end
  defp do_each_state(flow, mapper) do
    unless has_reduce?(flow) do
      raise ArgumentError, "each_state/2 must be called after a group_by/reduce operation"
    end
    add_operation(flow, {:map_state, mapper})
  end

  defp add_operation(%Flow{operations: operations} = flow, operation) do
    %{flow | operations: [operation | operations]}
  end
  defp add_operation(flow, _producers) do
    raise ArgumentError, "expected a flow as argument, got: #{inspect flow}"
  end

  defp has_reduce?(%{operations: operations}) do
    Enum.any?(operations, &match?({:reduce,_, _}, &1))
  end

  defimpl Enumerable do
    def reduce(flow, acc, fun) do
      case Flow.Coordinator.start(flow, :producer_consumer, [], [demand: :accumulate]) do
        {:ok, pid} ->
          Flow.Coordinator.stream(pid).(acc, fun)
        {:error, reason} ->
          exit({reason, {__MODULE__, :reduce, [flow, acc, fun]}})
      end
    end

    def count(_flow) do
      {:error, __MODULE__}
    end

    def member?(_flow, _value) do
      {:error, __MODULE__}
    end
  end
end
