defmodule Flow do
  @moduledoc ~S"""
  Computational flows with stages.

  `Flow` allows developers to express computations
  on collections, similar to the `Enum` and `Stream` modules,
  although computations will be executed in parallel using
  multiple `GenStage`s.

  Flow is designed to work with both bounded (finite) and
  unbounded (infinite) data. By default, Flow will work
  with batches of 500 items. This means Flow will only show
  improvements when working with larger collections. However,
  for certain cases, such as IO-bound flows, a smaller batch size
  can be configured through the `:min_demand` and `:max_demand`
  options supported by `from_enumerable/2`, `from_stages/2`,
  `from_specs/2`, `partition/2`, `departition/5`, etc.

  Flow also provides the concepts of "windows" and "triggers",
  which allow developers to split the data into arbitrary
  windows according to event time. Triggers allow computations
  to be materialized at different intervals, allowing developers
  to peek at results as they are computed.

  This module doc will cover the main constructs and concepts behind
  Flow, with examples. There is also a presentation about GenStage
  and Flow from José Valim at ElixirConf 2016, which covers
  data processing concepts for those unfamiliar with the domain:
  <https://youtu.be/srtMWzyqdp8?t=244>

  ## Example

  As an example, let's implement the classic word counting
  algorithm using Flow. The word counting program will receive
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
    2. We call `partition/2` so words are properly partitioned between stages

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
  `partition/2` back:

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

  The answer here lies in triggers. Every partition may have a
  `on_trigger/2` callback which receives the partition accumulator
  and returns the events to be emitted and the accumulator to be
  used after the trigger. All flows have at least one trigger:
  the `:done` trigger which is executed when all the data has
  been processed. In this case, the accumulator returned by
  `on_trigger/2` won't be used, only the events it emits.

  However, Flow provides many conveniences for working with
  unbound data, allowing us to set windows, time-based triggers,
  element counters and more.

  ## Data completion, windows and triggers

  By default, Flow shuts down its processes when all data has been
  processed. However, when working with an unbounded stream of data,
  there is no such thing as data completion. So when can we consider
  a reduce function to be "completed"?

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
  the data is bounded or unbounded. Every time a trigger is invoked,
  the `on_trigger/2` callback of that partition is invoked, allowing
  us to control which events to emit and what accumulator to use for
  the next time the partition starts reducing data.

  Windows and triggers effectively control how the `reduce/3` function
  works. While windows and triggers allow us to control when data is
  emitted, note that data can be emitted at any time during the reducing
  step by using `emit_and_reduce/3`. In truth, all window and trigger
  functionality provided by Flow can also be built by hand using the
  `emit_and_reduce/3` and `on_trigger/2` functions.

  In a nutshell, each stage in Flow goes through those steps:

    * mapping and filtering (`map/2`, `filter/2`, `flat_map/2`)
    * reducing (`reduce/3`, `group_by/3`, `emit_and_reduce/3`)
    * emitting events (`emit_and_reduce/3`, `emit/2`, `on_trigger/2`)

  The accumulator from reducing operations is shared with the one
  from emitting events. `emit_and_reduce/3` is special operation
  that allows both emitting and reducing events in one step.

  See `Flow.Window` for a complete introduction to windows and triggers.

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
  the flow as part of a supervision tree using `{Flow, your_flow}`
  as your child specification:

      children = [
        {Flow,
         Flow.from_stages(...)
         |> Flow.flat_map(&String.split(&1, " "))
         |> Flow.reduce(fn -> %{} end, fn word, acc ->
           Map.update(acc, word, 1, & &1 + 1)
         end)}
      ]

  It is also possible to move a Flow to its own module. This is done by
  calling `use Flow` and then defining a `start_link/1` function that
  calls `Flow.start_link/1` at the end:

      defmodule MyFlow do
        use Flow

        def start_link(_) do
          Flow.from_stages(...)
          |> Flow.flat_map(&String.split(&1, " "))
          |> Flow.reduce(fn -> %{} end, fn word, acc ->
            Map.update(acc, word, 1, & &1 + 1)
          end)
          |> Flow.start_link()
        end
      end

  By the default the `Flow` is permanent, which means it is always
  restarted. The `:shutdown` and `:restart` child spec configurations
  can be given to `use Flow`.

  Flow also provides integration with `GenStage`, allowing you to
  specify child specifications of producers, producer consumers, and
  consumers that are started alongside the flow and under the same
  supervision tree. This is achieved with the `from_specs/2` (producers),
  `through_specs/2` (producer consumers) and `into_specs/2` (consumers)
  functions.

  It is also possible to connect a flow to already running stages,
  via the `from_stages/2` (producers), `through_stages/2` (producer
  consumers) and `into_stages/2` (consumers) functions.

  `into_stages/3` and `into_specs/3` are alternatives to `start_link/1`
  that start the flow with the given consumers stages or the given
  consumers child specification. Similar to `start_link/1`, they return
  either `{:ok, pid}` or `{:error, reason}`.

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
      |> Flow.on_trigger(fn ets ->
        :ets.give_away(ets, parent, [])
        {[ets], :new_reduce_state_which_wont_be_used} # Emit the ETS
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
      call `on_trigger/2` to transfer the ETS table to the parent process
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

  @type t :: %Flow{
          producers: producers,
          operations: [operation],
          options: keyword(),
          window: Flow.Window.t()
        }

  @type join :: :inner | :left_outer | :right_outer | :full_outer

  @typep producers ::
           nil
           | {:from_stages, (fun() -> [{GenStage.stage(), keyword}])}
           | {:through_stages, t, (fun() -> [{GenStage.stage(), keyword}])}
           | {:enumerables, Enumerable.t()}
           | {:join, join, t, t, fun(), fun(), fun()}
           | {:departition, t, fun(), fun(), fun()}
           | {:flows, [t]}

  @typep operation ::
           {:mapper, atom(), [term()]}
           | {:uniq, fun()}
           | {:reduce, fun(), fun()}
           | {:emit_and_reduce, fun(), fun()}
           | {:on_trigger, fun()}

  @doc false
  defmacro __using__(opts) do
    quote location: :keep, bind_quoted: [opts: opts] do
      @doc false
      def child_spec(arg) do
        default = %{
          id: __MODULE__,
          start: {__MODULE__, :start_link, [arg]}
        }

        Supervisor.child_spec(default, unquote(Macro.escape(opts)))
      end

      defoverridable child_spec: 1
    end
  end

  @doc false
  def child_spec(%Flow{} = arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  ## Building

  @doc """
  Creates a flow with the given enumerable as the producer.

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
  @spec from_enumerable(Enumerable.t(), keyword()) :: t
  def from_enumerable(enumerable, options \\ [])

  def from_enumerable(%Flow{}, _options) do
    raise ArgumentError,
          "passing a Flow to Flow.from_enumerable/2 is not supported. " <>
            "Did you mean to use Flow.partition/2 or Flow.merge/2?"
  end

  def from_enumerable(enumerable, options) do
    from_enumerables([enumerable], options)
  end

  @doc """
  Creates a flow with the given enumerable as producer.

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
    * `:shutdown` - the shutdown time for this stage when the flow is shut down.
      The same as the `:shutdown` value in a Supervisor, defaults to 5000 milliseconds.
    * `:on_init` - a function invoked during the initialization of each stage.
      The function receives a single argument in the form of `{i, total}` where:
      - `i` is the stage index
      - `total` is the total number of stages

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.

  ## Examples

      files = [File.stream!("some/file1", read_ahead: 100_000),
               File.stream!("some/file2", read_ahead: 100_000),
               File.stream!("some/file3", read_ahead: 100_000)]
      Flow.from_enumerables(files)
  """
  @spec from_enumerables([Enumerable.t()], keyword()) :: t
  def from_enumerables(enumerables, options \\ [])

  def from_enumerables([_ | _] = enumerables, options) do
    options = stages(options)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global())
    %Flow{producers: {:enumerables, enumerables}, options: options, window: window}
  end

  def from_enumerables(enumerables, _options) do
    raise ArgumentError,
          "from_enumerables/2 expects a non-empty list as argument, got: #{inspect(enumerables)}"
  end

  @doc """
  Creates a flow with a list of already running stages as `producers`.

  `producers` are already running stages that have type `:producer`
   If instead you want the producers to be started alongside the flow,
   see `from_specs/2` instead.

  ## Options

  These options configure the stages connected to producers before partitioning.

    * `:window` - a window to run the next stages in, see `Flow.Window`
    * `:stages` - the number of stages
    * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
    * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`
    * `:shutdown` - the shutdown time for this stage when the flow is shut down.
      The same as the `:shutdown` value in a Supervisor, defaults to 5000 milliseconds.

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.

  ## Examples

      stages = [pid1, pid2, pid3]
      Flow.from_stages(stages)

  ## Termination

  Flow subscribes to producer stages using `cancel: :transient`. This
  means producer stages can signal the flow that it has emitted all events
  by terminating with reason `:normal`, `:shutdown` or `{:shutdown, _}`.
  Therefore, if you are implementing a producer that may eventually
  terminate, then the producer must exit with reason `:normal`, `:shutdown`
  or `{:shutdown, _}` after emitting all events. This is often done in the
  producer by using `GenStage.async_info(self(), :terminate)` to send a
  message to itself once all events have been dispatched:

      def handle_info(:terminate, state) do
        {:stop, :shutdown, state}
      end

  Once all producers have finished, the stages subscribed to the producer
  will terminate, causing the next layer of stages in the flow to terminate
  and so forth, until the whole flow shuts down.

  If the exit reason is none of the above, it will cause the next stages to
  terminate immediately, eventually causing the whole flow to terminate.
  """
  @spec from_stages([GenStage.stage()], keyword) :: t
  def from_stages(producers, options \\ [])

  def from_stages([_ | _] = producers, options) do
    options = stages(options)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global())
    producers = Enum.map(producers, &{&1, []})

    %Flow{
      producers: {:from_stages, fn _ -> producers end},
      options: options,
      window: window
    }
  end

  def from_stages(producers, _options) do
    raise ArgumentError,
          "from_stages/2 expects a non-empty list of stages as argument, " <>
            "got: #{inspect(producers)}"
  end

  @doc """
  Creates a flow with a list of `producers` child specifications.

  The child specification is the one defined in the `Supervisor`
  module. The `producers` will only be started when the flow starts.
  If the flow terminates, the producers will also be terminated.

  The `:id` field of the child specification will be randomized.
  The `:restart` option is set to `:temporary` but it behaves
  as `:transient`. If a producer terminates, its exit reason will
  propagate through the flow. The exit is considered abnormal
  unless the reason is `:normal`, `:shutdown` or `{:shutdown, _}`.
  All other child specification fields are kept unchanged.

  For options and termination behaviour, see `from_stages/2`.

  ## Examples

      specs = [{MyProducer, arg1}, {MyProducer, arg2}]
      Flow.from_specs(specs)

  """
  @spec from_specs([Supervisor.child_spec() | {module(), term()} | module()], keyword()) :: t
  def from_specs(producers, options \\ [])

  def from_specs([_ | _] = producers, options) do
    options = stages(options)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global())

    fun = fn start_link ->
      for producer <- producers do
        {:ok, pid} = start_link.(producer)
        {pid, []}
      end
    end

    %Flow{
      producers: {:from_stages, fun},
      options: options,
      window: window
    }
  end

  def from_specs(producers, _options) do
    raise ArgumentError,
          "from_specs/2 expects a non-empty list of Supervisor child specs " <>
            "of stages as argument, got: #{inspect(producers)}"
  end

  @doc """
  Passes a `flow` through a list of already running stages
  as `producer_consumers`.

  `producers_consumers` are already running stages that have type
  `:producer_consumer`. Each element represents the consumer or a
  tuple with the consumer and the subscription options as defined
  in `GenStage.sync_subscribe/2`. If instead you want the producer
  consumers to be started alongside the flow, see `through_specs/3`
  instead.

  You are required to pass an existing `flow` and it returns a new
  `flow` that you can continue processing.

  ## Options

  These options configure the stages after the producer consumers:

    * `:window` - a window to run the next stages in, see `Flow.Window`
    * `:stages` - the number of stages
    * `:buffer_keep` - how the buffer should behave, see `c:GenStage.init/1`
    * `:buffer_size` - how many events to buffer, see `c:GenStage.init/1`
    * `:shutdown` - the shutdown time for this stage when the flow is shut down.
      The same as the `:shutdown` value in a Supervisor, defaults to 5000 milliseconds.

  All remaining options are sent during subscription, allowing developers
  to customize `:min_demand`, `:max_demand` and others.

  ## Examples

      stages = [{pid1, min_demand: 10}, pid2, SomeProducerConsumer]
      Flow.from_enumerable([1, 2, 3])
      |> Flow.through_stages(stages)
      |> Flow.start_link()

  ## Termination

  Flow subscribes to stages using `cancel: :transient`. This means stages
  can signal the flow that it has emitted all events by terminating with
  reason `:normal`, `:shutdown` or `{:shutdown, _}`. If you are implementing
  your own producer consumer and you are subscribing to a flow that is finite,
  you need to take this into account in your producer consumer implementation:

    1. You need implement `c:GenStage.handle_subscribe/4` and store
       whenever the stage gets a new producer

    2. You need implement `c:GenStage.handle_cancel/3` and decrease
       whenever the stage loses a producer

    3. Once all producers are cancelled, you need to call
       `GenStage.async_info(self(), :terminate)` to send a message
       to yourself, allowing you to terminate after all events have
       been consumed:

       ```elixir
       def handle_info(:terminate, state) do
         {:stop, :shutdown, state}
       end
       ```

  Given the complexity in guaranteeing termination, we recommend
  developers to use `through_stages/3` and `through_specs/3` only
  when subscribing to unbounded (infinite) flows.

  If the exit reason is none of the above, it will cause the next stages
  to terminate immediately, eventually causing the whole flow to terminate.
  """
  @spec through_stages(t, producer_consumers, keyword()) :: t
        when producer_consumers: [GenStage.stage() | {GenStage.stage(), keyword()}]
  def through_stages(flow, producer_consumers, options \\ [])

  def through_stages(%Flow{} = flow, [_ | _] = producer_consumers, options) do
    options = stages(options)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global())

    %Flow{
      producers: {:through_stages, flow, normalize_stages(producer_consumers)},
      options: options,
      window: window
    }
  end

  def through_stages(%Flow{}, producers_consumers, _options) do
    raise ArgumentError,
          "through_stages/2 expects a non-empty list of stages as argument, " <>
            "got: #{inspect(producers_consumers)}"
  end

  @doc """
  Passes a `flow` through a list of `producer_consumers` child
  specifications and subscriptions that will be started alongside
  the flow.

  `producers_consumers` is a list of tuples where the first element
  is the child specification and the second is a list of subscription
  options. The child specification is the one defined in the `Supervisor`
  module. The `producers_consumers` will only be started when the flow
  starts. If the flow terminates, the producer consumers will also be
  terminated.

  The `:id` field of the child specification will be randomized.
  The `:restart` option is set to `:temporary` but it behaves
  as `:transient`. If a producer terminates, its exit reason will
  propagate through the flow. The exit is considered abnormal
  unless the reason is `:normal`, `:shutdown` or `{:shutdown, _}`.
  All other child specification fields are kept unchanged.

  For options and termination behaviour, see `through_stages/3`.

  ## Examples

      spec = {MyConsumerProducer, arg}
      subscription_opts = []
      specs = [{spec, subscription_opts}]
      Flow.through_specs(some_flow, specs)

  """
  @spec through_specs(t, [{Supervisor.child_spec(), keyword()}], keyword()) :: t
  def through_specs(flow, producer_consumers, options \\ [])

  def through_specs(%Flow{} = flow, [_ | _] = producer_consumers, options) do
    options = stages(options)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global())

    %Flow{
      producers: {:through_stages, flow, normalize_specs(producer_consumers)},
      options: options,
      window: window
    }
  end

  def through_specs(%Flow{}, producers_consumers, _options) do
    raise ArgumentError,
          "through_specs/2 expects a non-empty list of Supervisor child specs " <>
            " of stages as argument, got: #{inspect(producers_consumers)}"
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
  @spec bounded_join(join, t, t, fun(), fun(), fun(), keyword()) :: t
  def bounded_join(
        mode,
        %Flow{} = left,
        %Flow{} = right,
        left_key,
        right_key,
        join,
        options \\ []
      )
      when is_function(left_key, 1) and is_function(right_key, 1) and is_function(join, 2) and
             mode in @joins do
    window_join(mode, left, right, Flow.Window.global(), left_key, right_key, join, options)
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
  @spec window_join(join, t, t, Flow.Window.t(), fun(), fun(), fun(), keyword()) :: t
  def window_join(
        mode,
        %Flow{} = left,
        %Flow{} = right,
        %{} = window,
        left_key,
        right_key,
        join,
        options \\ []
      )
      when is_function(left_key, 1) and is_function(right_key, 1) and is_function(join, 2) and
             mode in @joins do
    options = stages(options)

    %Flow{
      producers: {:join, mode, left, right, left_key, right_key, join},
      options: options,
      window: window
    }
  end

  @doc """
  Runs a given flow.

  This runs the given flow as a stream for its side-effects. No
  items are sent from the flow to the current process.

  ## Options

    * `:link` - if the Flow supervision tree should be linked
      to the current process. Defaults to `true`.

  ## Examples

      iex> parent = self()
      iex> [1, 2, 3] |> Flow.from_enumerable() |> Flow.map(&send(parent, &1)) |> Flow.run()
      :ok
      iex> receive do
      ...>   1 -> :ok
      ...> end
      :ok

  """
  @spec run(t) :: :ok
  def run(flow, opts \\ []) do
    [] = flow |> emit_nothing() |> stream(opts) |> Enum.to_list()
    :ok
  end

  @doc """
  Explicitly converts the Flow into a Stream.

  All Flows behave as a stream but this function performs an
  explicit conversion. However, since Flow will link to the
  current process, this function can be useful to convert it
  to a non-linked stream by passing the `link: false` option.

  ## Options

    * `:link` - if the Flow supervision tree should be linked
      to the current process. Defaults to `true`.

  ## Examples

      iex> Flow.from_enumerable([1, 2, 3])
      ...> |> Flow.map(& &1 * 2)
      ...> |> Flow.stream()
      ...> |> Enum.to_list()
      [2, 4, 6]

  """
  def stream(flow, opts \\ []) do
    start = if Keyword.get(opts, :link, true), do: :start_link, else: :start

    fn acc, fun ->
      opts = [demand: :accumulate]
      args = [flow, :producer_consumer, {:outer, fn _ -> [] end}, opts]

      case apply(Flow.Coordinator, start, args) do
        {:ok, pid} ->
          Flow.Coordinator.stream(pid).(acc, fun)

        {:error, reason} ->
          exit({reason, {__MODULE__, :stream, [flow, acc, fun]}})

        :ignore ->
          acc
      end
    end
  end

  @doc """
  Starts and runs the flow as a separate process.

  See `into_stages/3` in case you want the flow to
  work as a producer for another series of stages.

  ## Options

    * `:name` - the name of the flow

    * `:demand` - configures the demand on the flow producers to `:forward`
      or `:accumulate`. The default is `:forward`. See `GenStage.demand/2`
      for more information.

    * `:subscribe_timeout` - timeout for the subscription between stages
      when setting up the flow. Defaults to `5_000` milliseconds.

  The flow exits with reason `:normal` only if all consumers exit with
  reason `:normal`. Otherwise exits with reason `:shutdown`.
  """
  @spec start_link(t, keyword()) :: GenServer.on_start()
  def start_link(flow, options \\ []) do
    Flow.Coordinator.start_link(emit_nothing(flow), :consumer, {:outer, fn _ -> [] end}, options)
  end

  @doc """
  Starts a flow with a list of already running stages as `consumers`.

  `consumers` is a list of already running stages that have type
  `:consumer` or `:producer_consumer`. Each element represents the
  consumer or a tuple with the consumer and the subscription options
  as defined in `GenStage.sync_subscribe/2`.

  The consumer stages given to this function won't be managed
  by Flow. If the Flow terminates, they will continue running.
  If instead you want the consumers to be started and managed
  alongside the flow, use `into_specs/3` instead.

  The `pid` returned by this function identifies a coordinator
  process. While it is possible to send subscribe requests to
  the coordinator process, the coordinator process will simply
  redirect the subscription to the proper flow processes and
  cancel the initial subscription. This means subscriptions
  to the flow should use at `cancel: :transient` (which is
  the default for stage subscriptions).

  The coordinator exits with reason `:normal` only if all
  consumers exit with reason `:normal`. Otherwise exits with
  reason `:shutdown`.

  ## Options

  This function receives the same options as `start_link/2` with
  the addition of a `:dispatcher` option that configures how the
  consumers get data from the flow and defaults to
  `GenStage.DemandDispatch`. It may be either an atom or a tuple
  with the dispatcher and the dispatcher options.

  ## Termination

  Flow subscribes to stages using `cancel: :transient`. This means stages
  can signal the flow that it has emitted all events by terminating with
  reason `:normal`, `:shutdown` or `{:shutdown, _}`. If you are implementing
  your own consumer and you are subscribing to a flow that is finite,
  you need to take this into account in your consumer implementation
  if you want proper consumer termination:

    1. You need implement `c:GenStage.handle_subscribe/4` and store
       whenever the stage gets a new producer

    2. You need implement `c:GenStage.handle_cancel/3` and decrease
       whenever the stage loses a producer

    3. Once all producers are cancelled, you can terminate:

       ```elixir
       def handle_info(:terminate, state) do
         {:stop, :shutdown, state}
       end
       ```

  Given the complexity in guaranteeing termination, we recommend
  developers to use `into_stages/3` and `into_specs/3` only
  when subscribing to unbounded (infinite) flows.
  """
  @spec into_stages(t, consumers, keyword()) :: GenServer.on_start()
        when consumers: [GenStage.stage() | {GenStage.stage(), keyword()}]
  def into_stages(flow, consumers, options \\ []) do
    Flow.Coordinator.start_link(
      flow,
      :producer_consumer,
      {:outer, normalize_stages(consumers)},
      options
    )
  end

  @doc """
  Starts a flow and the `consumers` child specifications.

  `consumers` is a list of tuples where the first element is the child
  specification and the second is a list of subscription options.
  The child specification is the one defined in the `Supervisor`
  module. The `consumers` will only be started when the flow starts.
  If the flow terminates, the consumers will also be terminated.

  The `:id` field of the child specification will be randomized.
  All other fields are kept as in. If the consumer terminates,
  it will behave according to its restart strategy. Once a consumer
  terminates, the whole flow is terminated.

  For options and termination behaviour, see `into_stages/3`.

  ## Examples

      spec = {MyConsumer, arg}
      subscription_opts = []
      specs = [{spec, subscription_opts}]
      Flow.into_specs(some_flow, specs)

  """
  @spec into_specs(t, [{Supervisor.child_spec(), keyword()}], keyword()) :: GenServer.on_start()
  def into_specs(flow, consumers, options \\ []) do
    Flow.Coordinator.start_link(
      flow,
      :producer_consumer,
      {:inner, normalize_specs(consumers)},
      options
    )
  end

  ## Mappers

  @deprecated "Use Flow.map/2 returning the input instead"
  def each(flow, each) when is_function(each, 1) do
    add_mapper(flow, :each, [each])
  end

  @doc """
  Applies the given function filtering each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.filter(&(rem(&1, 2) == 0))
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [2]

  """
  @spec filter(t, (term -> term)) :: t
  def filter(flow, filter) when is_function(filter, 1) do
    add_mapper(flow, :filter, [filter])
  end

  @doc """
  Applies the given function mapping each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.map(&(&1 * 2))
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [2, 4, 6]

      iex> flow = Flow.from_enumerables([[1, 2, 3], 1..3]) |> Flow.map(&(&1 * 2))
      iex> Enum.sort(flow)
      [2, 2, 4, 4, 6, 6]

  """
  @spec map(t, (term -> term)) :: t
  def map(flow, mapper) when is_function(mapper, 1) do
    add_mapper(flow, :map, [mapper])
  end

  @doc """
  Maps over the given values in the stage state.

  It is expected the state to emit two-elements tuples,
  such as list, maps, etc.

  ## Examples

      iex> flow = Flow.from_enumerable([a: 1, b: 2, c: 3, d: 4, e: 5], stages: 1)
      iex> flow |> Flow.map_values(& &1 * 2) |> Enum.sort()
      [a: 2, b: 4, c: 6, d: 8, e: 10]

  """
  def map_values(flow, value_fun) when is_function(value_fun) do
    map(flow, fn {key, value} -> {key, value_fun.(value)} end)
  end

  @doc """
  Applies the given function mapping each input in parallel and
  flattening the result, but only one level deep.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.flat_map(fn x -> [x, x * 2] end)
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [1, 2, 2, 3, 4, 6]

  """
  @spec flat_map(t, (term -> Enumerable.t())) :: t
  def flat_map(flow, flat_mapper) when is_function(flat_mapper, 1) do
    add_mapper(flow, :flat_map, [flat_mapper])
  end

  @doc """
  Applies the given function rejecting each input in parallel.

  ## Examples

      iex> flow = [1, 2, 3] |> Flow.from_enumerable() |> Flow.reject(&(rem(&1, 2) == 0))
      iex> Enum.sort(flow) # Call sort as we have no order guarantee
      [1, 3]

  """
  @spec reject(t, (term -> term)) :: t
  def reject(flow, filter) when is_function(filter, 1) do
    add_mapper(flow, :reject, [filter])
  end

  @doc """
  Applies the given function to each "batch" of GenStage events.

  Flow uses GenStage which sends events in batches, controlled by
  `min_demand` and `max_demand`. This callback allows you to hook
  into this batch, before any `map` or `reduce` operation is invoked.
  This often useful to preload data that is used in later stages.
  """
  def map_batch(flow, function) when is_function(function, 1) do
    case flow.operations do
      [] ->
        add_operation(flow, {:batch, function})

      [{:batch, _} | _] ->
        add_operation(flow, {:batch, function})

      [_ | _] ->
        raise ArgumentError,
              "map_batch/2 can only be called at the beginning of the stage/partition, " <>
                "before any map or reduce operation"
    end
  end

  ## Reducers

  @doc """
  Creates a new partition for the given flow (or flows) with the given options.

  Every time this function is called, a new partition is created.
  It is typically recommended to invoke it before a reducing function,
  such as `reduce/3`, so data belonging to the same partition can be
  kept together.

  However, notice that unnecessary partitioning will increase memory
  usage and reduce throughput with no benefit whatsoever. Flow takes
  care of using all cores regardless of the number of times you call
  partition. You should only partition when the problem you are trying
  to solve requires you to route the data around. Such as the problem
  presented in `Flow`'s module documentation. If you can solve a problem
  without using partition at all, that is typically preferred. Those
  are typically called "embarrassingly parallel" problems.

  ## Examples

      flow |> Flow.partition(window: Flow.Window.global)
      flow |> Flow.partition(stages: 4)

  ## Options

    * `:window` - a `Flow.Window` struct which controls how the
       reducing function behaves, see `Flow.Window` for more information.
    * `:stages` - the number of partitions (reducer stages)
    * `:shutdown` - the shutdown time for this stage when the flow is shut down.
      The same as the `:shutdown` value in a Supervisor, defaults to 5000 milliseconds.
    * `:key` - the key to use when partitioning. It is a function
      that receives a single argument (the event) and must return its key.
      The key will then be hashed by Flow. To facilitate customization, `:key`
      also allows common values, such as `{:elem, integer}` and `{:key, atom}`,
      to calculate the hash based on a tuple or a map field. See the "Key shortcuts"
      section below
    * `:hash` - the hashing function. By default a hashing function is built
      on the key but a custom one may be specified as described in
      `GenStage.PartitionDispatcher`
    * `:min_demand` - the minimum demand for this subscription
    * `:max_demand` - the maximum demand for this subscription

  ## Key shortcuts

  The following shortcuts can be given to the `:key` option:

    * `{:elem, index}` - apply the hash function to the element
      at `index` (zero-based) in the given tuple

    * `{:key, key}` - apply the hash function to the key of a given map

  """
  @spec partition(t | [t], keyword()) :: t
  def partition(flow_or_flows, options \\ []) when is_list(options) do
    merge(flow_or_flows, GenStage.PartitionDispatcher, options)
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

  A set of options may also be given to customize the `:window`,
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
      |> Flow.partition(window: Flow.Window.global |> Flow.Window.trigger_every(1000))
      |> Flow.reduce(fn -> %{} end, fn event, acc -> Map.update(acc, event, 1, & &1 + 1) end)
      |> Flow.on_trigger(fn acc -> {[acc], %{}} end)
      |> Flow.departition(&Map.new/0, &Map.merge(&1, &2, fn _, v1, v2 -> v1 + v2 end), &(&1))
      |> Enum.to_list

  Each approach is going to have different performance characteristics
  and it is important to measure to verify which one will be more efficient
  to the problem at hand.
  """
  def departition(%Flow{} = flow, acc_fun, merge_fun, done_fun, options \\ [])
      when is_function(acc_fun, 0) and is_function(merge_fun, 2) and
             (is_function(done_fun, 1) or is_function(done_fun, 2)) do
    unless has_any_reduce?(flow) do
      raise ArgumentError,
            "departition/5 must be called after a group_by/reduce/emit_and_reduce operation " <>
              "as it works on the accumulated state"
    end

    done_fun =
      if is_function(done_fun, 1) do
        fn acc, _ -> done_fun.(acc) end
      else
        done_fun
      end

    flow =
      inject_on_trigger(
        flow,
        fn events, {partition, _}, trigger -> Enum.map(events, &{&1, partition, trigger}) end,
        fn acc, {partition, _}, trigger -> [{acc, partition, trigger}] end
      )

    build_departition(flow, acc_fun, merge_fun, done_fun, options)
  end

  defp build_departition(flow, acc_fun, merge_fun, done_fun, options) do
    {window, options} =
      options
      |> Keyword.put(:stages, 1)
      |> Keyword.pop(:window, Flow.Window.global())

    %Flow{
      producers: {:departition, flow, acc_fun, merge_fun, done_fun},
      options: options,
      window: window
    }
  end

  @doc """
  Shuffles the given flow (or flows) into a new series of stages.

  This function defines a new series of stages with the given window
  and options using `GenStage.DemandDispatcher` to coordinate the
  demand between them. This function does not shuffle the data by
  itself. However, given the concurrent nature of Flow, adding new
  stages often have the indirect consequence of shuffling data too.

  ## Examples

      Flow.shuffle(flow1, window: Flow.Window.global)
      Flow.shuffle([flow1, flow2], stages: 4)

  ## Options

    * `:window` - a `Flow.Window` struct which controls how the
       reducing function behaves, see `Flow.Window` for more information.
    * `:stages` - the number of partitions (reducer stages)
    * `:shutdown` - the shutdown time for this stage when the flow is shut down.
      The same as the `:shutdown` value in a Supervisor, defaults to 5000 milliseconds.

  """
  @spec shuffle(t | [t], keyword()) :: t
  def shuffle(flow_or_flows, options \\ []) when is_list(options) do
    merge(flow_or_flows, GenStage.DemandDispatcher, options)
  end

  @doc """
  Merges the given flow or flows into a series of new stages with
  the given dispatcher and options.

  This is the function used as building block by `partition/2` and
  `shuffle/2`.

  ## Options

    * `:window` - a `Flow.Window` struct which controls how the
       reducing function behaves, see `Flow.Window` for more information.
    * `:stages` - the number of partitions (reducer stages)
    * `:shutdown` - the shutdown time for this stage when the flow is shut down.
      The same as the `:shutdown` value in a Supervisor, defaults to 5000 milliseconds.

  """
  def merge(flow_or_flows, dispatcher, options \\ [])

  def merge(%Flow{} = flow, dispatcher, options) when is_list(options) do
    merge([flow], dispatcher, options)
  end

  def merge([%Flow{} | _] = flows, dispatcher, options) when is_list(options) do
    options = options |> stages() |> put_dispatcher(dispatcher)
    {window, options} = Keyword.pop(options, :window, Flow.Window.global())
    %Flow{producers: {:flows, flows}, options: options, window: window}
  end

  def merge(other, _dispatcher, options) when is_list(options) do
    raise ArgumentError,
          "expected a flow or a non-empty list of flows as first argument, got: #{inspect(other)}"
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

  # We let the partition dispatcher be lazily calculated
  # in materialize as it knows all of the partitions.
  defp put_dispatcher(options, GenStage.PartitionDispatcher), do: options
  defp put_dispatcher(options, dispatcher), do: Keyword.put(options, :dispatcher, dispatcher)

  @doc """
  Reduces the given values with the given accumulator.

  `acc_fun` is a function that receives no arguments and returns
  the actual accumulator. The `acc_fun` function is invoked per window
  whenever a new window starts.

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
      has_any_reduce?(flow) ->
        raise ArgumentError,
              "cannot call group_by/reduce/emit_and_reduce on a flow after another " <>
                "group_by/reduce/emit_and_reduce operation (these functions can only be called " <>
                "once per partition, for subsequent transformations, consider using on_trigger/2 instead)"

      is_function(acc_fun, 0) ->
        add_operation(flow, {:reduce, acc_fun, reducer_fun})

      true ->
        raise ArgumentError, "Flow.reduce/3 expects the accumulator to be given as a function"
    end
  end

  @doc """
  Reduces values with the given accumulator and controls which values
  should be emitted.

  `acc_fun` is a function that receives no arguments and returns
  the actual accumulator. The `acc_fun` function is invoked per window
  whenever a new window starts.

  This function behaves similarly to `reduce/3`, but in addition to
  accumulating data, it also gives full control over what will be
  emitted. `reducer_fun` must return a tuple where the first element is
  the list of events to be emitted and the second is the new state of
  the accumulator.

  ## Examples

  As an example this is a simple implementation of a sliding window of
  3 events. The reducer function always emits a list of the most recent
  (at most) 3 events. Note that at the end of the input the current
  state of the accumulator will be emitted which we filter in this
  example at the last step.

      iex> flow = Flow.from_enumerable(1..5, stages: 1)
      iex> flow = flow |> Flow.emit_and_reduce(fn -> [] end, fn event, acc ->
      ...>   acc = [event | acc] |> Enum.take(3)
      ...>   {[Enum.reverse(acc)], acc}
      ...> end)
      iex> flow |> Enum.filter(&is_list/1)
      [[1], [1, 2], [1, 2, 3], [2, 3, 4], [3, 4, 5]]

  """
  @spec emit_and_reduce(t, (() -> acc), (term, acc -> {[event], acc})) :: t
        when acc: term(), event: term()
  def emit_and_reduce(flow, acc_fun, reducer_fun) when is_function(reducer_fun, 2) do
    cond do
      has_any_reduce?(flow) ->
        raise ArgumentError,
              "cannot call group_by/reduce/emit_and_reduce on a flow after another " <>
                "group_by/reduce/emit_and_reduce operation (these functions can only be called " <>
                "once per partition, for subsequent transformations, consider using on_trigger/2 instead)"

      is_function(acc_fun, 0) ->
        add_operation(flow, {:emit_and_reduce, acc_fun, reducer_fun})

      true ->
        raise ArgumentError,
              "Flow.emit_and_reduce/3 expects the accumulator to be given as a function"
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

  `take_sort/3` is built on top of `departition/5`, which means it will
  also take and sort entries across windows. A set of options may also
  be given to customize the `:window`, `:min_demand` and `:max_demand`
  of when departitioning.

  ## Examples

  As an example, imagine you are processing a list of URLs and you want
  the list of the most accessed URLs.

      iex> urls = ~w(www.foo.com www.bar.com www.foo.com www.foo.com www.baz.com)
      iex> flow = urls |> Flow.from_enumerable() |> Flow.partition()
      iex> flow = flow |> Flow.reduce(fn -> %{} end, fn url, map ->
      ...>   Map.update(map, url, 1, & &1 + 1)
      ...> end)
      iex> flow = flow |> Flow.take_sort(1, fn {_url_a, count_a}, {_url_b, count_b} ->
      ...>   count_b <= count_a
      ...> end)
      iex> Enum.to_list(flow)
      [[{"www.foo.com", 3}]]

  """
  def take_sort(flow, n, sort_fun \\ &<=/2, options \\ []) when is_integer(n) and n > 0 do
    unless has_any_reduce?(flow) do
      raise ArgumentError,
            "take_sort/3 must be called after a group_by/reduce/emit_and_reduce operation " <>
              "as it works on the accumulated state"
    end

    flow
    |> inject_on_trigger(fn events, {partition, _}, trigger ->
      [{events |> Enum.sort(sort_fun) |> Enum.take(n), partition, trigger}]
    end)
    |> build_departition(
      fn -> [] end,
      &merge_sorted(&1, &2, n, sort_fun),
      fn acc, _ -> acc end,
      options
    )
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
      iex> flow |> Flow.group_by(&String.length/1) |> Enum.sort()
      [{3, ["fox", "the"]}, {5, ["brown", "quick"]}]

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
      iex> flow |> Flow.group_by_key() |> Flow.emit(:state) |> Enum.to_list()
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
    if has_any_reduce?(flow) do
      raise ArgumentError, "uniq/uniq_by cannot be called after group_by/reduce/emit_and_reduce"
    end

    add_operation(flow, {:uniq, by})
  end

  @doc """
  Controls which values should be emitted.

  The argument can be either `:events`, `:state` or `:nothing`.
  This step must be called after the reduce operation and it will
  guarantee the state is a list that can be sent downstream.

  Most commonly `:events` is used and each partition will emit the
  events it has processed to the next stages. However, sometimes we
  want to emit counters or other data structures as a result of
  our computations. In such cases, the emit argument can be
  set to `:state`, to return the `:state` from `reduce/3` or even
  the processed collection as a whole.
  """
  @spec emit(t, :events | :state | :nothing) :: t | Enumerable.t()
  def emit(flow, type) do
    unless has_any_reduce?(flow) do
      raise ArgumentError,
            "emit/2 must be called after a group_by/reduce/emit_and_reduce operation " <>
              "as it works on the accumulated state"
    end

    if has_on_trigger?(flow) do
      raise ArgumentError, "emit/2 cannot be called after emit/on_trigger"
    end

    case type do
      :events -> flow
      :nothing -> add_operation(flow, {:on_trigger, fn acc, _, _ -> {[], acc} end})
      :state -> add_operation(flow, {:on_trigger, fn acc, _, _ -> {[acc], acc} end})
    end
  end

  @doc """
  Applies the given function over the window state.

  This function must be called after `group_by/3`, `reduce/3` or
  `emit_and_reduce/3` as it works on the accumulated state.
  `on_trigger/2` is invoked per window on every stage whenever
  there is a trigger: this gives us an understanding of the window
  data while leveraging the parallelism between stages.

  The given callback must return a tuple with elements to emit
  and the new accumulator. The new accumulator will then be used
  for subsequent reductions by `reduce/3`, `group_by/3`, and friends.

  ## The callback arguments

  The `callback` function may have arity 1, 2 or 3.

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

  ## Examples

  We can use `on_trigger/2` to transform the collection after
  processing. For example, if we want to count the amount of
  unique letters in a sentence, we can partition the data,
  then reduce over the unique entries and finally return the
  size of each stage, summing it all:

      iex> flow = Flow.from_enumerable(["the quick brown fox"]) |> Flow.flat_map(fn word ->
      ...>    String.graphemes(word)
      ...> end)
      iex> flow = Flow.partition(flow)
      iex> flow = Flow.reduce(flow, fn -> %{} end, &Map.put(&2, &1, true))
      iex> flow |> Flow.on_trigger(fn map -> {[map_size(map)], map} end) |> Enum.sum()
      16

  """
  @spec on_trigger(
          t,
          (acc -> {[event], acc})
          | (acc, partition_info -> {[event], acc})
          | (acc, partition_info, window_info -> {[event], acc})
        ) :: t
        when acc: term,
             event: term,
             partition_info: {non_neg_integer, pos_integer},
             window_info: {Flow.Window.type(), Flow.Window.id(), Flow.Window.trigger()}
  def on_trigger(flow, on_trigger) when is_function(on_trigger, 3) do
    add_on_trigger(flow, fn acc, index, window ->
      validate_on_trigger!(on_trigger.(acc, index, window))
    end)
  end

  def on_trigger(flow, on_trigger) when is_function(on_trigger, 2) do
    add_on_trigger(flow, fn acc, index, _ ->
      validate_on_trigger!(on_trigger.(acc, index))
    end)
  end

  def on_trigger(flow, on_trigger) when is_function(on_trigger, 1) do
    add_on_trigger(flow, fn acc, _, _ ->
      validate_on_trigger!(on_trigger.(acc))
    end)
  end

  defp validate_on_trigger!({events, _} = result) when is_list(events) do
    result
  end

  defp validate_on_trigger!(other) do
    raise "expected on_trigger/2 callback to return a tuple with a list as first element " <>
            "and a term as second, got: #{inspect(other)}"
  end

  defp add_on_trigger(flow, on_trigger) do
    unless has_any_reduce?(flow) do
      raise ArgumentError,
            "on_trigger/2 must be called after a group_by/reduce/emit_and_reduce operation " <>
              "as it works on the accumulated state"
    end

    if has_on_trigger?(flow) do
      raise ArgumentError, "on_trigger/2 cannot be called after emit/on_trigger"
    end

    add_operation(flow, {:on_trigger, on_trigger})
  end

  defp add_mapper(flow, name, args) do
    if has_any_reduce?(flow) do
      raise ArgumentError,
            "#{name}/#{length(args) + 1} cannot be called after group_by/reduce/emit_and_reduce operation " <>
              "(use on_trigger/2 if you want to further emit events or the accumulated state)"
    end

    add_operation(flow, {:mapper, name, args})
  end

  defp add_operation(%Flow{operations: operations} = flow, operation) do
    %{flow | operations: [operation | operations]}
  end

  defp add_operation(flow, _producers) do
    raise ArgumentError, "expected a flow as argument, got: #{inspect(flow)}"
  end

  defp emit_nothing(flow) do
    inject_on_trigger(flow, fn _, _, _ -> [] end)
  end

  defp has_any_reduce?(%{operations: operations}) do
    Enum.any?(operations, &match?({op, _, _} when op in [:reduce, :emit_and_reduce], &1))
  end

  defp has_on_trigger?(%{operations: operations}) do
    Enum.any?(operations, &match?({:on_trigger, _}, &1))
  end

  defp inject_on_trigger(flow, fun) do
    inject_on_trigger(flow, fun, fun)
  end

  defp inject_on_trigger(flow, events_fun, acc_fun) do
    update_in(flow.operations, &inject_on_trigger(&1, [], events_fun, acc_fun))
  end

  defp inject_on_trigger([{:on_trigger, operation} | rest], pre, events_fun, _acc_fun) do
    operation = fn state, index, trigger ->
      {events, acc} = operation.(state, index, trigger)
      {events_fun.(events, index, trigger), acc}
    end

    Enum.reverse(pre, [{:on_trigger, operation} | rest])
  end

  defp inject_on_trigger([op | ops], pre, events_fun, acc_fun) do
    inject_on_trigger(ops, [op | pre], events_fun, acc_fun)
  end

  defp inject_on_trigger([], pre, _events_fun, acc_fun) do
    on_trigger = fn acc, index, trigger -> {acc_fun.(acc, index, trigger), acc} end
    [{:on_trigger, on_trigger} | Enum.reverse(pre)]
  end

  defp normalize_specs(specs_with_opts) do
    fn start_link ->
      for {spec, subscription_opts} <- specs_with_opts do
        {:ok, pid} = start_link.(spec)
        {pid, subscription_opts}
      end
    end
  end

  defp normalize_stages(stages) do
    normalized = Enum.map(stages, &normalize_stage/1)
    fn _ -> normalized end
  end

  defp normalize_stage({_, opts} = pair) when is_list(opts), do: pair
  defp normalize_stage(other), do: {other, []}

  defimpl Enumerable do
    def reduce(flow, acc, fun) do
      Flow.stream(flow).(acc, fun)
    end

    def count(_flow) do
      {:error, __MODULE__}
    end

    def member?(_flow, _value) do
      {:error, __MODULE__}
    end

    def slice(_flow) do
      {:error, __MODULE__}
    end
  end
end
