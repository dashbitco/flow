defmodule Flow.Coordinator do
  @moduledoc false
  use GenServer

  def start_link(flow, type, consumers, options) do
    filtered_options =
      Keyword.take(options, [:debug, :name, :timeout, :spawn_opt, :hibernate_after])

    GenServer.start_link(__MODULE__, {flow, type, consumers, options}, filtered_options)
  end

  def start(flow, type, consumers, options) do
    filtered_options =
      Keyword.take(options, [:debug, :name, :timeout, :spawn_opt, :hibernate_after])

    GenServer.start(__MODULE__, {flow, type, consumers, options}, filtered_options)
  end

  def stream(pid) do
    GenServer.call(pid, :stream, :infinity)
  end

  ## Callbacks

  def init({flow, type, {inner_or_outer, consumers}, options}) do
    Process.flag(:trap_exit, true)

    {:ok, supervisor} = start_supervisor()
    start_link = &start_child(supervisor, &1, restart: :temporary)
    demand = Keyword.get(options, :demand, :forward)
    dispatcher = Keyword.get(options, :dispatcher, GenStage.DemandDispatcher)

    {producers, intermediary} =
      Flow.Materialize.materialize(flow, demand, start_link, type, dispatcher)

    producers = for {pid, _} <- producers, pid != :undefined, do: pid

    if producers == [] do
      :ignore
    else
      timeout = Keyword.get(options, :subscribe_timeout, 5_000)
      consumers = consumers.(&start_child(supervisor, &1, []))

      for {pid, _} <- intermediary, {consumer, opts} <- consumers do
        GenStage.sync_subscribe(consumer, [to: pid, cancel: :transient] ++ opts, timeout)
      end

      if demand == :forward do
        for producer <- producers, do: GenStage.demand(producer, demand)
      end

      to_ref = if inner_or_outer == :inner, do: consumers, else: intermediary
      refs = Enum.map(to_ref, fn {pid, _} -> Process.monitor(pid) end)

      state = %{
        intermediary: intermediary,
        refs: refs,
        producers: producers,
        supervisor: supervisor
      }

      {:ok, state}
    end
  end

  # We have a supervisor for the whole flow. We always wait for an error
  # to propagate through the whole flow, and then we terminate. For this
  # to work all children are started as temporary, except the consumers
  # given via into_specs. Once those crash, they terminate the whole
  # flow according to their restart type.
  defp start_supervisor do
    Supervisor.start_link([], strategy: :one_for_one, max_restarts: 0)
  end

  defp start_child(supervisor, spec, opts) do
    spec = Supervisor.child_spec(spec, [id: make_ref()] ++ opts)
    Supervisor.start_child(supervisor, spec)
  end

  def handle_call(:stream, _from, %{producers: producers, intermediary: intermediary} = state) do
    {:reply, GenStage.stream(intermediary, producers: producers), state}
  end

  def handle_cast({:"$demand", demand}, %{producers: producers} = state) do
    for producer <- producers, do: GenStage.demand(producer, demand)
    {:noreply, state}
  end

  def handle_info({:"$gen_producer", {consumer, ref}, {:subscribe, _, opts}}, state) do
    for {pid, _} <- state.intermediary do
      GenStage.async_subscribe(consumer, [to: pid] ++ opts)
    end

    send(consumer, {:"$gen_consumer", {self(), ref}, {:cancel, :normal}})
    {:noreply, state}
  end

  # Since consumers can send demand right after subscription,
  # we may still receive ask messages, which we promptly ignore.
  def handle_info({:"$gen_producer", _from, {:ask, _}}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{refs: refs} = state) do
    if ref in refs do
      refs = List.delete(refs, ref)
      state = %{state | refs: refs}

      non_normal_shutdown? =
        case reason do
          :normal -> false
          :shutdown -> false
          {:shutdown, _} -> false
          _ -> true
        end

      cond do
        non_normal_shutdown? -> {:stop, :shutdown, state}
        refs == [] -> {:stop, :normal, state}
        true -> {:noreply, state}
      end
    else
      {:noreply, state}
    end
  end

  def handle_info(_, state) do
    {:noreply, state}
  end

  def terminate(_reason, %{supervisor: supervisor}) do
    ref = Process.monitor(supervisor)
    Process.exit(supervisor, :shutdown)

    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
