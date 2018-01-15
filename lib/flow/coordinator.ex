defmodule Flow.Coordinator do
  @moduledoc false
  use GenServer

  def start_link(flow, type, consumers, options) do
    GenServer.start_link(__MODULE__, {self(), flow, type, consumers, options}, options)
  end

  def start(flow, type, consumers, options) do
    GenServer.start(__MODULE__, {self(), flow, type, consumers, options}, options)
  end

  def stream(pid) do
    GenServer.call(pid, :stream, :infinity)
  end

  ## Callbacks

  def init({parent, flow, type, consumers, options}) do
    Process.flag(:trap_exit, true)
    {:ok, sup} = start_supervisor()
    type_options = Keyword.take(options, [:dispatcher])

    {producers, intermediary} =
      Flow.Materialize.materialize(flow, &start_child(sup, &1, &2), type, type_options)

    demand = Keyword.get(options, :demand, :forward)
    producers = Enum.map(producers, &elem(&1, 0))

    for producer <- producers,
        demand == :accumulate do
      GenStage.demand(producer, demand)
    end

    subscribe_fn =
      case Keyword.fetch(options, :timeout) do
        ({:ok, timeout}) ->
          &(subscribe(&1, &2, timeout))
        (:error) ->
          &(subscribe(&1, &2))
      end

    refs =
      for {pid, _} <- intermediary do
        for consumer <- consumers do
          subscribe_fn.(consumer, pid)
        end
        Process.monitor(pid)
      end

    for producer <- producers,
        demand == :forward do
      GenStage.demand(producer, demand)
    end

    {:ok, %{supervisor: sup, producers: producers, intermediary: intermediary,
            refs: refs, parent_ref: Process.monitor(parent)}}
  end

  defp start_supervisor() do
    Supervisor.start_link([], strategy: :one_for_one, max_restarts: 0)
  end

  defp start_child(sup, args, opts) do
    shutdown = Keyword.get(opts, :shutdown, 5000)
    spec = {make_ref(), {GenStage, :start_link, args}, :transient, shutdown, :worker, [GenStage]}
    Supervisor.start_child(sup, spec)
  end

  defp subscribe({consumer, opts}, producer, timeout) when is_list(opts) do
    GenStage.sync_subscribe(consumer, [to: producer, cancel: :transient] ++ opts, timeout)
  end
  defp subscribe(consumer, producer, timeout) do
    GenStage.sync_subscribe(consumer, [to: producer, cancel: :transient], timeout)
  end

  defp subscribe({consumer, opts}, producer) when is_list(opts) do
    GenStage.sync_subscribe(consumer, [to: producer, cancel: :transient] ++ opts)
  end
  defp subscribe(consumer, producer) do
    GenStage.sync_subscribe(consumer, [to: producer, cancel: :transient])
  end

  def handle_call(:stream, _from, %{producers: producers, intermediary: intermediary} = state) do
    {:reply, GenStage.stream(intermediary, producers: producers), state}
  end

  def handle_cast({:"$demand", demand}, %{producers: producers} = state) do
    for producer <- producers, do: GenStage.demand(producer, demand)
    {:noreply, state}
  end

  def handle_info({:"$gen_producer", {consumer, ref}, {:subscribe, _, opts}}, %{intermediary: intermediary} = state) do
    for {pid, _} <- intermediary do
      GenStage.async_subscribe(consumer, [to: pid] ++ opts)
    end
    send(consumer, {:"$gen_consumer", {self(), ref}, {:cancel, :normal}})
    {:noreply, state}
  end
  # Since consumers can send demand right after subscription,
  # we may still receive ask messages, which we promptly ignore.
  def handle_info({:"$gen_producer", _from,  {:ask, _}}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, _, _, reason}, %{parent_ref: ref} = state) do
    {:stop, reason, state}
  end
  def handle_info({:DOWN, ref, _, _, _}, %{refs: refs} = state) do
    case List.delete(refs, ref) do
      [] -> {:stop, :normal, state}
      refs -> {:noreply, %{state | refs: refs}}
    end
  end
  def handle_info({:EXIT, sup, reason}, %{supervisor: sup} = state) do
    {:stop, reason, state}
  end

  def terminate(_reason, %{supervisor: supervisor}) do
    ref = Process.monitor(supervisor)
    Process.exit(supervisor, :shutdown)
    receive do
      {:DOWN, ^ref, _, _, _} -> :ok
    end
  end
end
