defmodule Flow.Coordinator do
  @moduledoc false
  use GenServer

  def start_link(flow, type, consumers, options) do
    GenServer.start_link(__MODULE__, {flow, type, consumers, options}, options)
  end

  def start(flow, type, consumers, options) do
    GenServer.start(__MODULE__, {flow, type, consumers, options}, options)
  end

  def stream(pid) do
    GenServer.call(pid, :stream, :infinity)
  end

  ## Callbacks

  def init({flow, type, consumers, options}) do
    Process.flag(:trap_exit, true)
    type_options = Keyword.take(options, [:dispatcher])

    {:ok, supervisor} = start_supervisor()
    start_link = &start_child(supervisor, &1, &2)
    {producers, intermediary} = Flow.Materialize.materialize(flow, start_link, type, type_options)

    demand = Keyword.get(options, :demand, :forward)
    timeout = Keyword.get(options, :subscribe_timeout, 5_000)
    producers = Enum.map(producers, &elem(&1, 0))

    for producer <- producers, demand == :accumulate do
      GenStage.demand(producer, demand)
    end

    refs =
      for {pid, _} <- intermediary do
        for consumer <- consumers do
          subscribe(consumer, pid, timeout)
        end
        Process.monitor(pid)
      end

    for producer <- producers, demand == :forward do
      GenStage.demand(producer, demand)
    end

    state = %{
      intermediary: intermediary,
      refs: refs,
      producers: producers,
      supervisor: supervisor
    }

    {:ok, state}
  end

  # We have a supervisor for producers and another for producer_consumers.
  #
  # If any producer crashes with non-normal/non-shutdown exit, it causes
  # other producers to exit eventually leading to the termination of all
  # map reducers.
  #
  # Once all map reducers exit, the coordinator exits too.
  defp start_supervisor() do
    Supervisor.start_link([], strategy: :one_for_one, max_restarts: 0)
  end

  defp start_child(supervisor, args, opts) do
    shutdown = Keyword.get(opts, :shutdown, 5000)
    spec = {make_ref(), {GenStage, :start_link, args}, :temporary, shutdown, :worker, [GenStage]}
    Supervisor.start_child(supervisor, spec)
  end

  defp subscribe({consumer, opts}, producer, timeout) when is_list(opts) do
    GenStage.sync_subscribe(consumer, [to: producer, cancel: :transient] ++ opts, timeout)
  end
  defp subscribe(consumer, producer, timeout) do
    GenStage.sync_subscribe(consumer, [to: producer, cancel: :transient], timeout)
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
