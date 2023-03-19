defmodule Flow.MapReducer do
  @moduledoc false
  use GenStage

  def init({type, opts, index, trigger, acc, reducer}) do
    Process.flag(:trap_exit, true)

    {on_init, opts} = Keyword.pop(opts, :on_init, & &1)
    on_init.(index)

    {type, {%{}, build_status(type, trigger), index, acc.(), reducer}, opts}
  end

  def handle_subscribe(:producer, opts, {pid, ref}, {producers, status, index, acc, reducer}) do
    opts[:tag] && Process.put(ref, opts[:tag])
    status = producer_status(pid, ref, status)
    state = {Map.put(producers, ref, nil), status, index, acc, reducer}

    if status.done? do
      GenStage.cancel({pid, ref}, :normal, [:noconnect])
      {:manual, state}
    else
      {:automatic, state}
    end
  end

  def handle_subscribe(:consumer, _opts, {pid, ref}, {producers, status, index, acc, reducer}) do
    status = consumer_status(pid, ref, status)
    {:automatic, {producers, status, index, acc, reducer}}
  end

  def handle_cancel(_reason, {_, ref}, {producers, status, index, acc, reducer}) do
    case producers do
      %{^ref => _} ->
        Process.delete(ref)
        {events, acc, status} = done_status(status, index, acc, ref)
        {:noreply, events, {Map.delete(producers, ref), status, index, acc, reducer}}

      _ ->
        consumers = Map.delete(status.consumers, ref)
        status = %{status | consumers: consumers}

        if consumers == %{} do
          {:stop, :normal, {producers, status, index, acc, reducer}}
        else
          {:noreply, [], {producers, status, index, acc, reducer}}
        end
    end
  end

  def handle_info({:trigger, name}, {producers, status, index, acc, reducer}) do
    %{trigger: trigger} = status
    {events, acc} = trigger.(acc, index, name)
    {:noreply, events, {producers, status, index, acc, reducer}}
  end

  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end

  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  def handle_events(events, {_, ref}, {producers, status, index, acc, reducer})
      when is_function(reducer, 4) do
    {events, acc} = reducer.(ref, events, acc, index)
    {:noreply, events, {producers, status, index, acc, reducer}}
  end

  def handle_events(events, {_, ref}, {producers, status, index, acc, reducer}) do
    {producers, events, acc} = reducer.(producers, ref, events, acc, index)
    {:noreply, events, {producers, status, index, acc, reducer}}
  end

  ## Helpers

  defp build_status(_type, trigger) do
    %{producers: %{}, consumers: %{}, done?: false, trigger: trigger}
  end

  defp producer_status(pid, ref, %{producers: producers} = status) do
    %{status | producers: Map.put(producers, ref, pid)}
  end

  defp consumer_status(pid, ref, %{consumers: consumers} = status) do
    %{status | consumers: Map.put(consumers, ref, pid)}
  end

  defp done_status(%{producers: map, done?: true} = status, _index, acc, _ref) when map == %{} do
    {[], acc, status}
  end

  defp done_status(%{done?: false} = status, index, acc, ref) do
    %{trigger: trigger, producers: producers} = status

    case Map.delete(producers, ref) do
      new_producers when new_producers == %{} and producers != %{} ->
        {events, acc} = trigger.(acc, index, :done)
        GenStage.async_info(self(), :stop)
        {events, acc, %{status | producers: %{}, done?: true}}

      producers ->
        {[], acc, %{status | producers: producers}}
    end
  end
end
