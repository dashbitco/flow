defmodule Flow.MapReducer do
  @moduledoc false
  use GenStage

  def init({type, opts, index, trigger, acc, reducer}) do
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
  def handle_subscribe(:consumer, _, _, state) do
    {:automatic, state}
  end

  def handle_cancel(_, {_, ref}, {producers, status, index, acc, reducer}) do
    cond do
      Map.has_key?(producers, ref) ->
        Process.delete(ref)
        {events, acc, status} = done_status(status, index, acc, ref)
        {:noreply, events, {Map.delete(producers, ref), status, index, acc, reducer}}
      true ->
        {:noreply, [], {producers, status, index, acc, reducer}}
    end
  end

  def handle_info({:trigger, keep_or_reset, name}, {producers, status, index, acc, reducer}) do
    %{trigger: trigger} = status
    {events, acc} = trigger.(acc, index, keep_or_reset, name)
    {:noreply, events, {producers, status, index, acc, reducer}}
  end
  def handle_info(:stop, state) do
    {:stop, :normal, state}
  end
  def handle_info(_msg, state) do
    {:noreply, [], state}
  end

  def handle_events(events, {_, ref}, {producers, status, index, acc, reducer}) when is_function(reducer, 4) do
    {events, acc} = reducer.(ref, events, acc, index)
    {:noreply, events, {producers, status, index, acc, reducer}}
  end
  def handle_events(events, {_, ref}, {producers, status, index, acc, reducer}) do
    {producers, events, acc} = reducer.(producers, ref, events, acc, index)
    {:noreply, events, {producers, status, index, acc, reducer}}
  end

  ## Helpers

  defp build_status(_type, trigger) do
    %{producers: %{}, done?: false, trigger: trigger}
  end

  defp producer_status(pid, ref, %{producers: producers} = status) do
    %{status | producers: Map.put(producers, ref, pid)}
  end

  defp done_status(%{producers: map, done?: true} = status, _index, acc, _ref) when map == %{} do
    {[], acc, status}
  end
  defp done_status(%{done?: false, trigger: trigger, producers: producers} = status,
                   index, acc, ref) do
    case Map.delete(producers, ref) do
      new_producers when new_producers == %{} and producers != %{} ->
        {events, acc} = trigger.(acc, index, :keep, :done)
        GenStage.async_info(self(), :stop)
        {events, acc, %{status | producers: %{}, done?: true}}
      producers ->
        {[], acc, %{status | producers: producers}}
    end
  end
end
