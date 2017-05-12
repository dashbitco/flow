defmodule Flow.MapReducer do
  @moduledoc false
  use GenStage

  def init({type, opts, index, trigger, acc, reducer}) do
    {type, {%{}, build_status(type, trigger), index, acc.(), reducer}, opts}
  end

  def handle_subscribe(:producer, opts, {pid, ref}, {producers, status, index, acc, reducer}) do
    opts[:tag] && Process.put(ref, opts[:tag])
    status = producer_status(pid, ref, status)
    {:automatic, {Map.put(producers, ref, nil), status, index, acc, reducer}}
  end

  def handle_subscribe(:consumer, _, {pid, ref}, {producers, status, index, acc, reducer}) do
    status = consumer_status(pid, ref, status)
    {:automatic, {producers, status, index, acc, reducer}}
  end

  def handle_cancel(_, {_, ref}, {producers, status, index, acc, reducer}) do
    status = cancel_status(ref, status)
    %{consumers: consumers} = status

    cond do
      Map.has_key?(producers, ref) ->
        Process.delete(ref)
        {events, acc, status} = done_status(status, index, acc, ref)
        {:noreply, events, {Map.delete(producers, ref), status, index, acc, reducer}}
      consumers == [] ->
        {:stop, :normal, {producers, status, index, acc, reducer}}
      true ->
        {:noreply, [], {producers, status, index, acc, reducer}}
    end
  end

  def handle_info({:trigger, keep_or_reset, name}, {producers, status, index, acc, reducer}) do
    %{trigger: trigger} = status
    {events, acc} = trigger.(acc, index, keep_or_reset, name)
    {:noreply, events, {producers, status, index, acc, reducer}}
  end
  def handle_info({{_, ref}, {:producer, state}}, {producers, status, index, acc, reducer}) when state in [:halted, :done] do
    {events, acc, status} = done_status(status, index, acc, ref)
    {:noreply, events, {producers, status, index, acc, reducer}}
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

  defp build_status(type, trigger) do
    consumers = if type == :consumer, do: nil, else: []
    %{consumers: consumers, producers: %{}, active: [], done?: false, trigger: trigger}
  end

  defp producer_status(pid, ref, %{active: active, producers: producers} = status) do
    %{status | active: [ref | active], producers: Map.put(producers, ref, pid)}
  end

  defp consumer_status(pid, ref, %{done?: true, consumers: consumers} = status) do
    send pid, {{pid, ref}, {:producer, :done}}
    %{status | consumers: [ref | consumers]}
  end

  defp consumer_status(_pid, ref, %{consumers: consumers} = status) do
    %{status | consumers: [ref | consumers]}
  end

  defp cancel_status(ref, %{consumers: consumers, producers: producers} = status) do
    %{status | consumers: consumers && List.delete(consumers, ref),
               producers: Map.delete(producers, ref)}
  end

  defp done_status(%{active: [], done?: true} = status, _index, acc, _ref) do
    {[], acc, status}
  end
  defp done_status(%{active: active, done?: false, trigger: trigger,
                     consumers: consumers, producers: producers} = status,
                   index, acc, ref) do
    case List.delete(active, ref) do
      [] when active != [] ->
        {events, acc} = trigger.(acc, index, :keep, :done)

        if is_list(consumers) do
          GenStage.async_notify(self(), {:producer, :done})
        else
          for {ref, pid} <- producers do
            GenStage.cancel({pid, ref}, :normal, [:noconnect])
          end
        end

        {events, acc, %{status | active: [], done?: true}}
      active ->
        {[], acc, %{status | active: active}}
    end
  end
end
