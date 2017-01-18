# Flow

`Flow` allows developers to express computations on collections, similar to the `Enum` and `Stream` modules, although computations will be executed in parallel using multiple `GenStage`s.

Here is a quick and example on how to count words in a document in parallel with Flow:

```elixir
File.stream!("path/to/some/file")
|> Flow.from_enumerable()
|> Flow.flat_map(&String.split(&1, " "))
|> Flow.partition()
|> Flow.reduce(fn -> %{} end, fn word, acc ->
  Map.update(acc, word, 1, & &1 + 1)
end)
|> Enum.to_list()
```

See documentation for [Flow](https://hexdocs.pm/flow) or [José Valim's keynote at ElixirConf 2016](https://youtu.be/srtMWzyqdp8?t=244) introducing the main concepts behind GenStage and Flow.

## Installation

Flow requires Elixir v1.3. Add `:flow` to your list of dependencies in mix.exs:

```elixir
def deps do
  [{:flow, "~> 0.11"}]
end
```

## License

Same as Elixir.
