# Flow

`Flow` allows developers to express computations on collections, similar to the `Enum` and `Stream` modules, although computations will be executed in parallel using multiple `GenStage`s.

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
