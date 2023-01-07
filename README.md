# Flow [![Build Status](https://github.com/dashbitco/flow/workflows/CI/badge.svg)](https://github.com/dashbitco/flow/actions?query=workflow%3A%22CI%22)

`Flow` allows developers to express computations on collections, similar to the `Enum` and `Stream` modules, although computations will be executed in parallel using multiple [`GenStage`](https://github.com/elixir-lang/gen_stage)s.

Here is a quick example on how to count words in a document in parallel with Flow:

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

See documentation for [Flow](https://hexdocs.pm/flow) or [José Valim's keynote at ElixirConf 2016](https://youtu.be/srtMWzyqdp8?t=244) introducing the main concepts behind [GenStage](https://github.com/elixir-lang/gen_stage) and [Flow](https://hexdocs.pm/flow).

## Installation

Flow requires Elixir v1.7 and Erlang/OTP 22+. Add `:flow` to your list of dependencies in mix.exs:

```elixir
def deps do
  [{:flow, "~> 1.0"}]
end
```

### Usage in Livebook

Flow pipelines starts several processes linked to the current process. This means that, if there is an error in your Flow, it will shut down the Livebook runtime. You can avoid this in your notebooks in two different ways:

1. Use `Flow.stream(flow, link: false)` to explicitly convert a Flow to a non-linked stream. You can them invoke `Enum` and `Stream` functions regularly:

    ```elixir
    Flow.from_enumerable([1, 2, 3])
    |> Flow.map(& &1 * 2)
    |> Flow.stream(link: false)
    |> Enum.to_list()
    ```

2. By trapping exits once before the Flow computation starts:

    ```elixir
    Process.flag(:trap_exit, true)
    ```

## License

Copyright 2017 Plataformatec \
Copyright 2020 Dashbit

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

