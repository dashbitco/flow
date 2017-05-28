defmodule Flow.Mixfile do
  use Mix.Project

  @version "0.12.0"

  def project do
    [app: :flow,
     version: @version,
     elixir: "~> 1.3",
     package: package(),
     description: "Computational parallel flows for Elixir",
     build_embedded: Mix.env == :prod,
     start_permanent: Mix.env == :prod,
     deps: deps(),
     docs: [main: "Flow", source_ref: "v#{@version}",
            source_url: "https://github.com/elixir-lang/flow"]]
  end

  def application do
    [applications: [:logger, :gen_stage]]
  end

  defp deps do
    [{:gen_stage, "~> 0.12.0"},
     {:ex_doc, "~> 0.12", only: :docs},
     {:inch_ex, ">= 0.4.0", only: :docs}]
  end

  defp package do
    %{licenses: ["Apache 2"],
      maintainers: ["José Valim", "James Fish"],
      links: %{"GitHub" => "https://github.com/elixir-lang/flow"}}
  end
end
