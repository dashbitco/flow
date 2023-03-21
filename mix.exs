defmodule Flow.Mixfile do
  use Mix.Project

  @version "1.2.4"

  def project do
    [
      app: :flow,
      version: @version,
      elixir: "~> 1.7",
      package: package(),
      description: "Computational parallel flows for Elixir",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Flow",
      docs: [
        main: "Flow",
        source_ref: "v#{@version}",
        source_url: "https://github.com/dashbitco/flow"
      ]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp deps do
    [
      {:gen_stage, "~> 1.0"},
      {:ex_doc, "~> 0.19", only: :docs}
    ]
  end

  defp package do
    %{
      licenses: ["Apache-2.0"],
      maintainers: ["José Valim", "James Fish"],
      links: %{"GitHub" => "https://github.com/dashbitco/flow"}
    }
  end
end
