defmodule ReqCh.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :req_ch,
      version: @version,
      elixir: "~> 1.14",
      name: "ReqCh",
      description: "A minimal Req plugin for ClickHouse",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:req, "~> 0.5"},
      {:explorer, "~> 0.10", optional: true},
      {:ex_doc, ">= 0.0.0", only: :docs, runtime: false}
    ]
  end
end
