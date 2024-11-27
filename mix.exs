defmodule ReqCH.MixProject do
  use Mix.Project

  @version "0.1.0"

  def project do
    [
      app: :req_ch,
      version: @version,
      elixir: "~> 1.14",
      name: "ReqCH",
      description: "A minimal Req plugin for ClickHouse",
      start_permanent: Mix.env() == :prod,
      preferred_cli_env: [
        docs: :docs,
        "hex.publish": :docs
      ],
      docs: docs(),
      package: package(),
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:req, "~> 0.5"},
      {:explorer, "~> 0.10", optional: true},
      {:ex_doc, ">= 0.0.0", only: :docs, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => "https://github.com/livebook-dev/req_ch"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: "https://github.com/livebook-dev/req_ch",
      source_ref: "v#{@version}",
      extras: ["README.md"]
    ]
  end
end
