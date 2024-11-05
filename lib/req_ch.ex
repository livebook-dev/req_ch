defmodule ReqCh do
  @moduledoc """
  A Req plugin for ClickHouse.
  """

  @doc """
  Attach this plugin to a Req Request.
  """
  def attach(%Req.Request{} = request, _opts \\ []) do
    request
  end
end
