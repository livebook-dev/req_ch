defmodule ReqChTest do
  use ExUnit.Case
  doctest ReqCh

  test "returns a request" do
    assert %Req.Request{} = ReqCh.attach(Req.new())
  end
end
