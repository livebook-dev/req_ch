defmodule ReqCHTest do
  use ExUnit.Case

  test "returns a request" do
    assert %Req.Request{} = ReqCH.attach(Req.new())
  end

  test "a simple query" do
    req = Req.new() |> ReqCH.attach()

    assert response =
             %Req.Response{} =
             Req.post!(req,
               clickhouse: "SELECT number, number - 2 as less_two from system.numbers LIMIT 10"
             )

    assert response.body == "0\t-2\n1\t-1\n2\t0\n3\t1\n4\t2\n5\t3\n6\t4\n7\t5\n8\t6\n9\t7\n"
  end

  test "with format option as :csv" do
    req = Req.new() |> ReqCH.attach()

    assert response =
             %Req.Response{} =
             Req.post!(req,
               clickhouse: "SELECT number, number - 2 as less_two from system.numbers LIMIT 10",
               format: :csv
             )

    assert response.body == "0,-2\n1,-1\n2,0\n3,1\n4,2\n5,3\n6,4\n7,5\n8,6\n9,7\n"
  end

  test "with format option as :explorer" do
    req = Req.new() |> ReqCH.attach()

    assert response =
             %Req.Response{} =
             Req.post!(req,
               clickhouse: "SELECT number, number - 2 as less_two from system.numbers LIMIT 10",
               format: :explorer
             )

    assert %Explorer.DataFrame{} = df = response.body

    assert Explorer.DataFrame.to_columns(df, atom_keys: true) ==
             %{number: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9], less_two: [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7]}
  end

  test "with format option as :explorer but different format in the query" do
    req = Req.new() |> ReqCH.attach()

    assert response =
             %Req.Response{} =
             Req.post!(req,
               clickhouse:
                 "SELECT number, number - 2 as less_two from system.numbers LIMIT 10 FORMAT JSON",
               format: :explorer
             )

    assert %{
             "data" => [
               %{"less_two" => "-2", "number" => "0"},
               %{"less_two" => "-1", "number" => "1"},
               %{"less_two" => "0", "number" => "2"},
               %{"less_two" => "1", "number" => "3"},
               %{"less_two" => "2", "number" => "4"},
               %{"less_two" => "3", "number" => "5"},
               %{"less_two" => "4", "number" => "6"},
               %{"less_two" => "5", "number" => "7"},
               %{"less_two" => "6", "number" => "8"},
               %{"less_two" => "7", "number" => "9"}
             ],
             "meta" => [
               %{"name" => "number", "type" => "UInt64"},
               %{"name" => "less_two", "type" => "Int64"}
             ],
             "rows" => 10,
             "rows_before_limit_at_least" => 10,
             "statistics" => %{
               "bytes_read" => 80,
               "elapsed" => _,
               "rows_read" => 10
             }
           } = response.body
  end

  test "with invalid format" do
    req = Req.new() |> ReqCH.attach()

    error_message =
      "the given format :invalid_format is invalid. Expecting one of [:tsv, :csv, :explorer] " <>
        "or one of the valid options described in https://clickhouse.com/docs/en/interfaces/formats"

    assert_raise ArgumentError, error_message, fn ->
      Req.post!(req,
        clickhouse: "SELECT number from system.numbers LIMIT 10",
        format: :invalid_format
      )
    end
  end

  test "give error without the :clickhouse option" do
    req = Req.new() |> ReqCH.attach()

    assert_raise ArgumentError, fn -> Req.post!(req) end
  end
end
