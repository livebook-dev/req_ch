defmodule ReqCHTest do
  use ExUnit.Case

  describe "new/1" do
    test "returns a request" do
      assert %Req.Request{} = ReqCH.new()
    end

    test "a simple query" do
      req = ReqCH.new()

      assert response =
               %Req.Response{} =
               Req.post!(req,
                 body: "SELECT number, number - 2 as less_two from system.numbers LIMIT 10"
               )

      assert response.body == "0\t-2\n1\t-1\n2\t0\n3\t1\n4\t2\n5\t3\n6\t4\n7\t5\n8\t6\n9\t7\n"
    end

    test "with format option as :csv" do
      req = ReqCH.new(format: :csv)

      assert response =
               %Req.Response{} =
               Req.post!(req,
                 body: "SELECT number, number - 2 as less_two from system.numbers LIMIT 10"
               )

      assert response.body == """
             0,-2
             1,-1
             2,0
             3,1
             4,2
             5,3
             6,4
             7,5
             8,6
             9,7
             """
    end

    test "with database option" do
      req = ReqCH.new(database: "system")

      assert response =
               %Req.Response{} =
               Req.post!(req,
                 body: "SELECT number FROM numbers LIMIT 3"
               )

      assert response.body == """
             0
             1
             2
             """
    end
  end

  describe "query/4" do
    test "a plain query with defaults" do
      req = ReqCH.new(database: "system")

      assert {:ok, %Req.Response{} = response} =
               ReqCH.query(req, "SELECT number FROM numbers LIMIT 3", [], [])

      assert response.status == 200

      assert response.body == """
             0
             1
             2
             """
    end

    test "a query with params and database in opts" do
      req = ReqCH.new()

      assert {:ok, %Req.Response{} = response} =
               ReqCH.query(
                 req,
                 "SELECT number FROM numbers WHERE number > {num:UInt8} LIMIT 3",
                 [num: 25],
                 database: "system"
               )

      assert response.status == 200

      assert response.body == """
             26
             27
             28
             """
    end

    test "with format option as :explorer" do
      req = ReqCH.new()

      assert {:ok, %Req.Response{} = response} =
               ReqCH.query(
                 req,
                 "SELECT number, number - 2 as less_two from system.numbers LIMIT 10",
                 [],
                 format: :explorer
               )

      assert %Explorer.DataFrame{} = df = response.body

      assert Explorer.DataFrame.to_columns(df, atom_keys: true) ==
               %{
                 number: [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
                 less_two: [-2, -1, 0, 1, 2, 3, 4, 5, 6, 7]
               }
    end

    test "with format option as :explorer but different format in the query" do
      assert {:ok, %Req.Response{} = response} =
               ReqCH.query(
                 ReqCH.new(),
                 "SELECT number, number - 2 as less_two from system.numbers LIMIT 10 FORMAT JSON",
                 format: :explorer
               )

      assert response.status == 200

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

    test "with format :json" do
      assert {:ok, %Req.Response{} = response} =
               ReqCH.query(
                 ReqCH.new(),
                 "SELECT number, number - 2 as less_two from system.numbers LIMIT 3",
                 [],
                 format: :json
               )

      assert response.status == 200

      assert %{
               "data" => [
                 %{"less_two" => "-2", "number" => "0"},
                 %{"less_two" => "-1", "number" => "1"},
                 %{"less_two" => "0", "number" => "2"}
               ],
               "meta" => [
                 %{"name" => "number", "type" => "UInt64"},
                 %{"name" => "less_two", "type" => "Int64"}
               ],
               "rows" => 3,
               "rows_before_limit_at_least" => 3
             } = response.body
    end

    test "with invalid format" do
      error_message =
        "the given format :invalid_format is invalid. Expecting one of [:tsv, :csv, :json, :explorer] " <>
          "or one of the valid options described in https://clickhouse.com/docs/en/interfaces/formats"

      assert_raise ArgumentError, error_message, fn ->
        ReqCH.query(
          ReqCH.new(),
          "SELECT number from system.numbers LIMIT 10",
          [],
          format: :invalid_format
        )
      end
    end

    test "a query with params" do
      assert {:ok, %Req.Response{} = response} =
               ReqCH.query(
                 ReqCH.new(),
                 "SELECT number FROM system.numbers WHERE number > {num:UInt8} LIMIT 7",
                 num: 5
               )

      assert response.status == 200

      assert response.body == """
             6
             7
             8
             9
             10
             11
             12
             """
    end

    test "a query with Date(Time) params" do
      utc_now = ~U[2024-12-20 05:44:53.855679Z]
      utc_now_seconds = DateTime.truncate(utc_now, :second)
      utc_today = DateTime.to_date(utc_now)

      params = [
        utc_now: utc_now,
        utc_now_seconds: utc_now_seconds,
        utc_today: utc_today
      ]

      response =
        ReqCH.query!(
          ReqCH.new(),
          """
          SELECT
            {utc_now:DateTime64(6)},
            {utc_now_seconds:DateTime},
            {utc_today:Date}
          FORMAT
            JSONCompact
          """,
          params
        )

      assert response.status == 200

      assert response.body["data"] == [
               [
                 "2024-12-20 05:44:53.855679",
                 "2024-12-20 05:44:53",
                 "2024-12-20"
               ]
             ]
    end

    # https://clickhouse.com/docs/en/interfaces/http#tabs-in-url-parameters
    # https://clickhouse.com/docs/en/interfaces/formats#tabseparated-data-formatting
    test "a query with tabs and newlines" do
      params = [
        tab: "a\tb",
        newline: "c\nd",
        both: "a\tb\nc\t\nd"
      ]

      response =
        ReqCH.query!(
          ReqCH.new(),
          "SELECT {tab:String}, {newline:String}, {both:String} FORMAT JSONCompact",
          params
        )

      assert response.status == 200
      assert response.body["data"] == [["a\tb", "c\nd", "a\tb\nc\t\nd"]]
    end

    test "a query with arrays" do
      params = [
        array: ["a", "b", "c", "a\tb\nc\t\nd"],
        empty_array: [],
        nested_array: [["a", "b"], ["c", "d"], ["a\tb\nc\t\nd"]],
        date_array: [~D[2024-12-20], ~D[2024-12-21]]
      ]

      response =
        ReqCH.query!(
          ReqCH.new(),
          """
          SELECT
            {array:Array(String)},
            {empty_array:Array(String)},
            {nested_array:Array(Array(String))},
            {date_array:Array(Date)}
          FORMAT
            JSONCompact
          """,
          params
        )

      assert response.status == 200

      assert response.body["data"] == [
               [
                 ["a", "b", "c", "a\tb\nc\t\nd"],
                 [],
                 [["a", "b"], ["c", "d"], ["a\tb\nc\t\nd"]],
                 ["2024-12-20", "2024-12-21"]
               ]
             ]
    end

    test "a query with tuples" do
      params = [
        tuple: {1, "a", ~D[2024-12-20]},
        empty_tuple: {},
        nested_tuple: {{1, "a"}, {2, "b"}},
        date_tuple: {~D[2024-12-20], ~D[2024-12-21]}
      ]

      response =
        ReqCH.query!(
          ReqCH.new(),
          """
          SELECT
            {tuple:Tuple(UInt8, String, Date)},
            {empty_tuple:Tuple()},
            {nested_tuple:Tuple(Tuple(UInt8, String), Tuple(UInt8, String))},
            {date_tuple:Tuple(Date, Date)}
          """,
          params
        )

      assert response.status == 200

      assert response.body ==
               """
               (1,'a','2024-12-20')\t\
               ()\t\
               ((1,'a'),(2,'b'))\t\
               ('2024-12-20','2024-12-21')
               """
    end

    test "a query with maps" do
      params = [
        map: %{"a" => 1, "b" => 2},
        empty_map: %{},
        nested_map: %{"a" => %{"b" => 1}, "c" => %{"d" => 2}},
        date_map: %{"a" => ~D[2024-12-20], "b" => ~D[2024-12-21]}
      ]

      response =
        ReqCH.query!(
          ReqCH.new(),
          """
          SELECT
            {map:Map(String, UInt8)},
            {empty_map:Map(String, UInt8)},
            {nested_map:Map(String, Map(String, UInt8))},
            {date_map:Map(String, Date)}
          FORMAT
            JSONCompact
          """,
          params
        )

      assert response.status == 200

      assert response.body["data"] == [
               [
                 %{"a" => 1, "b" => 2},
                 %{},
                 %{"a" => %{"b" => 1}, "c" => %{"d" => 2}},
                 %{"a" => "2024-12-20", "b" => "2024-12-21"}
               ]
             ]
    end

    test "a query with unknown database" do
      assert {:ok, %Req.Response{} = response} =
               ReqCH.query(
                 ReqCH.new(),
                 "SELECT number FROM sistema WHERE number > {num:UInt8} LIMIT 7",
                 num: 5
               )

      assert response.status == 404

      assert response.body =~ "UNKNOWN_TABLE"
    end
  end
end
