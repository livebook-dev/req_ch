defmodule ReqCH do
  @moduledoc """
  A Req plugin for ClickHouse.

  By default, `ReqCH` will use TSV as the default output format.
  To change that, see the `new/2` docs for details.
  """

  @options [
    :database,
    :format
  ]

  @formats_page "https://clickhouse.com/docs/en/interfaces/formats"
  @supported_formats ~w(TabSeparated TabSeparatedRaw TabSeparatedWithNames TabSeparatedWithNamesAndTypes TabSeparatedRawWithNames TabSeparatedRawWithNamesAndTypes Template TemplateIgnoreSpaces CSV CSVWithNames CSVWithNamesAndTypes CustomSeparated CustomSeparatedWithNames CustomSeparatedWithNamesAndTypes SQLInsert Values Vertical JSON JSONAsString JSONAsObject JSONStrings JSONColumns JSONColumnsWithMetadata JSONCompact JSONCompactStrings JSONCompactColumns JSONEachRow PrettyJSONEachRow JSONEachRowWithProgress JSONStringsEachRow JSONStringsEachRowWithProgress JSONCompactEachRow JSONCompactEachRowWithNames JSONCompactEachRowWithNamesAndTypes JSONCompactStringsEachRow JSONCompactStringsEachRowWithNames JSONCompactStringsEachRowWithNamesAndTypes JSONObjectEachRow BSONEachRow TSKV Pretty PrettyNoEscapes PrettyMonoBlock PrettyNoEscapesMonoBlock PrettyCompact PrettyCompactNoEscapes PrettyCompactMonoBlock PrettyCompactNoEscapesMonoBlock PrettySpace PrettySpaceNoEscapes PrettySpaceMonoBlock PrettySpaceNoEscapesMonoBlock Prometheus Protobuf ProtobufSingle ProtobufList Avro AvroConfluent Parquet ParquetMetadata Arrow ArrowStream ORC One Npy RowBinary RowBinaryWithNames RowBinaryWithNamesAndTypes RowBinaryWithDefaults Native Null XML CapnProto LineAsString Regexp RawBLOB MsgPack MySQLDump DWARF Markdown Form)

  @doc """
  Builds a new `%Req.Request{}` for ClickHouse requests.

  ## Options

  This function can receive any option that `Req.new/1` accepts,
  plus the ones described below.

  To set a new endpoint, use the `:base_url` option from `Req`.
  It is by default "http://localhost:8123".

    * `:format` - Optional. The format of the response. Default is `:tsv`.
      This option accepts `:tsv`, `:csv`, `:json` or `:explorer` as atoms.

      It also accepts all formats described in the #{@formats_page} page.
      Use plain strings for these formats.

      The `:explorer` format is special, and will build an Explorer dataframe
      in case the `:explorer` dependency is installed.

    * `:database` - Optional. The database to use in the queries.
      Default is `nil`.

  ## Examples

  After setting a default database, one can make a request directly:

      iex> req = ReqCH.new(database: "system")
      iex> Req.post!(req, body: "SELECT number + 1 FROM numbers LIMIT 3").body
      "1\\n2\\n3\\n"

  It's also possible to make a query using `Req.get/2`:

      iex> req = ReqCH.new(database: "system")
      iex> Req.get!(req, params: [query: "SELECT number + 1 FROM numbers LIMIT 3"]).body
      "1\\n2\\n3\\n"

  In case the server needs authentication, it's possible to use `Req` options for that.

      iex> req = ReqCH.new(base_url: "http://example.org:8123", auth: {:basic, "user:pass"})
      iex> Req.post!(req, body: "SELECT number FROM system.numbers LIMIT 3").body
      "0\\n1\\n2\\n"

  """
  @spec new(Keyword.t()) :: Req.Request.t()
  def new(opts \\ []) do
    attach(Req.new(base_url: "http://localhost:8123"), opts)
  end

  defp attach(%Req.Request{} = req, opts) do
    req
    |> Req.Request.prepend_request_steps(clickhouse_run: &run/1)
    |> Req.Request.register_options(@options)
    |> Req.Request.merge_options(opts)
  end

  defguardp is_query_params(value) when is_list(value) or is_map(value)

  @doc """
  Performs a query against the ClickHouse API.

  This version receives a `Req.Request.t()`, so it won't
  create a new one from scratch.

  By default, it will use the `http://localhost:8123` as `:base_url`.
  You can change that either providing in your Req request, or in passing
  down in the options.
  See `new/1` for the options. Like that function, `query/4` accepts any
  option that `Req.new/1` accepts.

  ## Examples

  Queries can be performed using both `Req.get/2` or `Req.post/2`, but GET
  is "read-only" and commands like `CREATE` or `INSERT` cannot be used with it.
  For that reason, by default we perform a `POST` request.

  A plain query:

      iex> req = ReqCH.new(database: "system")
      iex> {:ok, response} = ReqCH.query(req, "SELECT number FROM numbers LIMIT 3")
      iex> response.body
      "0\\n1\\n2\\n"

  With a specific format:

      iex> req = ReqCH.new(database: "system")
      iex> {:ok, response} = ReqCH.query(req, "SELECT number FROM numbers LIMIT 3", [], [format: :explorer])
      iex> response.body
      #Explorer.DataFrame<
        Polars[3 x 1]
        number u64 [0, 1, 2]
      >

   Passing SQL params:

      iex> req = ReqCH.new(database: "system")
      iex> {:ok, response} = ReqCH.query(req, "SELECT number FROM numbers WHERE number > {num:UInt8} LIMIT 3", [num: 5], [])
      iex> response.body
      "6\\n7\\n8\\n"

  This function can accept `Req` options, as well as mixing them with `ReqCH` options:

      iex> opts = [base_url: "http://example.org:8123", database: "system", auth: {:basic, "user:pass"}]
      iex> {:ok, response} = ReqCH.query(ReqCH.new(), "SELECT number FROM numbers LIMIT 3", [], opts)
      iex> response.body
      "0\\n1\\n2\\n"

  """
  @spec query(
          Req.Request.t(),
          sql_query :: binary(),
          sql_query_params :: map() | Keyword.t(),
          opts :: Keyword.t()
        ) :: {:ok, Req.Response.t()} | {:error, binary()}
  def query(req, sql_query, sql_query_params \\ [], opts \\ [])

  def query(%Req.Request{} = req, sql_query, sql_query_params, opts)
      when is_binary(sql_query) and is_query_params(sql_query_params) and is_list(opts) do
    req
    |> attach(opts)
    |> put_params(prepare_params(sql_query_params))
    |> Req.post(body: sql_query)
  end

  @doc """
  Same as `query/4`, but raises in case of error.
  """
  @spec query!(
          Req.Request.t(),
          sql_query :: binary(),
          sql_query_params :: map() | Keyword.t(),
          opts :: Keyword.t()
        ) :: Req.Response.t()
  def query!(req, sql_query, sql_query_params \\ [], opts \\ [])

  def query!(%Req.Request{} = req, sql_query, sql_query_params, opts) do
    case query(req, sql_query, sql_query_params, opts) do
      {:ok, response} -> response
      {:error, exception} -> raise exception
    end
  end

  defp run(%Req.Request{private: %{clickhouse_format: _}} = request), do: request

  defp run(%Req.Request{} = request) do
    request = update_in(request.options, &Map.put_new(&1, :base_url, "http://localhost:8123"))

    with %Req.Request{} = req1 <- add_format(request),
         %Req.Request{} = req2 <- maybe_add_database(req1) do
      Req.Request.append_response_steps(req2, clickhouse_result: &handle_clickhouse_result/1)
    end
  end

  defp put_params(request, params) do
    encoded = URI.encode_query(params, :rfc3986)

    update_in(request.url.query, fn
      nil -> encoded
      query -> query <> "&" <> encoded
    end)
  end

  defp prepare_params(params) do
    Enum.map(params, fn {key, value} -> {"param_#{key}", prepare_param_value(value)} end)
  end

  defp prepare_param_value(text) when is_binary(text) do
    escapes = [{"\\", "\\\\"}, {"\t", "\\\t"}, {"\n", "\\\n"}]

    Enum.reduce(escapes, text, fn {pattern, replacement}, text ->
      String.replace(text, pattern, replacement)
    end)
  end

  defp prepare_param_value(%DateTime{} = datetime) do
    unix_microseconds =
      datetime
      |> DateTime.shift_zone!("Etc/UTC")
      |> DateTime.to_unix(:microsecond)

    unix_seconds = unix_microseconds / 1_000_000
    unix_seconds_trunc = trunc(unix_seconds)

    if unix_seconds_trunc == unix_seconds do
      unix_seconds_trunc
    else
      :erlang.float_to_binary(unix_seconds, decimals: 6)
    end
  end

  defp prepare_param_value(array) when is_list(array) do
    elements = Enum.map(array, &prepare_array_param_value/1)
    IO.iodata_to_binary([?[, Enum.intersperse(elements, ?,), ?]])
  end

  defp prepare_param_value(tuple) when is_tuple(tuple) do
    elements = Enum.map(Tuple.to_list(tuple), &prepare_array_param_value/1)
    IO.iodata_to_binary([?(, Enum.intersperse(elements, ?,), ?)])
  end

  defp prepare_param_value(struct) when is_struct(struct), do: to_string(struct)

  defp prepare_param_value(map) when is_map(map) do
    elements = Enum.map(Map.to_list(map), &prepare_map_param_value/1)
    IO.iodata_to_binary([?{, Enum.intersperse(elements, ?,), ?}])
  end

  defp prepare_param_value(other), do: to_string(other)

  defp prepare_array_param_value(text) when is_binary(text) do
    text = prepare_param_value(text)
    [?', String.replace(text, "'", "''"), ?']
  end

  defp prepare_array_param_value(%s{} = param) when s in [Date, NaiveDateTime] do
    [?', to_string(param), ?']
  end

  defp prepare_array_param_value(other), do: prepare_param_value(other)

  defp prepare_map_param_value({key, value}) do
    key = prepare_array_param_value(key)
    value = prepare_array_param_value(value)
    [key, ?:, value]
  end

  @valid_formats [:tsv, :csv, :json, :explorer]

  defp add_format(%Req.Request{} = request) do
    format_option = Req.Request.get_option(request, :format, :tsv)
    format = normalise_format(format_option)

    if format do
      format_header = with :explorer <- format, do: "Parquet"

      request
      |> Req.Request.put_private(:clickhouse_format, format)
      |> Req.Request.put_header("x-clickhouse-format", format_header)
    else
      raise ArgumentError,
            "the given format #{inspect(format_option)} is invalid. Expecting one of #{inspect(@valid_formats)} " <>
              "or one of the valid options described in #{@formats_page}"
    end
  end

  defp normalise_format(:tsv), do: "TabSeparated"
  defp normalise_format(:csv), do: "CSV"
  defp normalise_format(:json), do: "JSON"

  if Code.ensure_loaded?(Explorer) do
    defp normalise_format(:explorer), do: :explorer
  else
    defp normalise_format(:explorer) do
      raise ArgumentError,
            "format: :explorer - you need to install Explorer as a dependency in order to use this format"
    end
  end

  defp normalise_format(format) when format in @supported_formats, do: format

  defp normalise_format(_), do: nil

  defp maybe_add_database(%Req.Request{} = request) do
    if database = Req.Request.get_option(request, :database) do
      put_params(request, database: database)
    else
      request
    end
  end

  defp handle_clickhouse_result({request, %{status: 200} = response} = pair) do
    want_explorer_df = Req.Request.get_private(request, :clickhouse_format) == :explorer
    is_parquet_response = response.headers["x-clickhouse-format"] == ["Parquet"]

    if want_explorer_df and is_parquet_response do
      Req.Request.halt(request, update_in(response.body, &load_parquet/1))
    else
      pair
    end
  end

  defp handle_clickhouse_result(request_response), do: request_response

  if Code.ensure_loaded?(Explorer) do
    defp load_parquet(body) do
      Explorer.DataFrame.load_parquet!(body)
    end
  else
    defp load_parquet(_body) do
      raise ArgumentError,
            "format: :explorer - you need to install Explorer as a dependency in order to use this format"
    end
  end
end
