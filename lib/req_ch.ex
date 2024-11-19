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

      The `:explorer` format is special, and will build an Explorer dataframe
      in case the `:explorer` dependency is installed.

    * `:database` - Optional. The database to use in the queries.
      Default is `nil`.

  ## Examples

  After setting a default database, one can make a request directly:

      iex> req = ReqCH.new(database: "system")
      iex> Req.post!(req, body: "SELECT number FROM numbers LIMIT 3").body
      1\n2\n3\n

  It's also possible to make a query using `Req.get/2`:

      iex> req = ReqCH.new(database: "system")
      iex> Req.get!(req, params: [query: "SELECT number FROM numbers LIMIT 3"]).body
      1\n2\n3\n

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
  Performs a query against ClickHouse API.

  See docs from `new/1` for details about the options.

  ## Examples

  Queries can be performed using both `Req.get/2` or `Req.post/2`, but GET
  is "read-only" and commands like `CREATE` or `INSERT` cannot be used with it.
  For that reason, by default we perform a `POST` request.
  To change that, use `query/2` with a pre-configured `req`.

  A plain query:

      iex> {:ok, response} = ReqCH.query("SELECT number FROM system.numbers LIMIT 3")
      iex> response.body
      "0\\n1\\n2\\n"

  Changing the format to `:explorer` will return a dataframe:

      iex> {:ok, response} = ReqCH.query("SELECT number FROM system.numbers LIMIT 3", [], format: :explorer)
      iex> response.body
      #Explorer.DataFrame<
        Polars[3 x 1]
        number u64 [0, 1, 2]
      >

  Using parameters is also possible:

      iex> opts = [format: :explorer, database: "system"]
      iex> {:ok, response} = ReqCH.query("SELECT number FROM numbers WHERE number > {num:UInt8} LIMIT 3", [num: 5], opts)
      #Explorer.DataFrame<
        Polars[3 x 1]
        number u64 [6, 7, 8]
      >

  """
  @spec query(sql_query :: binary(), params :: Map.t() | Keyword.t(), opts :: Keyword.t()) ::
          {:ok, Req.Response.t()} | {:error, binary()}
  def query(sql_query, params \\ [], opts \\ [])

  def query(sql_query, params, opts)
      when is_binary(sql_query) and is_query_params(params) and is_list(opts) do
    opts
    |> new()
    |> put_params(prepare_params(params))
    |> Req.post(body: sql_query)
  end

  @doc """
  Same as `query/3`, but raises in case of error.
  """
  @spec query!(sql_query :: binary(), params :: Map.t() | Keyword.t(), opts :: Keyword.t()) ::
          Req.Response.t()
  def query!(sql_query, params \\ [], opts \\ [])

  def query!(sql_query, params, opts)
      when is_binary(sql_query) and is_query_params(params) and is_list(opts) do
    case query(sql_query, params, opts) do
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
    Enum.map(params, fn {key, value} -> {"param_#{key}", value} end)
  end

  defp add_format(%Req.Request{} = request) do
    format_option = Req.Request.get_option(request, :format, :tsv)
    format = normalise_format(format_option)

    if format do
      format_header = with :explorer <- format, do: "Parquet"

      request
      |> Req.Request.put_private(:clickhouse_format, format)
      |> Req.Request.put_header("x-clickhouse-format", format_header)
    else
      Req.Request.halt(
        request,
        format_error(format_option)
      )
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

  @valid_formats [:tsv, :csv, :json, :explorer]

  defp format_error(format) do
    ArgumentError.exception(
      "the given format #{inspect(format)} is invalid. Expecting one of #{inspect(@valid_formats)} " <>
        "or one of the valid options described in #{@formats_page}"
    )
  end

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
