defmodule ReqCH do
  @moduledoc """
  A Req plugin for ClickHouse.

  By default, `ReqCH` will use TSV as the default output format.
  To change that, see the `attach/2` docs for details.
  """

  @connection_options [
    :database
  ]

  @general_options [
    :clickhouse,
    :format
  ]

  @formats_page "https://clickhouse.com/docs/en/interfaces/formats"
  @supported_formats ~w(TabSeparated TabSeparatedRaw TabSeparatedWithNames TabSeparatedWithNamesAndTypes TabSeparatedRawWithNames TabSeparatedRawWithNamesAndTypes Template TemplateIgnoreSpaces CSV CSVWithNames CSVWithNamesAndTypes CustomSeparated CustomSeparatedWithNames CustomSeparatedWithNamesAndTypes SQLInsert Values Vertical JSON JSONAsString JSONAsObject JSONStrings JSONColumns JSONColumnsWithMetadata JSONCompact JSONCompactStrings JSONCompactColumns JSONEachRow PrettyJSONEachRow JSONEachRowWithProgress JSONStringsEachRow JSONStringsEachRowWithProgress JSONCompactEachRow JSONCompactEachRowWithNames JSONCompactEachRowWithNamesAndTypes JSONCompactStringsEachRow JSONCompactStringsEachRowWithNames JSONCompactStringsEachRowWithNamesAndTypes JSONObjectEachRow BSONEachRow TSKV Pretty PrettyNoEscapes PrettyMonoBlock PrettyNoEscapesMonoBlock PrettyCompact PrettyCompactNoEscapes PrettyCompactMonoBlock PrettyCompactNoEscapesMonoBlock PrettySpace PrettySpaceNoEscapes PrettySpaceMonoBlock PrettySpaceNoEscapesMonoBlock Prometheus Protobuf ProtobufSingle ProtobufList Avro AvroConfluent Parquet ParquetMetadata Arrow ArrowStream ORC One Npy RowBinary RowBinaryWithNames RowBinaryWithNamesAndTypes RowBinaryWithDefaults Native Null XML CapnProto LineAsString Regexp RawBLOB MsgPack MySQLDump DWARF Markdown Form)

  @doc """
  Attach this plugin to a Req Request.

  ## Options

    * `:clickhouse` - The query to be performed. If not provided,
      the request and response pipelines won't be modified.
    * `:format` - Optional. The format of the response. Default is `:tsv`.
      This option accepts `:tsv`, `:csv`, `:json` or `:explorer` as atoms.

      It also accepts all formats described in the #{@formats_page} page.

      The `:explorer` format is special, and will build an Explorer dataframe
      in case the `:explorer` dependency is installed.
    * `:database` - Optional. The database to use in the queries.
      Default is `nil`.

  ## Examples

  Queries can be performed using both `Req.get/2` or `Req.post/2`, but GET
  is "read-only" and commands like `CREATE` or `INSERT` cannot be used with it.

  A plain query:

      iex> req = Req.new() |> ReqCH.attach()
      iex> Req.get!(req, clickhouse: "SELECT number FROM system.numbers LIMIT 3").body
      "0\\n1\\n2\\n"

  Changing the format to `:explorer` will return a dataframe:

      iex> req = Req.new() |> ReqCH.attach()
      iex> Req.get!(req, clickhouse: "SELECT number FROM system.numbers LIMIT 3", format: :explorer).body
      #Explorer.DataFrame<
        Polars[3 x 1]
        number u64 [0, 1, 2]
      >

  Using parameters is also possible:

      iex> req = Req.new() |> ReqCH.attach(format: :explorer, database: "system")
      iex> Req.get!(req, clickhouse: {"SELECT number FROM numbers WHERE number > {num:UInt8} LIMIT 3", [num: 5]}).body
      #Explorer.DataFrame<
        Polars[3 x 1]
        number u64 [6, 7, 8]
      >

  In case a different endpoint is needed, we can use the `:base_url` option
  from `Req` to set that value. We can use use `:auth` to pass down the username
  and password:

      iex> req = Req.new(base_url: "https://example.org:8123", auth: {:basic, "user:pass"})
      iex> req = ReqCH.attach(req)
      iex> Req.post!(req, clickhouse: "SELECT number FROM system.numbers LIMIT 3").body
      "0\\n1\\n2\\n"

  """
  def attach(%Req.Request{} = request, opts \\ []) do
    request
    |> Req.Request.prepend_request_steps(clickhouse_run: &run/1)
    |> Req.Request.register_options(@connection_options ++ @general_options)
    |> Req.Request.merge_options(opts)
  end

  defguardp is_query_params(value) when is_list(value) or is_map(value)

  @doc false
  def query(query, opts \\ [])

  @doc false
  def query(sql, opts) when is_binary(sql) and is_list(opts) do
    do_query(Req.new(), sql, opts)
  end

  def query({sql, params} = query, opts)
      when is_binary(sql) and is_query_params(params) and is_list(opts) do
    do_query(Req.new(), query, opts)
  end

  def query(%Req.Request{} = req, query), do: query(req, query, [])

  @doc false
  def query(%Req.Request{} = req, sql, opts) when is_binary(sql) and is_list(opts) do
    do_query(req, sql, opts)
  end

  def query(%Req.Request{} = req, {sql, params} = query, opts)
      when is_binary(sql) and is_query_params(params) and is_list(opts) do
    do_query(req, query, opts)
  end

  defp do_query(req, query, opts) do
    req
    |> attach(opts)
    |> Req.post!(clickhouse: query)
  end

  defp run(%Req.Request{private: %{clickhouse_format: _}} = request), do: request

  defp run(%Req.Request{options: %{clickhouse: _query}} = request) do
    request = update_in(request.options, &Map.put_new(&1, :base_url, "http://localhost:8123"))

    request
    |> add_format()
    |> add_query()
    |> maybe_add_database()
    |> Req.Request.append_response_steps(clickhouse_result: &handle_clickhouse_result/1)
  end

  defp run(%Req.Request{} = request), do: request

  defp add_query(%Req.Request{} = request) do
    query = Req.Request.fetch_option!(request, :clickhouse)

    case query do
      {sql, params} when is_binary(sql) and is_query_params(params) ->
        request
        |> add_sql_part(sql)
        |> put_params(prepare_params(params))

      sql when is_binary(sql) ->
        add_sql_part(request, sql)
    end
  end

  defp add_sql_part(%Req.Request{method: :post} = request, sql), do: %{request | body: sql}

  defp add_sql_part(%Req.Request{method: :get} = request, sql),
    do: put_params(request, query: sql)

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
    format = request |> Req.Request.get_option(:format, :tsv) |> normalise_format!()

    format_header = with :explorer <- format, do: "Parquet"

    request
    |> Req.Request.put_private(:clickhouse_format, format)
    |> Req.Request.put_header("x-clickhouse-format", format_header)
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
  defp normalise_format!(format) do
    if valid = normalise_format(format) do
      valid
    else
      raise ArgumentError,
            "the given format #{inspect(format)} is invalid. Expecting one of #{inspect(@valid_formats)} " <>
              "or one of the valid options described in #{@formats_page}"
    end
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
