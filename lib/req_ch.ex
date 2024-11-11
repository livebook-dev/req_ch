defmodule ReqCh do
  @moduledoc """
  A Req plugin for ClickHouse.

  By default, `ReqCh` will use TSV as the default output format.
  To change that, see the `attach/2` docs for details.
  """

  @connection_options [
    :database,
    :scheme,
    :username,
    :password,
    :hostname,
    :port
  ]

  @general_options [
    :clickhouse,
    :format
  ]

  @formats_page "https://clickhouse.com/docs/en/interfaces/formats"
  @supported_formats ~w(TabSeparated TabSeparatedRaw TabSeparatedWithNames TabSeparatedWithNamesAndTypes TabSeparatedRawWithNames TabSeparatedRawWithNamesAndTypes Template TemplateIgnoreSpaces CSV CSVWithNames CSVWithNamesAndTypes CustomSeparated CustomSeparatedWithNames CustomSeparatedWithNamesAndTypes SQLInsert Values Vertical JSON JSONAsString JSONAsObject JSONStrings JSONColumns JSONColumnsWithMetadata JSONCompact JSONCompactStrings JSONCompactColumns JSONEachRow PrettyJSONEachRow JSONEachRowWithProgress JSONStringsEachRow JSONStringsEachRowWithProgress JSONCompactEachRow JSONCompactEachRowWithNames JSONCompactEachRowWithNamesAndTypes JSONCompactStringsEachRow JSONCompactStringsEachRowWithNames JSONCompactStringsEachRowWithNamesAndTypes JSONObjectEachRow BSONEachRow TSKV Pretty PrettyNoEscapes PrettyMonoBlock PrettyNoEscapesMonoBlock PrettyCompact PrettyCompactNoEscapes PrettyCompactMonoBlock PrettyCompactNoEscapesMonoBlock PrettySpace PrettySpaceNoEscapes PrettySpaceMonoBlock PrettySpaceNoEscapesMonoBlock Prometheus Protobuf ProtobufSingle ProtobufList Avro AvroConfluent Parquet ParquetMetadata Arrow ArrowStream ORC One Npy RowBinary RowBinaryWithNames RowBinaryWithNamesAndTypes RowBinaryWithDefaults Native Null XML CapnProto LineAsString Regexp RawBLOB MsgPack MySQLDump DWARF Markdown Form)

  import Req.Request, only: [get_option: 3]

  @doc """
  Attach this plugin to a Req Request.

  ## Options

    * `:scheme` - Required. Either `"http"` or `"https"`. Default is `"http"`.
    * `:username` - Optional. Default is `nil`.
    * `:password` - Optional. Default is `nil`.
    * `:hostname` - Required. Default is `"localhost"`.
    * `:port` - Required. Default is `"8123"`.
    * `:clickhouse` - Required. The query to be performed. If not provided,
      the request and response pipelines won't be modified.
    * `:format` - Optional. The format of the response. Default is `:tsv`.
      This option accepts `:tsv`, `:csv` or `:explorer` as atoms.

      It also accepts all formats described in the #{@formats_page} page.

      The `:explorer` format is special, and will build an Explorer dataframe
      in case the `:explorer` dependency is installed.

  ## Examples

  With a plain query:

      iex> req = Req.new() |> ReqCh.attach()
      iex> Req.post!(req, clickhouse: "SELECT number from system.numbers LIMIT 3").body
      "0\\n1\\n2\\n"

  Changing the format to `:explorer` will return a dataframe:

      iex> req = Req.new() |> ReqCh.attach()
      iex> Req.post!(req, clickhouse: "SELECT number from system.numbers LIMIT 3", format: :explorer).body
      #Explorer.DataFrame<
        Polars[3 x 1]
        number u64 [0, 1, 2]
      >

  """
  def attach(%Req.Request{} = request, opts \\ []) do
    request
    |> Req.Request.prepend_request_steps(clickhouse_run: &run/1)
    |> Req.Request.register_options(@connection_options ++ @general_options)
    |> Req.Request.merge_options(opts)
  end

  defp run(%Req.Request{private: %{clickhouse_format: _}} = request), do: request

  defp run(%Req.Request{options: %{clickhouse: _query}} = request) do
    url_parts = [
      get_option(request, :scheme, "http"),
      "://",
      maybe_credentials(request),
      get_option(request, :hostname, "localhost"),
      ":",
      get_option(request, :port, "8123")
    ]

    url =
      url_parts
      |> IO.iodata_to_binary()
      |> URI.parse()

    request
    |> add_format()
    |> add_query()
    |> Req.Request.append_response_steps(clickhouse_result: &handle_clickhouse_result/1)
    |> Map.replace!(:url, url)
  end

  defp run(%Req.Request{} = request), do: request

  defp add_query(%Req.Request{} = request) do
    query = Req.Request.fetch_option!(request, :clickhouse)

    %{request | body: query}
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

  @valid_formats [:tsv, :csv, :explorer]
  defp normalise_format!(format) do
    if valid = normalise_format(format) do
      valid
    else
      raise ArgumentError,
            "the given format #{inspect(format)} is invalid. Expecting one of #{inspect(@valid_formats)} " <>
              "or one of the valid options described in #{@formats_page}"
    end
  end

  defp maybe_credentials(%Req.Request{} = request) do
    user = Req.Request.get_option(request, :username)

    if user do
      [user, ":", Req.Request.get_option(request, :password, ""), "@"]
    else
      []
    end
  end

  defp handle_clickhouse_result({request, %{status: 200} = response} = pair) do
    want_explorer_df = Req.Request.get_private(request, :clickhouse_format) == :explorer
    is_parquet_response = response.headers["x-clickhouse-format"] == ["Parquet"]

    if want_explorer_df and is_parquet_response do
      Req.Request.halt(request, %{response | body: load_parquet(response.body)})
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
