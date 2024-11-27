# ReqCH

[![Docs](https://img.shields.io/badge/hex.pm-docs-8e7ce6.svg)](https://hexdocs.pm/req_ch)
[![Hex pm](http://img.shields.io/hexpm/v/req_ch.svg?style=flat&color=blue)](https://hex.pm/packages/req_ch)

A [Req](https://github.com/wojtekmach/req) plugin for [ClickHouse](https://clickhouse.com/).

## Usage

Assuming that you have a ClickHouse server running on localhost, this code is going to run:

```elixir
Mix.install([
  {:req_ch, "~> 0.1.0"}
])

req = ReqCH.new(database: "system")

ReqCH.query!(req, "SELECT number FROM numbers LIMIT 5").body
# => "0\n1\n2\n3\n4\n"

ReqCH.query!(req, "SELECT number FROM numbers WHERE number > {num:UInt8} LIMIT 5", [num: 12]).body
# => "13\n14\n15\n16\n17\n"
```

It's also possible to return `Explorer` dataframes, if the `:explorer` package is installed
and the `:explorer` format is used:

```elixir
Mix.install([
  {:req_ch, "~> 0.1.0"},
  {:explorer, "~> 0.10.0"}
])

req = ReqCH.new(database: "system")

ReqCH.query!(req, "SELECT number, number - 2 as less_two FROM numbers LIMIT 5", [], [format: :explorer]).body
# => #Explorer.DataFrame<
#   Polars[5 x 2]
#   number u64 [0, 1, 2, 3, 4]
#   less_two s64 [-2, -1, 0, 1, 2]
# >
```

See the [documentation](https://hexdocs.pm/req_ch) for details.

## License

Copyright (C) 2024 Dashbit

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at [http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
