# Moon Flight SQL

Native MoonBit client for executing Flight SQL statements and reading Arrow record batches. It sends `CommandStatementQuery` through `GetFlightInfo`, retrieves every endpoint with `DoGet`, and decodes the resulting Arrow IPC messages using [MoonArrow](https://github.com/buildliming/MoonArrow). The gRPC and protobuf implementation comes from [MoonRPC](https://github.com/moonbitstack/moonrpc).

## Requirements

- MoonBit toolchain 2026-10-28 or newer and a working native C compiler. Windows requires MSVC; Linux/macOS require a suitable C toolchain.
- MoonRPC 0.21.0, MoonArrow 0.2.1 and moonbitlang/async 0.22.5, resolved by `moon install`.
- For the interoperability fixture only: Python 3 and PyArrow with Flight support (`pip install pyarrow`).

## Use

Add the published module once available, or use a local dependency while developing. Import `Ycq-68798/moon-flight-sql` and `moonbitlang/async`. The client is asynchronous and requires the async runtime:

```moonbit nocheck
///|
fn main {
  @async.run_async_main(() => {
    let client = @flight.Client::connect("127.0.0.1", 8815)
    defer client.close()
    let endpoints = client.query("SELECT id, name FROM items ORDER BY id")
    for endpoint in endpoints {
      for batch in endpoint.batches {
        for row in 0..<batch.num_rows {
          println("\{Repr(batch.get_row(row))}")
        }
      }
    }
  })
}
```

`query` propagates transport and Arrow errors; nonzero gRPC status is raised as `FlightError::Rpc`. The optional `timeout_ms` applies to each RPC separately and defaults to 30 seconds. SQL is sent to the server as given; the server decides what it is allowed to execute.

`Client::connect` accepts `max_query_bytes`, a positive limit on received HTTP/2 frame bytes across `GetFlightInfo` and all `DoGet` calls in one query. It includes each frame's 9-byte header, header blocks, DATA, and control frames received during the RPCs. The default is 64 MiB. On exceeding it the client raises `FlightError::Rpc(8, ...)` and closes the connection; open a new client for subsequent queries. The budget resets for each query.

`client.catalogs()` returns catalog names using Flight SQL `CommandGetCatalogs`. It accepts the same `metadata` and `timeout_ms` options as `query`, and checks the required non-null UTF-8 `catalog_name` schema.

`client.db_schemas(catalog?, pattern?)` and `client.tables(catalog?, schema_pattern?, table_pattern?, table_types~, include_schema~)` discover database schemas and tables. Omitting `catalog` applies no catalog filter; explicitly passing `catalog=""` requests entries without a catalog. Schema and table patterns use the server's SQL LIKE semantics (`%` and `_`). An empty `table_types` array leaves table types unrestricted. With `include_schema=true`, each `TableInfo.table_schema` contains the server-provided Arrow IPC schema bytes; `tables()` checks the column type and non-nullness without decoding the bytes. Call `TableInfo::decode_schema()` to decode it, or `decode_table_schema(bytes)` for standalone bytes. The decoder accepts one legacy or continuation-prefixed IPC schema message, requires exact framing with no trailing bytes (including an end-of-stream marker), and propagates MoonArrow decoding errors. `decode_schema()` returns `None` when no schema was requested. Supported Arrow types and validation limits are those of MoonArrow. Framing checks do not prove that every byte within the declared FlatBuffer metadata is referenced or canonical; MoonArrow performs the metadata decoding. Both methods accept `metadata` and `timeout_ms` like `query`.

`client.table_types()` returns the available table types. `client.primary_keys(table, catalog?, schema?)` returns primary-key columns for an exact table name, including nullable catalog, schema and key names and the 32-bit key sequence. Omitting `catalog` or `schema` leaves that filter unspecified; passing an empty string requests entries with no catalog or schema. Both methods accept `metadata` and `timeout_ms` and validate the standard response columns.

`client.imported_keys(table, catalog?, schema?)` and `client.exported_keys(table, catalog?, schema?)` retrieve foreign-key relationships. They return both sides of each relationship, key sequence and nullable key names. Update and delete rules are the protocol's unsigned byte codes: 0 cascade, 1 restrict, 2 set null, 3 no action, 4 set default; values are returned as received. Omitting a catalog or schema leaves it unspecified; an empty string selects entries without one. Both methods accept `metadata` and `timeout_ms`.

For the imported/exported commands, the fixture treats the imported table as the foreign table and the exported table as the referenced parent table, matching the command descriptions and common JDBC semantics. FlightSql.proto's individual field comments describe these tables in the opposite direction. The client forwards the provided values without applying its own filtering; check the server's behavior when integrating with it. The included tests use a hand-written PyArrow Flight SQL fixture and have not verified this ambiguity against a third-party Flight SQL implementation.

`client.cross_reference(pk_table, fk_table, pk_catalog?, pk_schema?, fk_catalog?, fk_schema?)` finds relationships between a named parent table and foreign table. Optional catalog and schema filters retain the same omitted-versus-empty distinction independently on both sides. It uses the same `ForeignKey` result and response schema checks as the imported/exported methods.

`client.xdbc_type_info(data_type?)` returns server XDBC type descriptions with column sizes, literal delimiters, creation parameters and other attributes. Omit `data_type` for all types; explicitly passing zero filters for code 0, and negative XDBC codes are supported. Nullable values and the distinction between a null and empty parameter list are preserved. The method checks the standard response schema, including non-null UTF-8 list items, and accepts `metadata` and `timeout_ms`.

For an empty catalog response with no endpoints, `catalogs()` validates the schema in `FlightInfo` and returns an empty list. If both the schema and endpoints are absent, it raises `FlightError::Protocol` because the catalog schema cannot be checked.

The host may be a DNS name such as `localhost`, an IPv4 address, or an unbracketed IPv6 literal such as `::1`; pass the port separately. The client sends the host and port as HTTP/2 `:authority`, adding brackets around IPv6 literals. URI schemes, bracketed IPv6 inputs and IPv6 zone identifiers are rejected.

Optional `metadata` is sent with both `GetFlightInfo` and each `DoGet` request. For example, pass `metadata=[{ name: "authorization", value: "Bearer TOKEN" }]`. Names use lowercase ASCII; values use printable ASCII. Binary and reserved gRPC metadata are rejected. The current connection is plaintext h2c: credentials and query data are visible on the network, so use this only within a protected local connection or tunnel.

The client accepts `FlightInfo.total_records = -1` (unknown). When the server provides a count, the client compares it with the decoded total across endpoints. It checks each endpoint's field names, physical Arrow types, nullability and child fields against the FlightInfo schema; if FlightInfo omits its schema, the first endpoint is used as the reference. Arrow metadata, including extension-type metadata, is not compared, so this is not a check of full schema semantics. A matching count and structure do not validate individual values, detect all corruption, or exclude offsetting errors across endpoints.

## Verify

The included PyArrow fixture implements the Flight SQL statement protocol for a fixed SQLite query. It is a real network server, but it is deliberately not a general-purpose Flight SQL server. In one terminal:

```sh
python tests/server.py --port 8815
```

In another terminal:

```sh
moon test --target native
moon run cmd/main --target native
```

The integration example checks two record batches, four exact rows including a null, and an invalid-SQL gRPC error. `moon run cmd/catalogs --target native` checks the catalog API against that server. `python tests/server.py --port 0 --self-test` separately checks SQL and catalog responses using PyArrow's own client.

For authentication, run `python tests/server.py --port 8815 --token demo-token` in one terminal and `moon run cmd/auth --target native` in another. This verifies missing and wrong tokens are rejected and the correct token works for both RPCs. For unknown counts, run the server with `--unknown-count` and use `moon run cmd/unknown --target native`. For an oversized batch, run the server with `--large --unknown-count` and use `moon run cmd/large --target native`; the client rejects the response with gRPC status 8 and closes the connection. The same test also works with a known count by omitting `--unknown-count`. Run the server with `--medium --unknown-count` and use `moon run cmd/medium --target native` to check a 3 MiB batch. The `--many`/`cmd/limit` and `--multi`/`cmd/multi` pairs check limits across small batches and endpoints. The `--delay 1`/`cmd/timeout` pair checks timeouts and concurrent-query rejection.

For address handling, use the default server and run `moon run cmd/address --target native` for DNS, IPv4 and invalid inputs. Run `python tests/server.py --host ::1 --port 8816` and `moon run cmd/ipv6 --target native` for IPv6 loopback; IPv6 must be enabled on the host. Additional fixture modes `--empty-catalog`, `--bad-catalog-schema` and `--schema-mismatch` are exercised by the correspondingly named commands in CI. The `tests/header_attack.py` fixture exercises repeated and oversized response headers.

With the default server, `moon run cmd/discovery --target native` checks schema, table, table type, primary-key, foreign-key, cross-reference and XDBC type discovery, filters and decoding a PyArrow schema with nested fields and metadata; it also rejects truncated and trailing schema bytes. The fixture modes `--metadata-no-endpoints`, `--bad-metadata-schema` and `--metadata-schema-mismatch` exercise empty and invalid discovery responses; CI runs their corresponding `cmd/discovery_*` tests.

## Current scope

- Unencrypted h2c and sequential use of one connection. Concurrent queries on one client are rejected. TLS and authentication handshake are not implemented. The current async TLS API does not expose the ALPN negotiation required by standard gRPC, while MoonRPC's HTTP/2 engine fixes `:scheme` to `http`. Bearer credentials can be passed as metadata over a protected connection.
- Endpoints on the current connection are supported, including the reuse-connection location. An endpoint requiring another location is rejected; all returned endpoints are consumed in order.
- Responses are buffered in memory before decoding, not streamed. `max_query_bytes` bounds received HTTP/2 frame bytes across a query, not process peak memory; HPACK expansion, framing copies and decoded Arrow values can require more memory. Arrow types and IPC compression are limited by MoonArrow's reader.
- MoonRPC 0.21.0 caps a single gRPC message at 4 MiB and may silently discard an oversized message. This client uses its own sequential socket driver around MoonRPC's HTTP/2 engine and checks the gRPC length prefix before passing each DATA frame to that engine. A message exceeding 4 MiB raises `FlightError::Rpc(8, ...)` and closes the connection; reconnect to continue. It does not receive batches larger than 4 MiB. Truncated gRPC messages and compressed gRPC messages are rejected; the latter avoids unchecked expansion in the dependency.
- Unexpected response DATA or header streams, unsolicited CONTINUATION frames, interleaved header blocks, and server push are rejected before they reach the MoonRPC engine. HTTP/2 connection control frames remain accepted.
- Response headers require a valid HTTP status; malformed, duplicated, or misplaced gRPC status fields and pseudo-headers in trailers are rejected. A successful gRPC status requires HTTP 200, and a missing gRPC status on HTTP 200 maps to UNKNOWN (2). Header decoding retains the connection-wide HPACK state across sequential calls.
- Statement queries and catalog, database schema, table, table type, primary-key, foreign-key, cross-reference and XDBC type discovery are implemented. Prepared statements, parameter binding, transactions, cancellation and other SQL metadata APIs are not implemented. The project is an interoperability prototype rather than a complete Flight SQL driver.

Protocol references: [Flight.proto](https://github.com/apache/arrow/blob/main/format/Flight.proto) and [FlightSql.proto](https://github.com/apache/arrow/blob/main/format/FlightSql.proto).
