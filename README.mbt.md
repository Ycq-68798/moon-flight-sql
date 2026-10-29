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

`Client::connect` accepts `max_query_bytes`, a positive limit on the total gRPC DATA bytes received across `GetFlightInfo` and all `DoGet` calls in one query. The default is 64 MiB. On exceeding it the client raises `FlightError::Rpc(8, ...)` and closes the connection; open a new client for subsequent queries. The limit includes gRPC message prefixes and resets for each query.

Optional `metadata` is sent with both `GetFlightInfo` and each `DoGet` request. For example, pass `metadata=[{ name: "authorization", value: "Bearer TOKEN" }]`. Names use lowercase ASCII; values use printable ASCII. Binary and reserved gRPC metadata are rejected. The current connection is plaintext h2c: credentials and query data are visible on the network, so use this only within a protected local connection or tunnel.

The client accepts `FlightInfo.total_records = -1` (unknown). When the server provides a count, the client compares it with the decoded total across endpoints. This check relies on an accurate server count: matching counts do not validate individual values, detect all corruption, or exclude offsetting errors across endpoints.

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

The integration example checks two record batches, four exact rows including a null, and an invalid-SQL gRPC error. `python tests/server.py --port 0 --self-test` separately checks the fixture using PyArrow's own client.

For authentication, run `python tests/server.py --port 8815 --token demo-token` in one terminal and `moon run cmd/auth --target native` in another. This verifies missing and wrong tokens are rejected and the correct token works for both RPCs. For unknown counts, run the server with `--unknown-count` and use `moon run cmd/unknown --target native`. For an oversized batch, run the server with `--large --unknown-count` and use `moon run cmd/large --target native`; the client rejects the response with gRPC status 8 and closes the connection. The same test also works with a known count by omitting `--unknown-count`. Run the server with `--medium --unknown-count` and use `moon run cmd/medium --target native` to check a 3 MiB batch. The `--many`/`cmd/limit` and `--multi`/`cmd/multi` pairs check limits across small batches and endpoints. The `--delay 1`/`cmd/timeout` pair checks timeouts and concurrent-query rejection.

## Current scope

- Unencrypted h2c, IPv4 numeric address and sequential use of one connection. Concurrent queries on one client are rejected. TLS, authentication handshake and DNS are not implemented. Bearer credentials can be passed as metadata over a protected connection.
- Endpoints on the current connection are supported, including the reuse-connection location. An endpoint requiring another location is rejected; all returned endpoints are consumed in order.
- Responses are buffered in memory before decoding, not streamed. `max_query_bytes` bounds received gRPC DATA bytes across a query, not HTTP/2 headers or process peak memory; framing copies and decoded Arrow values can require more memory. Arrow types and IPC compression are limited by MoonArrow's reader.
- MoonRPC 0.21.0 caps a single gRPC message at 4 MiB and may silently discard an oversized message. This client uses its own sequential socket driver around MoonRPC's HTTP/2 engine and checks the gRPC length prefix before passing each DATA frame to that engine. A message exceeding 4 MiB raises `FlightError::Rpc(8, ...)` and closes the connection; reconnect to continue. It does not receive batches larger than 4 MiB. Truncated gRPC messages and compressed gRPC messages are rejected; the latter avoids unchecked expansion in the dependency.
- Statement query only: prepared statements, parameter binding, transactions, cancellation and SQL metadata APIs are not implemented. The project is an interoperability prototype rather than a complete Flight SQL driver.

Protocol references: [Flight.proto](https://github.com/apache/arrow/blob/main/format/Flight.proto) and [FlightSql.proto](https://github.com/apache/arrow/blob/main/format/FlightSql.proto).
