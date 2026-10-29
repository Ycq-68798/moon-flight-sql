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

Optional `metadata` is sent with both `GetFlightInfo` and each `DoGet` request. For example, pass `metadata=[{ name: "authorization", value: "Bearer TOKEN" }]`. Names use lowercase ASCII; values use printable ASCII. Binary and reserved gRPC metadata are rejected. The current connection is plaintext h2c: credentials and query data are visible on the network, so use this only within a protected local connection or tunnel.

The client checks the total number of rows against `FlightInfo.total_records`. It rejects queries when the server reports an unknown count (`-1`), because the transport can silently discard an oversized batch. This check relies on an accurate server count: matching counts do not validate individual values, detect all corruption, or exclude offsetting errors across endpoints. Many Flight SQL servers report unknown counts, so they cannot currently be queried by this client.

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

For authentication, run `python tests/server.py --port 8815 --token demo-token` in one terminal and `moon run cmd/auth --target native` in another. This verifies missing and wrong tokens are rejected and the correct token works for both RPCs. For an oversized batch with unknown row count, use `python tests/server.py --port 8815 --large --unknown-count` and `moon run cmd/unknown --target native`; the query is rejected before retrieval. To reproduce the oversized-message check with a known count, use `python tests/server.py --port 8815 --large` and `moon run cmd/large --target native`.

## Current scope

- Unencrypted h2c, IPv4 numeric address and sequential use of one connection. TLS, authentication handshake and DNS are not implemented. Bearer credentials can be passed as metadata over a protected connection.
- Endpoints on the current connection are supported, including the reuse-connection location. An endpoint requiring another location is rejected; all returned endpoints are consumed in order.
- MoonRPC currently buffers the complete `DoGet` response before decoding; this is not a streaming or bounded-memory implementation. Arrow types and compression are limited by MoonArrow's reader.
- MoonRPC 0.21.0 limits an individual gRPC message to 4 MiB. In the tested version, a larger FlightData message can be dropped while the server's success status is still returned. Row counts are checked to catch this failure when the server reports an accurate count. The upstream transport may still buffer the oversized message, and a server that reports an incorrect count can defeat this check. Proper handling requires a transport fix that retains the error and aborts the oversized stream.
- Statement query only: prepared statements, parameter binding, transactions, cancellation and SQL metadata APIs are not implemented. The project is an interoperability prototype rather than a complete Flight SQL driver.

Protocol references: [Flight.proto](https://github.com/apache/arrow/blob/main/format/Flight.proto) and [FlightSql.proto](https://github.com/apache/arrow/blob/main/format/FlightSql.proto).
