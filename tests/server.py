"""Small Flight SQL interoperability server for the SELECT test case.

Run with: python tests/server.py --port 8815
Use --self-test to exercise the service with PyArrow's Flight client.
"""

import argparse
import sqlite3
import time
import uuid

import pyarrow as pa
import pyarrow.flight as flight


QUERY_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
TICKET_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.TicketStatementQuery"
QUERY = "SELECT id, name FROM items ORDER BY id"
SCHEMA = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])


def varint(value):
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def field(number, value):
    return varint(number * 8 + 2) + varint(len(value)) + value


def parse_fields(data):
    def read_varint(position):
        value = 0
        for shift in range(0, 70, 7):
            if position >= len(data):
                raise ValueError("truncated protobuf varint")
            byte = data[position]
            position += 1
            value |= (byte & 127) << shift
            if not byte & 128:
                return value, position
        raise ValueError("invalid protobuf varint")

    result = {}
    position = 0
    while position < len(data):
        tag, position = read_varint(position)
        if tag & 7 != 2 or tag >> 3 == 0:
            raise ValueError("unsupported protobuf field")
        length, position = read_varint(position)
        end = position + length
        if end > len(data):
            raise ValueError("truncated protobuf field")
        result[tag >> 3] = data[position:end]
        position = end
    return result


def pack_any(type_url, value):
    return field(1, type_url) + field(2, value)


def unpack_any(data, type_url):
    fields = parse_fields(data)
    if fields.get(1) != type_url or 2 not in fields:
        raise ValueError("unexpected Flight SQL message type")
    return parse_fields(fields[2])


class TokenMiddleware(flight.ServerMiddlewareFactory):
    def __init__(self, token):
        self.expected = f"Bearer {token}"

    def start_call(self, info, headers):
        if info.method in (flight.FlightMethod.GET_FLIGHT_INFO, flight.FlightMethod.DO_GET):
            if headers.get("authorization", []) != [self.expected]:
                raise flight.FlightUnauthenticatedError("invalid bearer token")
        return None


class TestFlightSqlServer(flight.FlightServerBase):
    def __init__(self, port, token=None, large=False, medium=False, many=False, multi=False, unknown_count=False, delay=0, host="127.0.0.1"):
        middleware = {"auth": TokenMiddleware(token)} if token is not None else None
        bind_host = f"[{host}]" if ":" in host else host
        super().__init__(f"grpc://{bind_host}:{port}", middleware=middleware)
        self.host = bind_host
        self.tickets = set()
        self.large = large
        self.medium = medium
        self.many = many
        self.multi = multi
        self.unknown_count = unknown_count
        self.delay = delay

    def get_flight_info(self, context, descriptor):
        if self.delay:
            time.sleep(self.delay)
        try:
            query = unpack_any(descriptor.command, QUERY_TYPE)[1].decode("utf-8")
        except (KeyError, ValueError, UnicodeDecodeError) as exc:
            raise flight.FlightServerError(f"invalid SQL command: {exc}") from exc
        if query.strip().rstrip(";").upper() != QUERY.upper():
            raise flight.FlightServerError("unsupported SQL query")
        endpoints = []
        for _ in range(2 if self.multi and query.strip().endswith(";") else 1):
            handle = uuid.uuid4().bytes
            self.tickets.add(handle)
            ticket = pack_any(TICKET_TYPE, field(1, handle))
            endpoints.append(flight.FlightEndpoint(ticket, []))
        total = -1 if self.unknown_count else (100 if self.many else (1 if self.large or self.medium else 4))
        if total >= 0:
            total *= len(endpoints)
        return flight.FlightInfo(SCHEMA, descriptor, endpoints, total, -1)

    def do_get(self, context, ticket):
        try:
            handle = unpack_any(ticket.ticket, TICKET_TYPE)[1]
        except (KeyError, ValueError) as exc:
            raise flight.FlightServerError(f"invalid ticket: {exc}") from exc
        if handle not in self.tickets:
            raise flight.FlightServerError("unknown or already used ticket")
        self.tickets.remove(handle)
        if self.many:
            batches = [pa.RecordBatch.from_pylist([{"id": i, "name": "x"}], schema=SCHEMA) for i in range(100)]
            return flight.RecordBatchStream(pa.RecordBatchReader.from_batches(SCHEMA, batches))
        if self.large or self.medium:
            batch = pa.RecordBatch.from_pylist(
                [{"id": 1, "name": "x" * ((5 if self.large else 3) * 1024 * 1024)}], schema=SCHEMA
            )
            return flight.RecordBatchStream(pa.RecordBatchReader.from_batches(SCHEMA, [batch]))
        db = sqlite3.connect(":memory:")
        try:
            db.execute("CREATE TABLE items (id INTEGER, name TEXT)")
            db.executemany("INSERT INTO items VALUES (?, ?)", [(1, "alpha"), (2, None), (3, "gamma"), (4, "delta")])
            rows = db.execute(QUERY).fetchall()
        finally:
            db.close()
        batches = [pa.RecordBatch.from_pylist([{"id": row[0], "name": row[1]} for row in rows[i:i + 2]], schema=SCHEMA) for i in (0, 2)]
        return flight.RecordBatchStream(pa.RecordBatchReader.from_batches(SCHEMA, batches))


def self_test(server, token=None):
    client = flight.FlightClient(f"grpc://{server.host}:{server.port}")
    command = pack_any(QUERY_TYPE, field(1, QUERY.encode()))
    descriptor = flight.FlightDescriptor.for_command(command)
    options = None
    if token is not None:
        for bad in (None, "wrong"):
            bad_options = None if bad is None else flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {bad}".encode())])
            try:
                client.get_flight_info(descriptor, options=bad_options)
            except flight.FlightUnauthenticatedError:
                pass
            else:
                raise AssertionError("invalid token was accepted")
        options = flight.FlightCallOptions(headers=[(b"authorization", f"Bearer {token}".encode())])
    info = client.get_flight_info(descriptor, options=options)
    if token is not None:
        try:
            client.do_get(info.endpoints[0].ticket)
        except flight.FlightUnauthenticatedError:
            pass
        else:
            raise AssertionError("DoGet accepted missing token")
    batches = list(client.do_get(info.endpoints[0].ticket, options=options))
    assert len(batches) == 2
    assert pa.Table.from_batches([chunk.data for chunk in batches]).to_pylist() == [
        {"id": 1, "name": "alpha"}, {"id": 2, "name": None},
        {"id": 3, "name": "gamma"}, {"id": 4, "name": "delta"},
    ]
    try:
        client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(QUERY_TYPE, field(1, b"SELECT invalid"))), options=options)
    except flight.FlightServerError:
        pass
    else:
        raise AssertionError("invalid query was accepted")
    print("PASS: SQL command, two batches, null, and server error", flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8815)
    parser.add_argument("--host", default="127.0.0.1", help="bind address, e.g. ::1 for IPv6")
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--token", help="require this bearer token on query and retrieval")
    parser.add_argument("--large", action="store_true", help="send a single batch exceeding 4 MiB")
    parser.add_argument("--medium", action="store_true", help="send a single 3 MiB batch")
    parser.add_argument("--many", action="store_true", help="send 100 small batches")
    parser.add_argument("--multi", action="store_true", help="send two endpoints for a query ending in semicolon")
    parser.add_argument("--unknown-count", action="store_true", help="report unknown total row count")
    parser.add_argument("--delay", type=float, default=0, help="delay GetFlightInfo by this many seconds")
    args = parser.parse_args()
    server = TestFlightSqlServer(args.port, args.token, args.large, args.medium, args.many, args.multi, args.unknown_count, args.delay, args.host)
    if args.self_test:
        try:
            self_test(server, args.token)
        finally:
            server.shutdown()
    else:
        print(f"Flight SQL test server listening on {server.host}:{server.port}", flush=True)
        server.serve()


if __name__ == "__main__":
    main()
