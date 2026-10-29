"""Small Flight SQL interoperability server for the SELECT test case.

Run with: python tests/server.py --port 8815
Use --self-test to exercise the service with PyArrow's Flight client.
"""

import argparse
import sqlite3
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
    def __init__(self, port, token=None, large=False, unknown_count=False):
        middleware = {"auth": TokenMiddleware(token)} if token is not None else None
        super().__init__(f"grpc://127.0.0.1:{port}", middleware=middleware)
        self.tickets = set()
        self.large = large
        self.unknown_count = unknown_count

    def get_flight_info(self, context, descriptor):
        try:
            query = unpack_any(descriptor.command, QUERY_TYPE)[1].decode("utf-8")
        except (KeyError, ValueError, UnicodeDecodeError) as exc:
            raise flight.FlightServerError(f"invalid SQL command: {exc}") from exc
        if query.strip().rstrip(";").upper() != QUERY.upper():
            raise flight.FlightServerError("unsupported SQL query")
        handle = uuid.uuid4().bytes
        self.tickets.add(handle)
        ticket = pack_any(TICKET_TYPE, field(1, handle))
        endpoint = flight.FlightEndpoint(ticket, [])
        total = -1 if self.unknown_count else (1 if self.large else 4)
        return flight.FlightInfo(SCHEMA, descriptor, [endpoint], total, -1)

    def do_get(self, context, ticket):
        try:
            handle = unpack_any(ticket.ticket, TICKET_TYPE)[1]
        except (KeyError, ValueError) as exc:
            raise flight.FlightServerError(f"invalid ticket: {exc}") from exc
        if handle not in self.tickets:
            raise flight.FlightServerError("unknown or already used ticket")
        self.tickets.remove(handle)
        if self.large:
            batch = pa.RecordBatch.from_pylist(
                [{"id": 1, "name": "x" * (5 * 1024 * 1024)}], schema=SCHEMA
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
    client = flight.FlightClient(f"grpc://127.0.0.1:{server.port}")
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
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument("--token", help="require this bearer token on query and retrieval")
    parser.add_argument("--large", action="store_true", help="send a single batch exceeding 4 MiB")
    parser.add_argument("--unknown-count", action="store_true", help="report unknown total row count")
    args = parser.parse_args()
    server = TestFlightSqlServer(args.port, args.token, args.large, args.unknown_count)
    if args.self_test:
        try:
            self_test(server, args.token)
        finally:
            server.shutdown()
    else:
        print(f"Flight SQL test server listening on 127.0.0.1:{server.port}", flush=True)
        server.serve()


if __name__ == "__main__":
    main()
