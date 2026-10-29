"""Small Flight SQL interoperability server for the SELECT test case.

Run with: python tests/server.py --port 8815
Use --self-test to exercise the service with PyArrow's Flight client.
"""

import argparse
import re
import sqlite3
import time
import uuid

import pyarrow as pa
import pyarrow.flight as flight


QUERY_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
CATALOGS_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetCatalogs"
SCHEMAS_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetDbSchemas"
TABLES_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetTables"
TYPES_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetTableTypes"
PRIMARY_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetPrimaryKeys"
IMPORTED_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetImportedKeys"
EXPORTED_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetExportedKeys"
CROSS_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.CommandGetCrossReference"
TICKET_TYPE = b"type.googleapis.com/arrow.flight.protocol.sql.TicketStatementQuery"
QUERY = "SELECT id, name FROM items ORDER BY id"
SCHEMA = pa.schema([pa.field("id", pa.int64()), pa.field("name", pa.string())])


def decode_metadata_request(data):
    values = {}
    pos = 0
    def read_varint():
        nonlocal pos
        result = 0
        for shift in range(0, 70, 7):
            if pos >= len(data):
                raise ValueError("truncated varint")
            b = data[pos]
            pos += 1
            result |= (b & 127) << shift
            if not b & 128:
                return result
        raise ValueError("invalid varint")
    while pos < len(data):
        tag = read_varint()
        if tag >> 3 == 0:
            raise ValueError("invalid field")
        if tag & 7 == 2:
            length = read_varint()
            if length > len(data) - pos:
                raise ValueError("truncated field")
            value = data[pos:pos + length].decode("utf-8")
            pos += length
        elif tag & 7 == 0:
            value = read_varint()
        else:
            raise ValueError("unexpected wire type")
        values.setdefault(tag >> 3, []).append(value)
    return values


def metadata_result(kind, request, bad_schema=False):
    args = decode_metadata_request(request)
    def optional(tag):
        return args[tag][-1] if tag in args else None
    def matches_catalog(value):
        arg = optional(1)
        return arg is None or (value is None if arg == "" else value == arg)
    def matches_pattern(value, pattern):
        if pattern is None:
            return True
        if value is None:
            return False
        regex = "".join(".*" if c == "%" else "." if c == "_" else re.escape(c) for c in pattern)
        return re.fullmatch(regex, value, flags=re.DOTALL) is not None
    schemas = [(None, "public"), ("main", "analytics"), ("main", "public")]
    tables = [(None, None, "local_tbl", "TABLE"), ("main", "analytics", "sales", "TABLE"), ("main", "public", "order_view", "VIEW"), ("main", "public", "orders", "TABLE")]
    if kind == SCHEMAS_TYPE:
        schema = pa.schema([pa.field("catalog_name", pa.string()), pa.field("db_schema_name", pa.string(), nullable=False)])
        rows = [{"catalog_name": cat, "db_schema_name": name} for cat, name in schemas if matches_catalog(cat) and matches_pattern(name, optional(2))]
    elif kind == TYPES_TYPE:
        if request:
            raise ValueError("table types command must be empty")
        schema = pa.schema([pa.field("table_type", pa.string(), nullable=False)])
        rows = [{"table_type": "TABLE"}, {"table_type": "VIEW"}]
    elif kind == PRIMARY_TYPE:
        schema = pa.schema([pa.field("catalog_name", pa.string()), pa.field("db_schema_name", pa.string()), pa.field("table_name", pa.string(), nullable=False), pa.field("column_name", pa.string(), nullable=False), pa.field("key_name", pa.string()), pa.field("key_sequence", pa.int32(), nullable=False)])
        candidates = [(None, None, "local_tbl", "id", None, 1), ("main", "public", "orders", "order_id", "orders_pk", 1), ("main", "public", "orders", "region", "orders_pk", 2)]
        rows = []
        for cat, group, table, column, key, sequence in candidates:
            schema_filter = optional(2)
            if not matches_catalog(cat) or (schema_filter is not None and (group is not None if schema_filter == "" else group != schema_filter)) or table != optional(3):
                continue
            rows.append(dict(zip(schema.names, (cat, group, table, column, key, sequence))))
    elif kind in (IMPORTED_TYPE, EXPORTED_TYPE, CROSS_TYPE):
        schema = pa.schema([
            pa.field("pk_catalog_name", pa.string()), pa.field("pk_db_schema_name", pa.string()),
            pa.field("pk_table_name", pa.string(), nullable=False), pa.field("pk_column_name", pa.string(), nullable=False),
            pa.field("fk_catalog_name", pa.string()), pa.field("fk_db_schema_name", pa.string()),
            pa.field("fk_table_name", pa.string(), nullable=False), pa.field("fk_column_name", pa.string(), nullable=False),
            pa.field("key_sequence", pa.int32(), nullable=False), pa.field("fk_key_name", pa.string()),
            pa.field("pk_key_name", pa.string()), pa.field("update_rule", pa.uint8(), nullable=False),
            pa.field("delete_rule", pa.uint8(), nullable=False)])
        # The imported-key table is the foreign table; the exported-key table is its parent.
        candidates = [("main", "public", "customers", "id", "main", "public", "orders", "customer_id", 1, "orders_customer_fk", "customers_pk", 0, 2),
                      (None, None, "local_parent", "id", None, None, "local_child", "parent_id", 1, None, None, 3, 4)]
        rows = []
        for values in candidates:
            def matches_optional(value, tag):
                expected = optional(tag)
                return expected is None or (value is None if expected == "" else value == expected)
            if kind == CROSS_TYPE:
                if not all((matches_optional(values[0], 1), matches_optional(values[1], 2),
                            values[2] == optional(3), matches_optional(values[4], 4),
                            matches_optional(values[5], 5), values[6] == optional(6))):
                    continue
            else:
                cat, group, table = values[4:7] if kind == IMPORTED_TYPE else values[:3]
                if not matches_optional(cat, 1) or not matches_optional(group, 2) or table != optional(3):
                    continue
            rows.append(dict(zip(schema.names, values)))
    else:
        include = bool(args.get(5, [0])[-1])
        fields = [pa.field("catalog_name", pa.string()), pa.field("db_schema_name", pa.string()), pa.field("table_name", pa.string(), nullable=False), pa.field("table_type", pa.string(), nullable=False)]
        if include:
            fields.append(pa.field("table_schema", pa.binary(), nullable=False))
        schema = pa.schema(fields)
        rows = []
        for cat, group, name, kind_name in tables:
            if not matches_catalog(cat) or not matches_pattern(group, optional(2)) or not matches_pattern(name, optional(3)) or (args.get(4) and kind_name not in args[4]):
                continue
            row = {"catalog_name": cat, "db_schema_name": group, "table_name": name, "table_type": kind_name}
            if include:
                row["table_schema"] = pa.schema([pa.field("id", pa.int64())]).serialize().to_pybytes()
            rows.append(row)
    if bad_schema:
        original = schema.field(0)
        schema = schema.set(0, pa.field("wrong_column", original.type, nullable=original.nullable))
        for row in rows:
            row["wrong_column"] = row.pop(original.name)
    return schema, rows


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
    def __init__(self, port, token=None, large=False, medium=False, many=False, multi=False, unknown_count=False, delay=0, host="127.0.0.1", bad_catalog_schema=False, schema_mismatch=False, empty_catalog=False, catalog_no_endpoints=False, bad_metadata_schema=False, metadata_schema_mismatch=False, metadata_no_endpoints=False):
        middleware = {"auth": TokenMiddleware(token)} if token is not None else None
        bind_host = f"[{host}]" if ":" in host else host
        super().__init__(f"grpc://{bind_host}:{port}", middleware=middleware)
        self.host = bind_host
        self.tickets = {}
        self.large = large
        self.medium = medium
        self.many = many
        self.multi = multi
        self.unknown_count = unknown_count
        self.delay = delay
        self.bad_catalog_schema = bad_catalog_schema
        self.schema_mismatch = schema_mismatch
        self.empty_catalog = empty_catalog
        self.catalog_no_endpoints = catalog_no_endpoints
        self.bad_metadata_schema = bad_metadata_schema
        self.metadata_schema_mismatch = metadata_schema_mismatch
        self.metadata_no_endpoints = metadata_no_endpoints
        self.metadata_tickets = {}

    def get_flight_info(self, context, descriptor):
        if self.delay:
            time.sleep(self.delay)
        try:
            envelope = parse_fields(descriptor.command)
            if envelope.get(1) in (SCHEMAS_TYPE, TABLES_TYPE, TYPES_TYPE, PRIMARY_TYPE, IMPORTED_TYPE, EXPORTED_TYPE, CROSS_TYPE):
                kind, request = envelope[1], envelope.get(2, b"")
                schema, rows = metadata_result(kind, request, self.bad_metadata_schema)
                endpoints = []
                for index in range(0 if self.metadata_no_endpoints else (2 if self.metadata_schema_mismatch or self.multi else 1)):
                    ticket = b"metadata:" + uuid.uuid4().bytes
                    self.metadata_tickets[ticket] = (kind, request, self.metadata_schema_mismatch and index == 1)
                    endpoints.append(flight.FlightEndpoint(ticket, []))
                return flight.FlightInfo(schema, descriptor, endpoints, len(rows) * len(endpoints), -1)
            if envelope.get(1) == CATALOGS_TYPE:
                if envelope.get(2, b"") != b"":
                    raise ValueError("catalog command must be empty")
                schema = pa.schema([pa.field("catalog_name", pa.string(), nullable=self.bad_catalog_schema)])
                endpoints = [] if self.catalog_no_endpoints else [flight.FlightEndpoint(descriptor.command, [])]
                return flight.FlightInfo(schema, descriptor, endpoints, 0 if self.empty_catalog or self.catalog_no_endpoints else 2, -1)
            query = unpack_any(descriptor.command, QUERY_TYPE)[1].decode("utf-8")
        except (KeyError, ValueError, UnicodeDecodeError) as exc:
            raise flight.FlightServerError(f"invalid SQL command: {exc}") from exc
        if query.strip().rstrip(";").upper() != QUERY.upper():
            raise flight.FlightServerError("unsupported SQL query")
        endpoints = []
        count = 2 if self.schema_mismatch or (self.multi and query.strip().endswith(";")) else 1
        for index in range(count):
            handle = uuid.uuid4().bytes
            self.tickets[handle] = self.schema_mismatch and index == count - 1
            ticket = pack_any(TICKET_TYPE, field(1, handle))
            endpoints.append(flight.FlightEndpoint(ticket, []))
        total = -1 if self.unknown_count else (100 if self.many else (1 if self.large or self.medium else 4))
        if total >= 0:
            total *= len(endpoints)
        return flight.FlightInfo(SCHEMA, descriptor, endpoints, total, -1)

    def do_get(self, context, ticket):
        try:
            if ticket.ticket in self.metadata_tickets:
                kind, request, mismatch = self.metadata_tickets.pop(ticket.ticket)
                schema, rows = metadata_result(kind, request, self.bad_metadata_schema or mismatch)
                batches = [pa.RecordBatch.from_pylist(rows, schema=schema)] if rows else []
                return flight.RecordBatchStream(pa.RecordBatchReader.from_batches(schema, batches))
            envelope = parse_fields(ticket.ticket)
            if envelope.get(1) == CATALOGS_TYPE:
                schema = pa.schema([pa.field("catalog_name", pa.string(), nullable=self.bad_catalog_schema)])
                rows = [] if self.empty_catalog else [{"catalog_name": "main"}, {"catalog_name": "warehouse"}]
                batches = [pa.RecordBatch.from_pylist(rows, schema=schema)] if rows else []
                return flight.RecordBatchStream(pa.RecordBatchReader.from_batches(schema, batches))
            handle = unpack_any(ticket.ticket, TICKET_TYPE)[1]
        except (KeyError, ValueError) as exc:
            raise flight.FlightServerError(f"invalid ticket: {exc}") from exc
        if handle not in self.tickets:
            raise flight.FlightServerError("unknown or already used ticket")
        mismatch = self.tickets.pop(handle)
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
        if mismatch:
            schema = pa.schema([pa.field("id", pa.int64()), pa.field("renamed", pa.string())])
            batches = [pa.RecordBatch.from_pylist([{"id": row[0], "renamed": row[1]} for row in rows[i:i + 2]], schema=schema) for i in (0, 2)]
            return flight.RecordBatchStream(pa.RecordBatchReader.from_batches(schema, batches))
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
    catalog_info = client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(CATALOGS_TYPE, b"")), options=options)
    assert catalog_info.schema.equals(pa.schema([pa.field("catalog_name", pa.string(), nullable=False)]))
    assert client.do_get(catalog_info.endpoints[0].ticket, options=options).read_all().to_pylist() == [{"catalog_name": "main"}, {"catalog_name": "warehouse"}]
    schema_info = client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(SCHEMAS_TYPE, field(1, b""))), options=options)
    assert client.do_get(schema_info.endpoints[0].ticket, options=options).read_all().to_pylist() == [{"catalog_name": None, "db_schema_name": "public"}]
    table_request = field(1, b"main") + field(2, b"pub%") + field(3, b"order%") + field(4, b"VIEW") + b"\x28\x01"
    table_info = client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(TABLES_TYPE, table_request)), options=options)
    table_rows = client.do_get(table_info.endpoints[0].ticket, options=options).read_all().to_pylist()
    assert len(table_rows) == 1 and table_rows[0]["table_name"] == "order_view"
    assert pa.ipc.read_schema(pa.BufferReader(table_rows[0]["table_schema"])).field(0).name == "id"
    types_info = client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(TYPES_TYPE, b"")), options=options)
    assert client.do_get(types_info.endpoints[0].ticket, options=options).read_all().to_pylist() == [{"table_type": "TABLE"}, {"table_type": "VIEW"}]
    key_request = field(1, b"main") + field(2, b"public") + field(3, b"orders")
    keys_info = client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(PRIMARY_TYPE, key_request)), options=options)
    keys = client.do_get(keys_info.endpoints[0].ticket, options=options).read_all().to_pylist()
    assert [(key["column_name"], key["key_sequence"]) for key in keys] == [("order_id", 1), ("region", 2)]
    fk_request = field(1, b"main") + field(2, b"public") + field(3, b"orders")
    fk_info = client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(IMPORTED_TYPE, fk_request)), options=options)
    fk = client.do_get(fk_info.endpoints[0].ticket, options=options).read_all().to_pylist()
    assert len(fk) == 1 and fk[0]["pk_table_name"] == "customers" and fk[0]["delete_rule"] == 2
    exported_request = field(3, b"customers")
    exported_info = client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(EXPORTED_TYPE, exported_request)), options=options)
    exported = client.do_get(exported_info.endpoints[0].ticket, options=options).read_all().to_pylist()
    assert exported == fk
    cross_request = field(1, b"main") + field(2, b"public") + field(3, b"customers") + field(4, b"main") + field(5, b"public") + field(6, b"orders")
    cross_info = client.get_flight_info(flight.FlightDescriptor.for_command(pack_any(CROSS_TYPE, cross_request)), options=options)
    assert client.do_get(cross_info.endpoints[0].ticket, options=options).read_all().to_pylist() == fk
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
    parser.add_argument("--bad-catalog-schema", action="store_true", help="emit nonconforming nullable catalog schema")
    parser.add_argument("--schema-mismatch", action="store_true", help="make the second endpoint use a different schema")
    parser.add_argument("--empty-catalog", action="store_true", help="return no catalog names")
    parser.add_argument("--catalog-no-endpoints", action="store_true", help="return catalog schema with no endpoints")
    parser.add_argument("--bad-metadata-schema", action="store_true", help="return invalid schema/table metadata column")
    parser.add_argument("--metadata-schema-mismatch", action="store_true", help="return a different schema on the second metadata endpoint")
    parser.add_argument("--metadata-no-endpoints", action="store_true", help="return metadata schema without endpoints")
    args = parser.parse_args()
    server = TestFlightSqlServer(args.port, args.token, args.large, args.medium, args.many, args.multi, args.unknown_count, args.delay, args.host, args.bad_catalog_schema, args.schema_mismatch, args.empty_catalog, args.catalog_no_endpoints, args.bad_metadata_schema, args.metadata_schema_mismatch, args.metadata_no_endpoints)
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
