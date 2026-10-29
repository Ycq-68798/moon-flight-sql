///|
test "statement descriptor wraps the SQL command in protobuf Any" {
  let expected : Bytes = b"\x08\x02\x12\x49\x0a\x43" +
    b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery" +
    b"\x12\x02\x0a\x00"
  assert_eq(statement_descriptor(""), expected)
}

///|
test "endpoint parsing preserves ticket and locations" {
  let ticket = @proto.PbWriter::new()
  ticket.bytes_(1, b"abc")
  let location = @proto.PbWriter::new()
  location.string_(1, "arrow-flight-reuse-connection://?")
  let endpoint = @proto.PbWriter::new()
  endpoint.message_(1, ticket.to_bytes())
  endpoint.message_(2, location.to_bytes())
  let info = @proto.PbWriter::new()
  info.message_(3, endpoint.to_bytes())
  let decoded = decode_endpoints(info.to_bytes())
  assert_eq(decoded.length(), 1)
  assert_eq(decoded[0].ticket, ticket.to_bytes())
  assert_eq(decoded[0].locations[0], "arrow-flight-reuse-connection://?")
}

///|
test "FlightData frames metadata with eight-byte alignment" {
  let data = @proto.PbWriter::new()
  data.bytes_(2, b"123")
  data.bytes_(1000, b"body")
  assert_eq(
    flight_data_to_ipc([data.to_bytes()]),
    b"\xff\xff\xff\xff\x08\x00\x00\x00123\x00\x00\x00\x00\x00body",
  )
}

///|
test "missing endpoint ticket is rejected" {
  let info = @proto.PbWriter::new()
  info.message_(3, b"")
  let rejected = try {
    let _ = decode_endpoints(info.to_bytes())
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(rejected)
}

///|
test "request metadata accepts ASCII and rejects unsafe or reserved fields" {
  let valid = encode_metadata([
    { name: "authorization", value: "Bearer demo-token", },
    { name: "x-request-id", value: "request-123", },
  ])
  assert_eq(valid.length(), 2)
  assert_eq(valid[0].value, b"Bearer demo-token")
  let invalid : Array[Metadata] = [
    { name: "Authorization", value: "x", },
    { name: "x-test", value: "line\nbreak", },
    { name: "grpc-timeout", value: "1S", },
    { name: "x-test-bin", value: "binary", },
    { name: "", value: "x", },
    { name: ":authority", value: "x", },
  ]
  for entry in invalid {
    let rejected = try {
      let _ = encode_metadata([entry])
      false
    } catch {
      FlightError::Protocol(_) => true
      _ => false
    }
    assert_true(rejected)
  }
}

///|
test "FlightInfo count handles known, absent and unknown values" {
  let known = @proto.PbWriter::new()
  known.int64(4, 42L)
  assert_eq(total_records(known.to_bytes()), 42L)
  assert_eq(total_records(b""), 0L)
  let unknown = @proto.PbWriter::new()
  unknown.int64(4, -1L)
  assert_eq(total_records(unknown.to_bytes()), -1L)
  let invalid = @proto.PbWriter::new()
  invalid.int64(4, -2L)
  let rejected = try {
    let _ = total_records(invalid.to_bytes())
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(rejected)
}

///|
test "gRPC message guard handles split prefixes, bodies, and empty messages" {
  let validator = MessageGuard::new()
  validator.push(b"\x00\x00")
  validator.push(b"\x00\x00\x03ab")
  validator.push(b"c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01z")
  validator.finish()
}

///|
test "gRPC message guard rejects oversized and truncated frames" {
  let oversized = MessageGuard::new()
  oversized.push(b"\x00\x00\x40")
  let rejected = try {
    oversized.push(b"\x00\x01")
    false
  } catch {
    FlightError::Rpc(8, _) => true
    _ => false
  }
  assert_true(rejected)
  let partial = MessageGuard::new()
  partial.push(b"\x00\x00\x00\x00\x02x")
  let truncated = try {
    partial.finish()
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(truncated)
  let partial_header = MessageGuard::new()
  partial_header.push(b"\x00\x00")
  let header_truncated = try {
    partial_header.finish()
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(header_truncated)
}

///|
test "gRPC message guard rejects compression and accepts 4 MiB prefix" {
  let compressed = MessageGuard::new()
  let rejected = try {
    compressed.push(b"\x01\x00\x00\x00\x00")
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(rejected)
  let boundary = MessageGuard::new()
  boundary.push(b"\x00\x00\x40\x00\x00")
  assert_eq(boundary.remaining, 4194304)
}

///|
test "query response limit counts frames across RPCs" {
  let budget = QueryBudget::new(25)
  budget.charge(9 + 10)
  budget.charge(6)
  let rejected = try {
    budget.charge(1)
    false
  } catch {
    FlightError::Rpc(8, "query responses exceed the configured byte limit") =>
      true
    _ => false
  }
  assert_true(rejected)
}

///|
test "HTTP authority formats DNS, IPv4 and IPv6 endpoints" {
  assert_eq(make_authority("localhost", 8815), "localhost:8815")
  assert_eq(make_authority("127.0.0.1", 1234), "127.0.0.1:1234")
  assert_eq(make_authority("::1", 8816), "[::1]:8816")
  let engine = @rpcclient.H2Client::new()
  let (_, frames) = engine.unary(
    "/test",
    b"",
    authority=make_authority("::1", 8816),
  )
  guard frames[0] is @http2.Frame::Headers(fragment~, ..) else {
    fail("expected request headers")
  }
  let headers = @hpack.Decoder::new().decode(fragment[:])
  let mut found = false
  for header in headers {
    if header.name == b":authority" && header.value == b"[::1]:8816" {
      found = true
    }
  }
  assert_true(found)
}

///|
test "HTTP2 response guard accepts control frames and rejects unexpected streams" {
  let f = FrameGuard::new(1)
  f.check(@http2.Frame::Settings(params=[], ack=true))
  f.check(@http2.Frame::Ping(payload=b"12345678", ack=false))
  f.check(@http2.Frame::WindowUpdate(stream_id=0, increment=1))
  f.check(@http2.Frame::WindowUpdate(stream_id=3, increment=1))
  f.check(@http2.Frame::RstStream(stream_id=3, error_code=0))
  f.check(@http2.Frame::GoAway(last_stream_id=1, error_code=0, debug=b""))
  f.check(
    @http2.Frame::Headers(
      stream_id=1,
      fragment=b"",
      end_stream=false,
      end_headers=false,
      priority=None,
      padding=0,
    ),
  )
  f.check(
    @http2.Frame::Continuation(stream_id=1, fragment=b"", end_headers=true),
  )
  f.check(
    @http2.Frame::Data(stream_id=1, data=b"", end_stream=false, padding=0),
  )
  let wrong = try {
    f.check(
      @http2.Frame::Headers(
        stream_id=3,
        fragment=b"",
        end_stream=false,
        end_headers=false,
        priority=None,
        padding=0,
      ),
    )
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(wrong)
  let unsolicited = try {
    f.check(
      @http2.Frame::Continuation(stream_id=3, fragment=b"", end_headers=false),
    )
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(unsolicited)
  let wrong_data = try {
    f.check(
      @http2.Frame::Data(stream_id=3, data=b"", end_stream=false, padding=0),
    )
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(wrong_data)
  let server_push = try {
    f.check(
      @http2.Frame::PushPromise(
        stream_id=1,
        promised_id=2,
        fragment=b"",
        end_headers=true,
        padding=0,
      ),
    )
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(server_push)
  let repeated = try {
    f.check(
      @http2.Frame::Headers(
        stream_id=1,
        fragment=b"",
        end_stream=false,
        end_headers=false,
        priority=None,
        padding=0,
      ),
    )
    false
  } catch {
    FlightError::Protocol("response trailers must end the stream") => true
    _ => false
  }
  assert_true(repeated)
  let partial_block = FrameGuard::new(1)
  partial_block.check(
    @http2.Frame::Headers(
      stream_id=1,
      fragment=b"",
      end_stream=false,
      end_headers=false,
      priority=None,
      padding=0,
    ),
  )
  let interleaved = try {
    partial_block.check(@http2.Frame::Ping(payload=b"12345678", ack=false))
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(interleaved)
}

///|
test "schema comparison checks nested fields but ignores custom metadata" {
  assert_true(flight_info_schema(b"") is None)
  let empty = @proto.PbWriter::new()
  empty.bytes_(1, b"")
  assert_true(flight_info_schema(empty.to_bytes()) is None)
  let child = @arrow.Field::new("item", @arrow.DataType::Int64, nullable=false)
  let baseline = [
    @arrow.Field::new("items", @arrow.DataType::List, children=[child]),
  ]
  let with_metadata = [
    @arrow.Field::new("items", @arrow.DataType::List, children=[child], metadata=[
      { key: "note", value: Some("x"), },
    ]),
  ]
  assert_true(fields_match(baseline, with_metadata))
  let other_child = @arrow.Field::new(
    "item",
    @arrow.DataType::Int32,
    nullable=false,
  )
  let different = [
    @arrow.Field::new("items", @arrow.DataType::List, children=[other_child]),
  ]
  assert_false(fields_match(baseline, different))
  let nullable_child = @arrow.Field::new(
    "item",
    @arrow.DataType::Int64,
    nullable=true,
  )
  assert_false(
    fields_match(baseline, [
      @arrow.Field::new("items", @arrow.DataType::List, children=[
        nullable_child,
      ]),
    ]),
  )
}

///|
test "response data before headers is rejected" {
  let guarder = FrameGuard::new(1)
  let rejected = try {
    guarder.check(
      @http2.Frame::Data(stream_id=1, data=b"", end_stream=false, padding=0),
    )
    false
  } catch {
    FlightError::Protocol("HTTP/2 data before response headers") => true
    _ => false
  }
  assert_true(rejected)
}

///|
test "catalogs validate FlightInfo schema even with no endpoints" {
  let valid = @arrow.Schema::new([
    @arrow.Field::new("catalog_name", @arrow.DataType::Utf8, nullable=false),
  ])
  let result : DescriptorResult = { schema: Some(valid), parts: [], }
  assert_true(catalog_names(result).is_empty())
  let invalid = @arrow.Schema::new([
    @arrow.Field::new("catalog_name", @arrow.DataType::Utf8, nullable=true),
  ])
  let bad : DescriptorResult = { schema: Some(invalid), parts: [], }
  let rejected = try {
    let _ = catalog_names(bad)
    false
  } catch {
    FlightError::Protocol("invalid Flight SQL catalog schema") => true
    _ => false
  }
  assert_true(rejected)
  let missing : DescriptorResult = { schema: None, parts: [], }
  let missing_rejected = try {
    let _ = catalog_names(missing)
    false
  } catch {
    FlightError::Protocol("missing Flight SQL catalog schema") => true
    _ => false
  }
  assert_true(missing_rejected)
}

///|
fn test_command_payload(descriptor : Bytes) -> Bytes raise {
  let outer = @proto.PbReader::new(descriptor)
  let mut any : Bytes = b""
  while !outer.eof() {
    let (field, wire) = outer.read_tag()
    if field == 2 && wire == LengthDelim {
      any = outer.read_bytes()
    } else {
      outer.skip(wire)
    }
  }
  let inner = @proto.PbReader::new(any)
  let mut value : Bytes = b""
  while !inner.eof() {
    let (field, wire) = inner.read_tag()
    if field == 2 && wire == LengthDelim {
      value = inner.read_bytes()
    } else {
      inner.skip(wire)
    }
  }
  value
}

///|
test "metadata commands preserve optional presence and repeated types" {
  assert_eq(test_command_payload(db_schemas_descriptor(None, None)), b"")
  assert_eq(
    test_command_payload(db_schemas_descriptor(Some(""), None)),
    b"\x0a\x00",
  )
  assert_eq(
    test_command_payload(db_schemas_descriptor(None, Some(""))),
    b"\x12\x00",
  )
  assert_eq(
    test_command_payload(
      tables_descriptor(None, None, None, ["TABLE", "VIEW"], true),
    ),
    b"\x22\x05TABLE\x22\x04VIEW\x28\x01",
  )
  assert_eq(
    test_command_payload(tables_descriptor(Some(""), None, Some(""), [], false)),
    b"\x0a\x00\x1a\x00",
  )
}

///|
test "metadata discovery requires schema even with zero endpoints" {
  let result : DescriptorResult = { schema: None, parts: [], }
  let rejected = try {
    check_metadata_schema(result, [("name", @arrow.DataType::Utf8, false)])
    false
  } catch {
    FlightError::Protocol("missing Flight SQL metadata schema") => true
    _ => false
  }
  assert_true(rejected)
}
