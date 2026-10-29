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
  let validator = MessageGuard::new(QueryBudget::new(67108864))
  validator.push(b"\x00\x00")
  validator.push(b"\x00\x00\x03ab")
  validator.push(b"c\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01z")
  validator.finish()
}

///|
test "gRPC message guard rejects oversized and truncated frames" {
  let oversized = MessageGuard::new(QueryBudget::new(67108864))
  oversized.push(b"\x00\x00\x40")
  let rejected = try {
    oversized.push(b"\x00\x01")
    false
  } catch {
    FlightError::Rpc(8, _) => true
    _ => false
  }
  assert_true(rejected)
  let partial = MessageGuard::new(QueryBudget::new(67108864))
  partial.push(b"\x00\x00\x00\x00\x02x")
  let truncated = try {
    partial.finish()
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(truncated)
  let partial_header = MessageGuard::new(QueryBudget::new(67108864))
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
  let compressed = MessageGuard::new(QueryBudget::new(67108864))
  let rejected = try {
    compressed.push(b"\x01\x00\x00\x00\x00")
    false
  } catch {
    FlightError::Protocol(_) => true
    _ => false
  }
  assert_true(rejected)
  let boundary = MessageGuard::new(QueryBudget::new(67108864))
  boundary.push(b"\x00\x00\x40\x00\x00")
  assert_eq(boundary.remaining, 4194304)
}

///|
test "gRPC response limit counts all message prefixes and bodies" {
  let budget = QueryBudget::new(12)
  let validator = MessageGuard::new(budget)
  validator.push(b"\x00\x00\x00\x00\x01a")
  validator.finish()
  let next_rpc = MessageGuard::new(budget)
  next_rpc.push(b"\x00\x00\x00\x00\x01b")
  next_rpc.finish()
  let rejected = try {
    next_rpc.push(b"\x00")
    false
  } catch {
    FlightError::Rpc(8, "query responses exceed the configured byte limit") =>
      true
    _ => false
  }
  assert_true(rejected)
}
