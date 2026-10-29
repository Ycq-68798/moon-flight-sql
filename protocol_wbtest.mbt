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
