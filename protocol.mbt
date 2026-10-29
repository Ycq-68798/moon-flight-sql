///|
pub(all) suberror FlightError {
  Protocol(String)
  Rpc(Int32, String)
} derive(Debug)

///|
pub extend FlightError with @debug.Debug::{to_repr}

///|
/// Encode the standard Flight SQL CommandStatementQuery in a FlightDescriptor.
pub fn statement_descriptor(sql : String) -> Bytes {
  let statement = @proto.PbWriter::new()
  statement.string_(1, sql)
  let any = @proto.PbWriter::new()
  any.string_(
    1, "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery",
  )
  any.bytes_(2, statement.to_bytes())
  let descriptor = @proto.PbWriter::new()
  descriptor.enum_(1, 2)
  descriptor.bytes_(2, any.to_bytes())
  descriptor.to_bytes()
}

///|
pub(all) struct Endpoint {
  ticket : Bytes
  locations : Array[String]
} derive(Debug)

///|
pub extend Endpoint with @debug.Debug::{to_repr}

///|
fn decode_location(data : Bytes) -> String raise {
  let r = @proto.PbReader::new(data)
  let mut uri = ""
  while !r.eof() {
    let (field, wire) = r.read_tag()
    if field == 1 && wire == LengthDelim {
      uri = r.read_string()
    } else {
      r.skip(wire)
    }
  }
  uri
}

///|
fn decode_endpoint(data : Bytes) -> Endpoint raise {
  let r = @proto.PbReader::new(data)
  let mut ticket : Bytes? = None
  let locations = []
  while !r.eof() {
    let (field, wire) = r.read_tag()
    if field == 1 && wire == LengthDelim {
      ticket = Some(r.read_bytes())
    } else if field == 2 && wire == LengthDelim {
      locations.push(decode_location(r.read_bytes()))
    } else {
      r.skip(wire)
    }
  }
  match ticket {
    Some(t) => { ticket: t, locations, }
    None => raise FlightError::Protocol("FlightEndpoint is missing its ticket")
  }
}

///|
/// Extract all endpoints from a FlightInfo protobuf message.
pub fn decode_endpoints(data : Bytes) -> Array[Endpoint] raise {
  let r = @proto.PbReader::new(data)
  let endpoints = []
  while !r.eof() {
    let (field, wire) = r.read_tag()
    if field == 3 && wire == LengthDelim {
      endpoints.push(decode_endpoint(r.read_bytes()))
    } else {
      r.skip(wire)
    }
  }
  endpoints
}

///|
/// Convert FlightData protobuf messages to an Arrow IPC stream. Metadata-only
/// messages are ignored; schema, dictionary and record-batch order is preserved.
pub fn flight_data_to_ipc(messages : Array[Bytes]) -> Bytes raise {
  let out : Array[Byte] = []
  for message in messages {
    let reader = @proto.PbReader::new(message)
    let mut header : Bytes = b""
    let mut body : Bytes = b""
    while !reader.eof() {
      let (field, wire) = reader.read_tag()
      if field == 2 && wire == LengthDelim {
        header = reader.read_bytes()
      } else if field == 1000 && wire == LengthDelim {
        body = reader.read_bytes()
      } else {
        reader.skip(wire)
      }
    }
    if !header.is_empty() {
      // IPC continuation token, followed by a little-endian metadata length.
      out.push(0xff)
      out.push(0xff)
      out.push(0xff)
      out.push(0xff)
      let n = header.length()
      let padded = (n + 7) / 8 * 8
      out.push((padded & 255).to_byte())
      out.push(((padded >> 8) & 255).to_byte())
      out.push(((padded >> 16) & 255).to_byte())
      out.push(((padded >> 24) & 255).to_byte())
      for b in header {
        out.push(b)
      }
      for _ in n..<padded {
        out.push(0)
      }
      for b in body {
        out.push(b)
      }
    } else if !body.is_empty() {
      raise FlightError::Protocol("FlightData body without header")
    }
  }
  Bytes::from_array(out)
}
