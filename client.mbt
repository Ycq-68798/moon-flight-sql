///|
/// A single, unencrypted h2c Flight SQL connection. Use sequentially.
pub struct Client {
  channel : @net.Channel
}

///|
pub async fn Client::connect(host : String, port : Int32) -> Client {
  { channel: @net.Channel::connect(host, port), }
}

///|
pub fn Client::close(self : Client) -> Unit {
  self.channel.close()
}

///|
/// An ASCII gRPC request metadata entry. Names must be lowercase and values
/// printable ASCII; binary metadata and reserved protocol headers are rejected.
pub(all) struct Metadata {
  name : String
  value : String
}

///|
fn encode_metadata(
  entries : Array[Metadata],
) -> Array[@header.Header] raise FlightError {
  let encoded = []
  for entry in entries {
    if entry.name.is_empty() ||
      entry.name.has_prefix("grpc-") ||
      entry.name == "content-type" ||
      entry.name == "te" ||
      entry.name == "user-agent" {
      raise FlightError::Protocol("invalid or reserved metadata name")
    }
    for c in entry.name {
      if !(c is ('a'..<='z') ||
        c is ('0'..<='9') ||
        c is '-' ||
        c is '_' ||
        c is '.') {
        raise FlightError::Protocol("metadata names must use lowercase ASCII")
      }
    }
    for c in entry.value {
      if !(c is (' '..<='~')) {
        raise FlightError::Protocol("metadata values must use printable ASCII")
      }
    }
    if entry.name.has_suffix("-bin") {
      raise FlightError::Protocol("binary metadata is not supported")
    }
    encoded.push(@header.Header::of(entry.name, entry.value))
  }
  encoded
}

///|
/// Execute a statement and retrieve all Flight endpoints. Results are buffered
/// in memory by the underlying transport before Arrow batches are decoded.
pub async fn Client::query(
  self : Client,
  sql : String,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
  allow_unknown_count~ : Bool = false,
) -> Array[@arrow.IpcData] {
  let headers = encode_metadata(metadata)
  let info = self.channel.unary(
    "/arrow.flight.protocol.FlightService/GetFlightInfo",
    statement_descriptor(sql),
    timeout_millis=timeout_ms,
    metadata=headers,
  )
  if info.grpc_status != 0 {
    raise FlightError::Rpc(info.grpc_status, info.grpc_message)
  }
  if info.messages.length() != 1 {
    raise FlightError::Protocol("GetFlightInfo must return one message")
  }
  let expected = total_records(info.messages[0])
  if expected == -1L && !allow_unknown_count {
    raise FlightError::Protocol(
      "server did not provide total_records; result integrity cannot be checked",
    )
  }
  let endpoints = decode_endpoints(info.messages[0])
  let result = []
  let mut actual = 0L
  for endpoint in endpoints {
    if !endpoint.locations.is_empty() &&
      !endpoint.locations.contains("arrow-flight-reuse-connection://?") &&
      !endpoint.locations.contains("") {
      raise FlightError::Protocol("endpoint requires a different location")
    }
    let reply = self.channel.server_streaming(
      "/arrow.flight.protocol.FlightService/DoGet",
      endpoint.ticket,
      timeout_millis=timeout_ms,
      metadata=headers,
    )
    if reply.grpc_status != 0 {
      raise FlightError::Rpc(reply.grpc_status, reply.grpc_message)
    }
    let data = @arrow.read_stream(flight_data_to_ipc(reply.messages))
    for batch in data.batches {
      if actual > 9223372036854775807L - batch.num_rows.to_int64() {
        raise FlightError::Protocol("result row count overflow")
      }
      actual += batch.num_rows.to_int64()
    }
    result.push(data)
  }
  if expected >= 0L && expected != actual {
    raise FlightError::Protocol(
      "result row count differs from FlightInfo.total_records",
    )
  }
  result
}
