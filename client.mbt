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
/// Execute a statement and retrieve all Flight endpoints. Results are buffered
/// in memory by the underlying transport before Arrow batches are decoded.
pub async fn Client::query(
  self : Client,
  sql : String,
  timeout_ms~ : Int32 = 30000,
) -> Array[@arrow.IpcData] {
  let info = self.channel.unary(
    "/arrow.flight.protocol.FlightService/GetFlightInfo",
    statement_descriptor(sql),
    timeout_millis=timeout_ms,
  )
  if info.grpc_status != 0 {
    raise FlightError::Rpc(info.grpc_status, info.grpc_message)
  }
  if info.messages.length() != 1 {
    raise FlightError::Protocol("GetFlightInfo must return one message")
  }
  let endpoints = decode_endpoints(info.messages[0])
  let result = []
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
    )
    if reply.grpc_status != 0 {
      raise FlightError::Rpc(reply.grpc_status, reply.grpc_message)
    }
    result.push(@arrow.read_stream(flight_data_to_ipc(reply.messages)))
  }
  result
}
