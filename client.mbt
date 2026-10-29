///|
/// A single, unencrypted h2c Flight SQL connection. Use sequentially.
pub struct Client {
  channel : SafeChannel
  mut busy : Bool
}

///|
pub async fn Client::connect(
  host : String,
  port : Int32,
  max_query_bytes~ : Int32 = 67108864,
) -> Client {
  { channel: SafeChannel::connect(host, port, max_query_bytes~), busy: false, }
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
) -> Array[@arrow.IpcData] {
  self.execute_descriptor(statement_descriptor(sql), timeout_ms~, metadata~).parts
}

///|
/// List catalog names using the standard Flight SQL CommandGetCatalogs.
/// The returned schema and non-null UTF-8 values are checked before return.
pub async fn Client::catalogs(
  self : Client,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[String] {
  let result = self.execute_descriptor(
    catalogs_descriptor(),
    timeout_ms~,
    metadata~,
  )
  catalog_names(result)
}

///|
priv struct DescriptorResult {
  schema : @arrow.Schema?
  parts : Array[@arrow.IpcData]
}

///|
fn catalog_names(result : DescriptorResult) -> Array[String] raise {
  let schema = match result.schema {
    Some(schema) => schema
    None => raise FlightError::Protocol("missing Flight SQL catalog schema")
  }
  let fields = schema.fields
  if fields.length() != 1 ||
    fields[0].name != "catalog_name" ||
    fields[0].data_type != @arrow.DataType::Utf8 ||
    fields[0].nullable {
    raise FlightError::Protocol("invalid Flight SQL catalog schema")
  }
  let names = []
  for part in result.parts {
    for batch in part.batches {
      for i in 0..<batch.num_rows {
        match batch.get_row(i) {
          [Some(@arrow.Value::Utf8Value(name))] => names.push(name)
          _ => raise FlightError::Protocol("invalid Flight SQL catalog value")
        }
      }
    }
  }
  names
}

///|
async fn Client::execute_descriptor(
  self : Client,
  descriptor : Bytes,
  timeout_ms~ : Int32,
  metadata~ : Array[Metadata],
) -> DescriptorResult {
  if self.busy {
    raise FlightError::Protocol("concurrent queries are not supported")
  }
  self.busy = true
  defer {
    self.busy = false
  }
  let headers = encode_metadata(metadata)
  let budget = QueryBudget::new(self.channel.max_query_bytes)
  let info = self.channel.call(
    "/arrow.flight.protocol.FlightService/GetFlightInfo",
    descriptor,
    timeout_millis=timeout_ms,
    metadata=headers,
    budget~,
  )
  if info.grpc_status != 0 {
    raise FlightError::Rpc(info.grpc_status, info.grpc_message)
  }
  if info.messages.length() != 1 {
    raise FlightError::Protocol("GetFlightInfo must return one message")
  }
  let expected = total_records(info.messages[0])
  let endpoints = decode_endpoints(info.messages[0])
  let mut expected_schema : @arrow.Schema? = match
    flight_info_schema(info.messages[0]) {
    Some(bytes) => Some(@arrow.StreamReader::new(bytes).schema)
    None => None
  }
  let result = []
  let mut actual = 0L
  for endpoint in endpoints {
    if !endpoint.locations.is_empty() &&
      !endpoint.locations.contains("arrow-flight-reuse-connection://?") &&
      !endpoint.locations.contains("") {
      raise FlightError::Protocol("endpoint requires a different location")
    }
    let reply = self.channel.call(
      "/arrow.flight.protocol.FlightService/DoGet",
      endpoint.ticket,
      timeout_millis=timeout_ms,
      metadata=headers,
      budget~,
    )
    if reply.grpc_status != 0 {
      raise FlightError::Rpc(reply.grpc_status, reply.grpc_message)
    }
    let data = @arrow.read_stream(flight_data_to_ipc(reply.messages))
    match expected_schema {
      Some(schema) =>
        if !fields_match(schema.fields, data.schema.fields) {
          raise FlightError::Protocol(
            "Flight endpoint schema differs from FlightInfo or other endpoints",
          )
        }
      None => expected_schema = Some(data.schema)
    }
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
  { schema: expected_schema, parts: result, }
}
