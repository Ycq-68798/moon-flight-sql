///|
/// A bounded guard over gRPC DATA, independent of DATA-frame boundaries.
/// The MoonRPC decoder caps wire messages at 4 MiB; detecting the prefix first
/// prevents its oversized-message status from being overwritten by trailers.
priv struct MessageGuard {
  prefix : Array[Byte]
  mut remaining : Int32
}

///|
priv struct QueryBudget {
  mut received : Int32
  limit : Int32
}

///|
fn QueryBudget::new(limit : Int32) -> QueryBudget {
  { received: 0, limit, }
}

///|
fn QueryBudget::charge(
  self : QueryBudget,
  count : Int32,
) -> Unit raise FlightError {
  if count < 0 || count > self.limit - self.received {
    raise FlightError::Rpc(
      8, "query responses exceed the configured byte limit",
    )
  }
  self.received += count
}

///|
fn MessageGuard::new() -> MessageGuard {
  { prefix: [], remaining: 0, }
}

///|
fn MessageGuard::push(
  self : MessageGuard,
  data : Bytes,
) -> Unit raise FlightError {
  let mut at = 0
  while at < data.length() {
    if self.remaining > 0 {
      let count = if self.remaining < data.length() - at {
        self.remaining
      } else {
        data.length() - at
      }
      self.remaining -= count
      at += count
    } else {
      self.prefix.push(data[at])
      at += 1
      if self.prefix.length() == 5 {
        if self.prefix[0] != 0 {
          raise FlightError::Protocol(
            "compressed gRPC messages are unsupported",
          )
        }
        let length = (self.prefix[1].to_int64() << 24) |
          (self.prefix[2].to_int64() << 16) |
          (self.prefix[3].to_int64() << 8) |
          self.prefix[4].to_int64()
        if length > 4194304L {
          raise FlightError::Rpc(
            8, "gRPC message exceeds the 4 MiB receive limit",
          )
        }
        self.remaining = length.to_int()
        self.prefix.clear()
      }
    }
  }
}

///|
fn MessageGuard::finish(self : MessageGuard) -> Unit raise FlightError {
  if !self.prefix.is_empty() || self.remaining != 0 {
    raise FlightError::Protocol("truncated gRPC message")
  }
}

///|
/// Sequential h2c adapter using MoonRPC's HTTP/2 engine, with a pre-decoder
/// message guard. Transport failures and local timeouts invalidate the connection.
struct SafeChannel {
  conn : @socket.Tcp
  engine : @rpcclient.H2Client
  response_decoder : @hpack.Decoder
  authority : String
  mut closed : Bool
  max_query_bytes : Int32
}

///|
async fn SafeChannel::connect(
  host : String,
  port : Int32,
  max_query_bytes~ : Int32,
) -> SafeChannel {
  if max_query_bytes <= 0 {
    raise FlightError::Protocol("max_query_bytes must be positive")
  }
  if host.is_empty() || port <= 0 || port > 65535 {
    raise FlightError::Protocol("host and port must identify a valid endpoint")
  }
  for c in host {
    if !(c is ('a'..<='z') ||
      c is ('A'..<='Z') ||
      c is ('0'..<='9') ||
      c is '.' ||
      c is '-' ||
      c is '_' ||
      c is ':') {
      raise FlightError::Protocol(
        "host must be a hostname or unbracketed IP address",
      )
    }
  }
  let authority = make_authority(host, port)
  let conn = @socket.Tcp::connect_to_host(host, port~)
  errdefer conn.close()
  let engine = @rpcclient.H2Client::new()
  conn.write(@http2.preface)
  for frame in engine.preface() {
    conn.write(frame.encode())
  }
  {
    conn,
    engine,
    response_decoder: @hpack.Decoder::new(),
    authority,
    closed: false,
    max_query_bytes,
  }
}

///|
/// Permit a single response stream while accepting HTTP/2 connection control
/// frames. Header continuations cannot be interleaved with any other frame.
priv struct FrameGuard {
  id : Int32
  decoder : @hpack.Decoder
  header_block : @buffer.Buffer
  mut in_headers : Bool
  mut headers_seen : Bool
  mut initial_headers : Bool
  mut terminal_headers : Bool
}

///|
fn FrameGuard::new(
  id : Int32,
  decoder~ : @hpack.Decoder = @hpack.Decoder::new(),
) -> FrameGuard {
  {
    id,
    decoder,
    header_block: @buffer.Buffer(),
    in_headers: false,
    headers_seen: false,
    initial_headers: false,
    terminal_headers: false,
  }
}

///|
/// Validate the gRPC status before the underlying decoder parses it as an Int.
fn FrameGuard::finish_headers(self : FrameGuard) -> Unit raise FlightError {
  let headers = self.decoder.decode(
    self.header_block.to_bytes()[:],
    limit=131072,
  ) catch {
    _ => raise FlightError::Protocol("invalid HTTP/2 header block")
  }
  self.header_block.reset()
  let mut count = 0
  let mut http_status_count = 0
  for header in headers {
    if !self.initial_headers &&
      !header.name.is_empty() &&
      header.name[0] == b':' {
      raise FlightError::Protocol("pseudo-header in HTTP/2 trailers")
    }
    if header.name == b":status" {
      http_status_count += 1
      if !self.initial_headers || http_status_count > 1 {
        raise FlightError::Protocol("invalid HTTP status placement or count")
      }
      if header.value.length() != 3 ||
        header.value[0] < b'1' ||
        header.value[0] > b'5' ||
        header.value[1] < b'0' ||
        header.value[1] > b'9' ||
        header.value[2] < b'0' ||
        header.value[2] > b'9' {
        raise FlightError::Protocol("invalid HTTP status")
      }
    }
    if header.name == b"grpc-status" {
      count += 1
      if !self.terminal_headers || count > 1 || header.value.is_empty() {
        raise FlightError::Protocol("invalid gRPC status placement or count")
      }
      if header.value.length() > 1 && header.value[0] == b'0' {
        raise FlightError::Protocol("invalid gRPC status")
      }
      // Cap the decimal representation to avoid integer overflow in MoonRPC.
      if header.value.length() > 9 {
        raise FlightError::Protocol("invalid gRPC status")
      }
      for byte in header.value {
        if byte < b'0' || byte > b'9' {
          raise FlightError::Protocol("invalid gRPC status")
        }
      }
    }
  }
  if self.initial_headers && http_status_count != 1 {
    raise FlightError::Protocol("missing HTTP status")
  }
}

///|
fn FrameGuard::check(
  self : FrameGuard,
  frame : @http2.Frame,
) -> Unit raise FlightError {
  if self.in_headers {
    match frame {
      @http2.Frame::Continuation(stream_id~, end_headers~, fragment~) if stream_id ==
        self.id => {
        self.header_block.write_bytes(fragment)
        if self.header_block.length() > 131072 {
          raise FlightError::Protocol("HTTP/2 header block exceeds limit")
        }
        if end_headers {
          self.finish_headers()
        }
        self.in_headers = !end_headers
        return
      }
      _ => raise FlightError::Protocol("interleaved HTTP/2 header block")
    }
  }
  match frame {
    @http2.Frame::Headers(stream_id~, end_headers~, end_stream~, fragment~, ..) => {
      if stream_id != self.id {
        raise FlightError::Protocol("unexpected HTTP/2 response stream")
      }
      if self.headers_seen && !end_stream {
        raise FlightError::Protocol("response trailers must end the stream")
      }
      self.initial_headers = !self.headers_seen
      self.headers_seen = true
      self.in_headers = !end_headers
      self.terminal_headers = end_stream
      self.header_block.write_bytes(fragment)
      if self.header_block.length() > 131072 {
        raise FlightError::Protocol("HTTP/2 header block exceeds limit")
      }
      if end_headers {
        self.finish_headers()
      }
    }
    @http2.Frame::Data(stream_id~, ..) => {
      if stream_id != self.id {
        raise FlightError::Protocol("unexpected HTTP/2 response stream")
      }
      if !self.headers_seen {
        raise FlightError::Protocol("HTTP/2 data before response headers")
      }
    }
    @http2.Frame::Continuation(..) =>
      raise FlightError::Protocol("unexpected HTTP/2 continuation")
    @http2.Frame::PushPromise(..) =>
      raise FlightError::Protocol("HTTP/2 server push is disabled")
    _ => ()
  }
}

///|
/// HTTP/2 authority includes the port and brackets an IPv6 literal.
fn make_authority(host : String, port : Int32) -> String {
  if host.contains(":") {
    "[" + host + "]:" + port.to_string()
  } else {
    host + ":" + port.to_string()
  }
}

///|
fn SafeChannel::close(self : SafeChannel) -> Unit {
  if !self.closed {
    self.closed = true
    self.conn.close()
  }
}

///|
async fn SafeChannel::pump(
  self : SafeChannel,
  id : Int32,
  budget : QueryBudget,
) -> @rpcclient.CallReply {
  let validator = MessageGuard::new()
  let frame_guard = FrameGuard::new(id, decoder=self.response_decoder)
  while !self.engine.is_done(id) {
    let head = self.conn.read_exactly(9)
    let hdr = @http2.FrameHeader::decode(head)
    // We advertise the HTTP/2 default SETTINGS_MAX_FRAME_SIZE (16 KiB).
    if hdr.length > 16384 {
      raise FlightError::Protocol("HTTP/2 frame exceeds advertised limit")
    }
    // Count the frame header and payload before reading or buffering the payload.
    budget.charge(9 + hdr.length)
    let payload = self.conn.read_exactly(hdr.length)
    let buf = @buffer.Buffer()
    buf.write_bytes(head)
    buf.write_bytes(payload)
    let (frame, _) = @http2.decode(buf.to_bytes())
    frame_guard.check(frame)
    if frame is @http2.Frame::Data(stream_id~, data~, ..) && stream_id == id {
      validator.push(data)
    }
    for out in self.engine.feed(frame) {
      self.conn.write(out.encode())
    }
  }
  let reply = self.engine.reply(id)
  validate_success_status(reply.status, reply.grpc_status)
  if reply.grpc_status == 0 {
    validator.finish()
  }
  reply
}

///|
fn validate_success_status(
  status : Bytes,
  grpc_status : Int32,
) -> Unit raise FlightError {
  if grpc_status == -1 {
    raise FlightError::Rpc(2, "missing gRPC status")
  }
  if grpc_status == 0 && status != b"200" {
    raise FlightError::Rpc(2, "unexpected HTTP response status")
  }
}

///|
async fn SafeChannel::call(
  self : SafeChannel,
  path : String,
  request : Bytes,
  metadata~ : Array[@header.Header],
  timeout_millis~ : Int32,
  budget~ : QueryBudget,
) -> @rpcclient.CallReply {
  if self.closed {
    raise FlightError::Protocol("connection is closed")
  }
  if timeout_millis <= 0 {
    raise FlightError::Protocol("timeout must be positive")
  }
  let (id, frames) = self.engine.unary(
    path,
    request,
    metadata~,
    timeout_millis~,
    authority=self.authority,
  )
  if id == 0 {
    raise FlightError::Rpc(14, "connection is draining")
  }
  defer self.engine.release(id)
  errdefer self.close()
  match
    (
      @async.with_timeout_opt(timeout_millis, () => {
        for frame in frames {
          self.conn.write(frame.encode())
        }
        self.pump(id, budget)
      }) : @rpcclient.CallReply?) {
    Some(reply) => reply
    None => raise FlightError::Rpc(4, "deadline exceeded")
  }
}
