///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let first = @flight.Client::connect("127.0.0.1", 8817)
  defer first.close()
  let repeated = try {
    let _ = first.query("SELECT 1")
    false
  } catch {
    @flight.FlightError::Protocol("response trailers must end the stream") =>
      true
    _ => false
  }
  guard repeated else { fail("repeated response headers were accepted") }
  let second = @flight.Client::connect("127.0.0.1", 8818, max_query_bytes=64)
  defer second.close()
  let limited = try {
    let _ = second.query("SELECT 1")
    false
  } catch {
    @flight.FlightError::Rpc(
      8,
      "query responses exceed the configured byte limit"
    ) => true
    _ => false
  }
  guard limited else { fail("response headers bypassed query budget") }
  for port in [8819, 8820, 8821, 8822] {
    let client = @flight.Client::connect("127.0.0.1", port)
    defer client.close()
    let rejected = try {
      let _ = client.query("SELECT 1")
      false
    } catch {
      @flight.FlightError::Protocol(_) => port == 8819 || port == 8822
      @flight.FlightError::Rpc(2, _) => port == 8820 || port == 8821
      _ => false
    }
    guard rejected else { fail("invalid response status was accepted") }
  }
  println("PASS: malformed and oversized response headers rejected")
}
