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
  println("PASS: repeated headers and oversized header frame rejected")
}
