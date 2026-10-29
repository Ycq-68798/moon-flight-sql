///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let sql = "SELECT id, name FROM items ORDER BY id"
  let baseline = @flight.Client::connect("127.0.0.1", 8815)
  defer baseline.close()
  let full = baseline.query(sql)
  guard full.length() == 1 && full[0].batches.length() == 100 else {
    fail("expected 100 small batches under the default limit")
  }
  let client = @flight.Client::connect("127.0.0.1", 8815, max_query_bytes=1024)
  defer client.close()
  let rejected = try {
    let _ = client.query(sql)
    false
  } catch {
    @flight.FlightError::Rpc(
      8,
      "query responses exceed the configured byte limit"
    ) => true
    _ => false
  }
  guard rejected else { fail("expected response byte limit") }
  let closed = try {
    let _ = client.query(sql)
    false
  } catch {
    @flight.FlightError::Protocol("connection is closed") => true
    _ => false
  }
  guard closed else { fail("over-limit connection must be closed") }
  println("PASS: aggregate gRPC response limit rejected and closed connection")
}
