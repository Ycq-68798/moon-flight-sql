///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let rejected = try {
    let result = client.query("SELECT id, name FROM items ORDER BY id")
    println("Unexpectedly accepted \{result.length()} endpoint(s)")
    false
  } catch {
    @flight.FlightError::Rpc(8, message) =>
      message == "gRPC message exceeds the 4 MiB receive limit"
    _ => false
  }
  guard rejected else { fail("oversized batch was silently accepted") }
  let closed = try {
    let _ = client.query("SELECT id, name FROM items ORDER BY id")
    false
  } catch {
    @flight.FlightError::Protocol("connection is closed") => true
    _ => false
  }
  guard closed else { fail("oversized call must close the connection") }
  println("PASS: oversized batch rejected and connection closed")
}
