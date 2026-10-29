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
    @flight.FlightError::Protocol(message) =>
      message == "result row count differs from FlightInfo.total_records"
    _ => false
  }
  guard rejected else { fail("oversized batch was silently accepted") }
  println("PASS: oversized batch rejected by row-count check")
}
