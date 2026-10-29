///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let rejected = try {
    let _ = client.tables()
    false
  } catch {
    @flight.FlightError::Protocol(
      "Flight endpoint schema differs from FlightInfo or other endpoints"
    ) => true
    _ => false
  }
  guard rejected else { fail("inconsistent endpoint schema accepted") }
  println("PASS: metadata endpoint schema mismatch rejected")
}
