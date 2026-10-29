///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let rejected = try {
    let _ = client.query("SELECT id, name FROM items ORDER BY id")
    false
  } catch {
    @flight.FlightError::Protocol(
      "Flight endpoint schema differs from FlightInfo or other endpoints"
    ) => true
    _ => false
  }
  guard rejected else { fail("heterogeneous endpoint schemas were accepted") }
  println("PASS: mismatching endpoint schema rejected")
}
