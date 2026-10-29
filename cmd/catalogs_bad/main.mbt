///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let rejected = try {
    let _ = client.catalogs()
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL catalog schema") => true
    _ => false
  }
  guard rejected else { fail("expected invalid catalog schema to be rejected") }
  println("PASS: invalid catalog schema rejected")
}
