///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let rejected = try {
    let _ = client.xdbc_type_info()
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL metadata schema") => true
    _ => false
  }
  guard rejected else { fail("nullable XDBC list child accepted") }
  println("PASS: nullable XDBC list child rejected")
}
