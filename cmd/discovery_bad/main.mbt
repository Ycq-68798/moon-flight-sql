///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let bad_schema = try {
    let _ = client.db_schemas()
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL metadata schema") => true
    _ => false
  }
  guard bad_schema else { fail("invalid schema list schema accepted") }
  let bad_tables = try {
    let _ = client.tables()
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL metadata schema") => true
    _ => false
  }
  guard bad_tables else { fail("invalid table list schema accepted") }
  println("PASS: invalid metadata schemas rejected")
}
