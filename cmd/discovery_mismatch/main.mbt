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
  let keys_rejected = try {
    let _ = client.primary_keys("orders")
    false
  } catch {
    @flight.FlightError::Protocol(
      "Flight endpoint schema differs from FlightInfo or other endpoints"
    ) => true
    _ => false
  }
  let types_rejected = try {
    let _ = client.table_types()
    false
  } catch {
    @flight.FlightError::Protocol(
      "Flight endpoint schema differs from FlightInfo or other endpoints"
    ) => true
    _ => false
  }
  guard keys_rejected && types_rejected else {
    fail("inconsistent key or type schema accepted")
  }
  let foreign_rejected = try {
    let _ = client.exported_keys("customers")
    false
  } catch {
    @flight.FlightError::Protocol(
      "Flight endpoint schema differs from FlightInfo or other endpoints"
    ) => true
    _ => false
  }
  guard foreign_rejected else {
    fail("foreign key endpoint schema mismatch accepted")
  }
  println("PASS: metadata endpoint schema mismatch rejected")
}
