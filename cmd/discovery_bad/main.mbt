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
  let bad_types = try {
    let _ = client.table_types()
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL metadata schema") => true
    _ => false
  }
  let bad_keys = try {
    let _ = client.primary_keys("orders")
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL metadata schema") => true
    _ => false
  }
  guard bad_types && bad_keys else {
    fail("invalid types or keys schema accepted")
  }
  let bad_foreign = try {
    let _ = client.imported_keys("orders")
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL metadata schema") => true
    _ => false
  }
  guard bad_foreign else { fail("invalid foreign key schema accepted") }
  let bad_cross = try {
    let _ = client.cross_reference("customers", "orders")
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL metadata schema") => true
    _ => false
  }
  guard bad_cross else { fail("invalid cross reference schema accepted") }
  let bad_xdbc = try {
    let _ = client.xdbc_type_info()
    false
  } catch {
    @flight.FlightError::Protocol("invalid Flight SQL metadata schema") => true
    _ => false
  }
  guard bad_xdbc else { fail("invalid XDBC schema accepted") }
  println("PASS: invalid metadata schemas rejected")
}
