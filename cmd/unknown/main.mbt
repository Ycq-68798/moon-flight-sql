///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let sql = "SELECT id, name FROM items ORDER BY id"
  let rejected = try {
    let _ = client.query(sql)
    false
  } catch {
    @flight.FlightError::Protocol(message) =>
      message ==
      "server did not provide total_records; result integrity cannot be checked"
    _ => false
  }
  guard rejected else { fail("unknown row count was not rejected") }
  println("PASS: unknown row count rejected before result retrieval")
}
