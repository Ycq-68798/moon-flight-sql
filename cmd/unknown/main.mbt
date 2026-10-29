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
  let results = client.query(sql, allow_unknown_count=true)
  guard results.length() == 1 && results[0].batches.length() == 2 else {
    fail("expected two batches with explicit opt-in")
  }
  println("PASS: unknown row count rejected by default and accepted on opt-in")
}
