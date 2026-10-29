///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let sql = "SELECT id, name FROM items ORDER BY id"
  let baseline = @flight.Client::connect("127.0.0.1", 8815)
  defer baseline.close()
  let all = baseline.query(sql + ";")
  guard all.length() == 2 &&
    all[0].batches.length() == 2 &&
    all[1].batches.length() == 2 else {
    fail("expected both endpoints")
  }
  let limited = @flight.Client::connect("127.0.0.1", 8815, max_query_bytes=1600)
  defer limited.close()
  let one = limited.query(sql)
  guard one.length() == 1 && one[0].batches.length() == 2 else {
    fail("single endpoint should fit the budget")
  }
  let rejected = try {
    let _ = limited.query(sql + ";")
    false
  } catch {
    @flight.FlightError::Rpc(
      8,
      "query responses exceed the configured byte limit"
    ) => true
    _ => false
  }
  guard rejected else {
    fail("multiple endpoints should exceed the query budget")
  }
  println("PASS: query budget spans multiple endpoints")
}
