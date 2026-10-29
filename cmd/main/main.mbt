///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  println("Connecting")
  let client = @flight.Client::connect("127.0.0.1", 8815)
  println("Querying")
  defer client.close()
  let results = client.query("SELECT id, name FROM items ORDER BY id")
  guard results.length() == 1 else { fail("expected one endpoint") }
  let batches = results[0].batches
  guard batches.length() == 2 else { fail("expected two batches") }
  let mut rows = 0
  for batch in batches {
    for i in 0..<batch.num_rows {
      println("\{Repr(batch.get_row(i))}")
      rows += 1
    }
  }
  guard rows == 4 else { fail("expected four rows") }
  guard batches[0].get_row(0)
    is [Some(@arrow.Int64Value(1)), Some(@arrow.Utf8Value("alpha"))] else {
    fail("wrong first row")
  }
  guard batches[0].get_row(1) is [Some(@arrow.Int64Value(2)), None] else {
    fail("wrong null row")
  }
  guard batches[1].get_row(0)
    is [Some(@arrow.Int64Value(3)), Some(@arrow.Utf8Value("gamma"))] else {
    fail("wrong third row")
  }
  guard batches[1].get_row(1)
    is [Some(@arrow.Int64Value(4)), Some(@arrow.Utf8Value("delta"))] else {
    fail("wrong fourth row")
  }
  let bad_query_failed = try {
    let _ = client.query("THIS IS INVALID")
    false
  } catch {
    @flight.FlightError::Rpc(_, _) => true
    _ => false
  }
  guard bad_query_failed else { fail("expected a gRPC error for invalid SQL") }
  println("Invalid SQL returned a gRPC error")
}
