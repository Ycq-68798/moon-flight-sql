///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let sql = "SELECT id, name FROM items ORDER BY id"
  let result = client.query(sql)
  guard result.length() == 1 && result[0].batches.length() == 2 else {
    fail("expected two batches with unknown row count")
  }
  guard result[0].batches[0].get_row(1)[1] is None else {
    fail("expected null value")
  }
  guard result[0].batches[0].get_row(0)
    is [Some(@arrow.Int64Value(1)), Some(@arrow.Utf8Value("alpha"))] else {
    fail("wrong first row")
  }
  guard result[0].batches[1].get_row(0)
    is [Some(@arrow.Int64Value(3)), Some(@arrow.Utf8Value("gamma"))] else {
    fail("wrong third row")
  }
  guard result[0].batches[1].get_row(1)
    is [Some(@arrow.Int64Value(4)), Some(@arrow.Utf8Value("delta"))] else {
    fail("wrong fourth row")
  }
  println("PASS: query with unknown row count returned both batches")
}
