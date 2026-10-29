///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let result = client.query("SELECT id, name FROM items ORDER BY id")
  guard result.length() == 1 && result[0].batches.length() == 1 else {
    fail("expected one batch")
  }
  guard result[0].batches[0].get_row(0)
    is [Some(@arrow.Int64Value(1)), Some(@arrow.Utf8Value(value))] else {
    fail("expected a string row")
  }
  guard value.length() == 3 * 1024 * 1024 &&
    value[0] == 'x' &&
    value[value.length() - 1] == 'x' else {
    fail("unexpected string data")
  }
  for c in value {
    guard c == 'x' else { fail("corrupt string") }
  }
  println("PASS: received and checked all 3 MiB of data")
}
