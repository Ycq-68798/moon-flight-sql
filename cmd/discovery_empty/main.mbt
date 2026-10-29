///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  guard client.db_schemas().is_empty() &&
    client.tables(include_schema=true).is_empty() else {
    fail("expected no endpoints")
  }
  println("PASS: empty schema and table metadata with no endpoints")
}
