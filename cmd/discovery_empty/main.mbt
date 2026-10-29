///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  guard client.db_schemas().is_empty() &&
    client.tables(include_schema=true).is_empty() &&
    client.table_types().is_empty() &&
    client.primary_keys("orders").is_empty() &&
    client.imported_keys("orders").is_empty() &&
    client.exported_keys("customers").is_empty() &&
    client.cross_reference("customers", "orders").is_empty() else {
    fail("expected no endpoints")
  }
  println("PASS: empty schema and table metadata with no endpoints")
}
