///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let names = client.catalogs()
  guard names is ["main", "warehouse"] else { fail("wrong catalog names") }
  let rows = client.query("SELECT id, name FROM items ORDER BY id")
  guard rows.length() == 1 && rows[0].batches.length() == 2 else {
    fail("expected query batches after metadata")
  }
  println("PASS: catalogs metadata and subsequent SQL query")
}
