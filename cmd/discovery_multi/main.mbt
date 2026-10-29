///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  guard client.table_types().length() == 4 &&
    client.primary_keys("orders").length() == 4 else {
    fail("metadata endpoints were not all read")
  }
  println("PASS: table types and primary keys from two endpoints")
}
