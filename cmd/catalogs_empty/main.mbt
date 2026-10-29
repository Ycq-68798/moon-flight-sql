///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  guard client.catalogs().is_empty() else { fail("expected no catalogs") }
  println("PASS: empty catalog response")
}
