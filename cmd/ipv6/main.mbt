///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("::1", 8816)
  defer client.close()
  let result = client.query("SELECT id, name FROM items ORDER BY id")
  guard result.length() == 1 && result[0].batches.length() == 2 else {
    fail("IPv6 query did not return both batches")
  }
  println("PASS: IPv6 loopback query")
}
