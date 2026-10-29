///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let sql = "SELECT id, name FROM items ORDER BY id"
  for (host, port) in [("localhost", 8815), ("127.0.0.1", 8815)] {
    let client = @flight.Client::connect(host, port)
    let result = client.query(sql)
    client.close()
    guard result.length() == 1 && result[0].batches.length() == 2 else {
      fail("address did not return both batches")
    }
  }
  for
    (host, port) in [
      ("", 8815),
      ("localhost", 0),
      ("localhost", 65536),
      ("[::1]", 8816),
      ("fe80::1%1", 8816),
      ("https://localhost", 8815),
    ] {
    let rejected = try {
      let client = @flight.Client::connect(host, port)
      client.close()
      false
    } catch {
      @flight.FlightError::Protocol(_) => true
      _ => false
    }
    guard rejected else { fail("invalid address was not rejected") }
  }
  println("PASS: DNS, IPv4 and invalid addresses")
}
