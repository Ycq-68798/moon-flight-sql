///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let sql = "SELECT id, name FROM items ORDER BY id"
  let requests : Array[Array[@flight.Metadata]] = [
    [],
    [{ name: "authorization", value: "Bearer wrong", }],
  ]
  for metadata in requests {
    let rejected = try {
      let _ = client.query(sql, metadata~)
      false
    } catch {
      @flight.FlightError::Rpc(16, _) => true
      _ => false
    }
    guard rejected else { fail("expected unauthenticated status 16") }
  }
  let results = client.query(sql, metadata=[
    { name: "authorization", value: "Bearer demo-token", },
  ])
  guard results.length() == 1 && results[0].batches.length() == 2 else {
    fail("expected two authenticated record batches")
  }
  guard results[0].batches[0].get_row(1)[1] is None else {
    fail("expected null value")
  }
  println(
    "PASS: missing and wrong token rejected; authenticated query returned two batches",
  )
}
