///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let sql = "SELECT id, name FROM items ORDER BY id"
  let timed_out = try {
    let _ = client.query(sql, timeout_ms=100)
    false
  } catch {
    @flight.FlightError::Rpc(4, _) => true
    _ => false
  }
  guard timed_out else { fail("expected deadline exceeded") }
  let closed = try {
    let _ = client.query(sql)
    false
  } catch {
    @flight.FlightError::Protocol("connection is closed") => true
    _ => false
  }
  guard closed else { fail("timed-out connection must be closed") }
  let another = @flight.Client::connect("127.0.0.1", 8815)
  defer another.close()
  @async.with_task_group() <| group => {
    group.spawn_bg(() => {
      let result = another.query(sql, timeout_ms=3000)
      guard result.length() == 1 else { fail("expected complete result") }
    })
    @async.sleep(100)
    let rejected = try {
      let _ = another.query(sql)
      false
    } catch {
      @flight.FlightError::Protocol("concurrent queries are not supported") =>
        true
      _ => false
    }
    guard rejected else { fail("concurrent query must be rejected") }
  }
  println("PASS: timeout closed connection; concurrent query rejected")
}
