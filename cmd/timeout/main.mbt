///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8823)
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
  let server_deadline = @flight.Client::connect("127.0.0.1", 8824)
  defer server_deadline.close()
  let remote_deadline = try {
    let _ = server_deadline.query(sql, timeout_ms=3000)
    false
  } catch {
    @flight.FlightError::Rpc(4, _) => true
    _ => false
  }
  guard remote_deadline else { fail("expected server deadline status") }
  let reused = try {
    let _ = server_deadline.query(sql, timeout_ms=3000)
    false
  } catch {
    @flight.FlightError::Rpc(13, _) => true
    _ => false
  }
  guard reused else { fail("completed server deadline must allow reuse") }
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
  println(
    "PASS: local timeout closed connection; server deadline allowed reuse; concurrent query rejected",
  )
}
