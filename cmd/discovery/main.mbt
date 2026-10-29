///|
fn main {
  @async.run_async_main(run)
}

///|
async fn run() -> Unit {
  let client = @flight.Client::connect("127.0.0.1", 8815)
  defer client.close()
  let schemas = client.db_schemas()
  guard schemas.length() == 3 &&
    schemas[0].catalog_name is None &&
    schemas[0].schema_name == "public" else {
    fail("wrong schema list")
  }
  let no_catalog = client.db_schemas(catalog="")
  guard no_catalog.length() == 1 && no_catalog[0].catalog_name is None else {
    fail("empty catalog presence was lost")
  }
  let filtered = client.db_schemas(catalog="main", pattern="a%")
  guard filtered.length() == 1 && filtered[0].schema_name == "analytics" else {
    fail("schema pattern filter failed")
  }
  guard client.db_schemas(pattern="p_blic").length() == 2 else {
    fail("schema underscore pattern failed")
  }
  guard client.db_schemas(catalog="missing").is_empty() else {
    fail("expected no schemas")
  }
  guard client.db_schemas(pattern="").is_empty() else {
    fail("empty schema pattern presence was lost")
  }
  let tables = client.tables()
  guard tables.length() == 4 &&
    tables[0].catalog_name is None &&
    tables[0].schema_name is None else {
    fail("wrong table list")
  }
  guard client.tables(catalog="").length() == 1 else {
    fail("table catalog presence was lost")
  }
  let views = client.tables(
    catalog="main",
    schema_pattern="pub%",
    table_pattern="order%",
    table_types=["VIEW"],
    include_schema=true,
  )
  guard views.length() == 1 &&
    views[0].table_name == "order_view" &&
    views[0].table_type == "VIEW" else {
    fail("table filter failed")
  }
  guard views[0].table_schema is Some(bytes) else {
    fail("missing serialized table schema")
  }
  let table_schema = @arrow.StreamReader::new(bytes).schema
  guard table_schema.fields.length() == 1 && table_schema.fields[0].name == "id" else {
    fail("wrong table schema")
  }
  guard client.tables(table_types=["TABLE", "VIEW"]).length() == 4 else {
    fail("repeated table type fields failed")
  }
  guard client.tables(table_pattern="no-match%").is_empty() else {
    fail("expected no tables")
  }
  guard client.tables(table_pattern="").is_empty() else {
    fail("empty table pattern presence was lost")
  }
  println("PASS: schema and table discovery, presence, filters and schema IPC")
}
