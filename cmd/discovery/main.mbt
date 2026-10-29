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
  guard client.table_types() is ["TABLE", "VIEW"] else {
    fail("wrong table types")
  }
  let keys = client.primary_keys("orders")
  guard keys.length() == 2 &&
    keys[0].column_name == "order_id" &&
    keys[0].key_sequence == 1 &&
    keys[1].column_name == "region" &&
    keys[1].key_sequence == 2 &&
    keys[0].key_name == Some("orders_pk") else {
    fail("wrong primary keys")
  }
  let local_keys = client.primary_keys("local_tbl", catalog="", schema="")
  guard local_keys.length() == 1 &&
    local_keys[0].catalog_name is None &&
    local_keys[0].schema_name is None &&
    local_keys[0].key_name is None else {
    fail("empty primary key catalog or schema presence lost")
  }
  guard client.primary_keys("orders", catalog="", schema="").is_empty() &&
    client.primary_keys("orders", catalog="main", schema="public").length() == 2 &&
    client.primary_keys("missing").is_empty() else {
    fail("primary key filters failed")
  }
  let imports = client.imported_keys("orders", catalog="main", schema="public")
  guard imports.length() == 1 &&
    imports[0].pk_table_name == "customers" &&
    imports[0].fk_column_name == "customer_id" &&
    imports[0].update_rule == 0 &&
    imports[0].delete_rule == 2 &&
    imports[0].key_sequence == 1 else {
    fail("wrong imported keys")
  }
  guard client.exported_keys("customers").length() == 1 &&
    client.imported_keys("missing").is_empty() &&
    client.imported_keys("orders", catalog="", schema="").is_empty() else {
    fail("foreign key filters failed")
  }
  let local_foreign_keys = client.imported_keys(
    "local_child",
    catalog="",
    schema="",
  )
  guard local_foreign_keys.length() == 1 &&
    local_foreign_keys[0].pk_catalog_name is None &&
    local_foreign_keys[0].fk_key_name is None &&
    local_foreign_keys[0].delete_rule == 4 else {
    fail("empty foreign key filters or nullable fields failed")
  }
  println("PASS: schema, table, type and key discovery")
}
