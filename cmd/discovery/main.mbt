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
  guard tables[0].decode_schema() is None else {
    fail("unexpected decoded schema")
  }
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
  guard views[0].table_schema is Some(_) else {
    fail("missing serialized table schema")
  }
  let table_schema = match views[0].decode_schema() {
    Some(schema) => schema
    None => fail("missing schema")
  }
  guard table_schema.fields.length() == 2 &&
    table_schema.fields[0].name == "ID" &&
    !table_schema.fields[0].nullable &&
    table_schema.fields[1].name == "labels" &&
    table_schema.fields[1].nullable &&
    table_schema.fields[1].children.length() == 1 &&
    table_schema.fields[1].children[0].nullable &&
    table_schema.fields[0].metadata.length() == 1 &&
    table_schema.fields[0].metadata[0].key == "ARROW:FLIGHT:SQL:TYPE_NAME" &&
    table_schema.fields[0].metadata[0].value == Some("BIGINT") &&
    table_schema.metadata.length() == 1 &&
    table_schema.metadata[0].value == Some("fixture") else {
    fail("wrong nested table schema or metadata")
  }
  guard views[0].table_schema is Some(raw_schema) else {
    fail("missing schema")
  }
  let trailing_rejected = try {
    let _ = @flight.decode_table_schema(raw_schema + b"x")
    false
  } catch {
    _ => true
  }
  let truncated : Array[Byte] = []
  for i in 0..<(raw_schema.length() - 1) {
    truncated.push(raw_schema[i])
  }
  let truncated_rejected = try {
    let _ = @flight.decode_table_schema(Bytes::from_array(truncated))
    false
  } catch {
    _ => true
  }
  guard trailing_rejected && truncated_rejected else {
    fail("invalid schema accepted")
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
  let refs = client.cross_reference(
    "customers",
    "orders",
    pk_catalog="main",
    pk_schema="public",
    fk_catalog="main",
    fk_schema="public",
  )
  guard refs == imports else {
    fail("cross reference did not return matching keys")
  }
  guard client.cross_reference("customers", "missing").is_empty() &&
    client.cross_reference("customers", "orders", fk_catalog="").is_empty() &&
    client.cross_reference("customers", "orders", pk_schema="").is_empty() &&
    client.cross_reference("customers", "orders", pk_catalog="").is_empty() &&
    client.cross_reference("customers", "orders", fk_schema="").is_empty() else {
    fail("cross reference filtering failed")
  }
  let local_refs = client.cross_reference(
    "local_parent",
    "local_child",
    pk_catalog="",
    pk_schema="",
    fk_catalog="",
    fk_schema="",
  )
  guard local_refs.length() == 1 && local_refs[0].pk_key_name is None else {
    fail("cross reference empty presence or nullable values failed")
  }
  let all_types = client.xdbc_type_info()
  guard all_types.length() == 3 &&
    all_types[0].type_name == "BIGINT" &&
    all_types[0].data_type == -5 &&
    all_types[0].create_params is None &&
    all_types[0].auto_increment == Some(true) &&
    all_types[1].create_params is Some([]) &&
    all_types[2].create_params is Some(["length"]) &&
    all_types[2].literal_prefix == Some("'") else {
    fail("XDBC metadata values were decoded incorrectly")
  }
  guard client.xdbc_type_info(data_type=0).length() == 1 &&
    client.xdbc_type_info(data_type=0)[0].type_name == "UNKNOWN" &&
    client.xdbc_type_info(data_type=-5)[0].type_name == "BIGINT" &&
    client.xdbc_type_info(data_type=99).is_empty() else {
    fail("XDBC optional or negative type filter failed")
  }
  println("PASS: schema, table, type and key discovery")
}
