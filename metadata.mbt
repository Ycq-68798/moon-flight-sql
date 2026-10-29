///|
/// A database schema advertised by the server.
pub(all) struct DatabaseSchema {
  catalog_name : String?
  schema_name : String
} derive(Eq, Debug)

///|
pub extend DatabaseSchema with Eq::{equal, not_equal}

///|
pub extend DatabaseSchema with @debug.Debug::{to_repr}

///|
/// A table or view. `table_schema` is the serialized Arrow IPC schema when
/// `include_schema=true`; its bytes are not decoded by this API.
pub(all) struct TableInfo {
  catalog_name : String?
  schema_name : String?
  table_name : String
  table_type : String
  table_schema : Bytes?
} derive(Eq, Debug)

///|
pub extend TableInfo with Eq::{equal, not_equal}

///|
pub extend TableInfo with @debug.Debug::{to_repr}

///|
/// One column of a table's primary key, including the server-provided sequence.
pub(all) struct PrimaryKey {
  catalog_name : String?
  schema_name : String?
  table_name : String
  column_name : String
  key_name : String?
  key_sequence : Int32
} derive(Eq, Debug)

///|
pub extend PrimaryKey with Eq::{equal, not_equal}

///|
pub extend PrimaryKey with @debug.Debug::{to_repr}

///|
fn db_schemas_descriptor(catalog : String?, pattern : String?) -> Bytes {
  let payload = @proto.PbWriter::new()
  if catalog is Some(value) {
    payload.string_(1, value)
  }
  if pattern is Some(value) {
    payload.string_(2, value)
  }
  command_descriptor(
    "type.googleapis.com/arrow.flight.protocol.sql.CommandGetDbSchemas",
    payload.to_bytes(),
  )
}

///|
fn tables_descriptor(
  catalog : String?,
  schema_pattern : String?,
  table_pattern : String?,
  table_types : Array[String],
  include_schema : Bool,
) -> Bytes {
  let payload = @proto.PbWriter::new()
  if catalog is Some(value) {
    payload.string_(1, value)
  }
  if schema_pattern is Some(value) {
    payload.string_(2, value)
  }
  if table_pattern is Some(value) {
    payload.string_(3, value)
  }
  for value in table_types {
    payload.string_(4, value)
  }
  if include_schema {
    payload.bool_(5, true)
  }
  command_descriptor(
    "type.googleapis.com/arrow.flight.protocol.sql.CommandGetTables",
    payload.to_bytes(),
  )
}

///|
fn table_types_descriptor() -> Bytes {
  command_descriptor(
    "type.googleapis.com/arrow.flight.protocol.sql.CommandGetTableTypes", b"",
  )
}

///|
fn primary_keys_descriptor(
  table : String,
  catalog : String?,
  schema : String?,
) -> Bytes {
  let payload = @proto.PbWriter::new()
  if catalog is Some(value) {
    payload.string_(1, value)
  }
  if schema is Some(value) {
    payload.string_(2, value)
  }
  payload.string_(3, table)
  command_descriptor(
    "type.googleapis.com/arrow.flight.protocol.sql.CommandGetPrimaryKeys",
    payload.to_bytes(),
  )
}

///|
fn check_metadata_schema(
  result : DescriptorResult,
  expected : Array[(String, @arrow.DataType, Bool)],
) -> Unit raise FlightError {
  let schema = match result.schema {
    Some(schema) => schema
    None => raise FlightError::Protocol("missing Flight SQL metadata schema")
  }
  if schema.fields.length() != expected.length() {
    raise FlightError::Protocol("invalid Flight SQL metadata schema")
  }
  for i in 0..<expected.length() {
    let f = schema.fields[i]
    let (name, kind, nullable) = expected[i]
    if f.name != name ||
      f.data_type != kind ||
      f.nullable != nullable ||
      !f.children.is_empty() {
      raise FlightError::Protocol("invalid Flight SQL metadata schema")
    }
  }
}

///|
fn optional_text(value : @arrow.Value?) -> String? raise FlightError {
  match value {
    None => None
    Some(@arrow.Value::Utf8Value(text)) => Some(text)
    _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
  }
}

///|
/// Find schemas. `catalog=""` means no catalog; omitting it applies no filter.
/// Pattern matching follows the server's SQL LIKE semantics.
pub async fn Client::db_schemas(
  self : Client,
  catalog? : String,
  pattern? : String,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[DatabaseSchema] {
  let result = self.execute_descriptor(
    db_schemas_descriptor(catalog, pattern),
    timeout_ms~,
    metadata~,
  )
  check_metadata_schema(result, [
    ("catalog_name", @arrow.DataType::Utf8, true),
    ("db_schema_name", @arrow.DataType::Utf8, false),
  ])
  let rows = []
  for part in result.parts {
    for batch in part.batches {
      for i in 0..<batch.num_rows {
        match batch.get_row(i) {
          [catalog_value, Some(@arrow.Value::Utf8Value(name))] =>
            rows.push({
              catalog_name: optional_text(catalog_value),
              schema_name: name,
            })
          _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
        }
      }
    }
  }
  rows
}

///|
/// Find tables using optional catalog and SQL LIKE schema/name patterns.
/// `table_types` is sent verbatim; an empty array leaves types unrestricted.
pub async fn Client::tables(
  self : Client,
  catalog? : String,
  schema_pattern? : String,
  table_pattern? : String,
  table_types~ : Array[String] = [],
  include_schema~ : Bool = false,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[TableInfo] {
  let descriptor = tables_descriptor(
    catalog, schema_pattern, table_pattern, table_types, include_schema,
  )
  let result = self.execute_descriptor(descriptor, timeout_ms~, metadata~)
  let expected : Array[(String, @arrow.DataType, Bool)] = [
    ("catalog_name", @arrow.DataType::Utf8, true),
    ("db_schema_name", @arrow.DataType::Utf8, true),
    ("table_name", @arrow.DataType::Utf8, false),
    ("table_type", @arrow.DataType::Utf8, false),
  ]
  if include_schema {
    expected.push(("table_schema", @arrow.DataType::Binary, false))
  }
  check_metadata_schema(result, expected)
  let rows = []
  for part in result.parts {
    for batch in part.batches {
      for i in 0..<batch.num_rows {
        let row = batch.get_row(i)
        match row {
          [
            catalog_value,
            schema_value,
            Some(@arrow.Value::Utf8Value(name)),
            Some(@arrow.Value::Utf8Value(kind)),
            .. rest,
          ] => {
            let schema = if include_schema {
              match rest {
                [Some(@arrow.Value::BinaryValue(bytes))] if !bytes.is_empty() =>
                  Some(bytes)
                _ =>
                  raise FlightError::Protocol(
                    "invalid Flight SQL table schema value",
                  )
              }
            } else {
              if !rest.is_empty() {
                raise FlightError::Protocol(
                  "unexpected Flight SQL table metadata column",
                )
              }
              None
            }
            rows.push({
              catalog_name: optional_text(catalog_value),
              schema_name: optional_text(schema_value),
              table_name: name,
              table_type: kind,
              table_schema: schema,
            })
          }
          _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
        }
      }
    }
  }
  rows
}

///|
/// List table types advertised by the server.
pub async fn Client::table_types(
  self : Client,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[String] {
  let result = self.execute_descriptor(
    table_types_descriptor(),
    timeout_ms~,
    metadata~,
  )
  check_metadata_schema(result, [("table_type", @arrow.DataType::Utf8, false)])
  let rows = []
  for part in result.parts {
    for batch in part.batches {
      for i in 0..<batch.num_rows {
        match batch.get_row(i) {
          [Some(@arrow.Value::Utf8Value(value))] => rows.push(value)
          _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
        }
      }
    }
  }
  rows
}

///|
/// Find primary-key columns for an exact table name. Omitted catalog/schema
/// impose no filter; an explicitly empty string selects entries without one.
pub async fn Client::primary_keys(
  self : Client,
  table : String,
  catalog? : String,
  schema? : String,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[PrimaryKey] {
  let result = self.execute_descriptor(
    primary_keys_descriptor(table, catalog, schema),
    timeout_ms~,
    metadata~,
  )
  check_metadata_schema(result, [
    ("catalog_name", @arrow.DataType::Utf8, true),
    ("db_schema_name", @arrow.DataType::Utf8, true),
    ("table_name", @arrow.DataType::Utf8, false),
    ("column_name", @arrow.DataType::Utf8, false),
    ("key_name", @arrow.DataType::Utf8, true),
    ("key_sequence", @arrow.DataType::Int32, false),
  ])
  let rows = []
  for part in result.parts {
    for batch in part.batches {
      for i in 0..<batch.num_rows {
        match batch.get_row(i) {
          [
            catalog_value,
            schema_value,
            Some(@arrow.Value::Utf8Value(name)),
            Some(@arrow.Value::Utf8Value(column)),
            key_value,
            Some(@arrow.Value::Int32Value(sequence)),
          ] =>
            rows.push({
              catalog_name: optional_text(catalog_value),
              schema_name: optional_text(schema_value),
              table_name: name,
              column_name: column,
              key_name: optional_text(key_value),
              key_sequence: sequence,
            })
          _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
        }
      }
    }
  }
  rows
}

///|
/// A foreign-key column and the referenced primary-key column.
pub(all) struct ForeignKey {
  pk_catalog_name : String?
  pk_schema_name : String?
  pk_table_name : String
  pk_column_name : String
  fk_catalog_name : String?
  fk_schema_name : String?
  fk_table_name : String
  fk_column_name : String
  key_sequence : Int32
  fk_key_name : String?
  pk_key_name : String?
  update_rule : Int32
  delete_rule : Int32
} derive(Eq, Debug)

///|
pub extend ForeignKey with Eq::{equal, not_equal}

///|
pub extend ForeignKey with @debug.Debug::{to_repr}

///|
fn foreign_keys_descriptor(
  command : String,
  table : String,
  catalog : String?,
  schema : String?,
) -> Bytes {
  let payload = @proto.PbWriter::new()
  if catalog is Some(value) {
    payload.string_(1, value)
  }
  if schema is Some(value) {
    payload.string_(2, value)
  }
  payload.string_(3, table)
  command_descriptor(
    "type.googleapis.com/arrow.flight.protocol.sql." + command,
    payload.to_bytes(),
  )
}

///|
async fn Client::foreign_keys(
  self : Client,
  command : String,
  table : String,
  catalog : String?,
  schema : String?,
  timeout_ms~ : Int32,
  metadata~ : Array[Metadata],
) -> Array[ForeignKey] {
  let result = self.execute_descriptor(
    foreign_keys_descriptor(command, table, catalog, schema),
    timeout_ms~,
    metadata~,
  )
  check_metadata_schema(result, [
    ("pk_catalog_name", @arrow.DataType::Utf8, true),
    ("pk_db_schema_name", @arrow.DataType::Utf8, true),
    ("pk_table_name", @arrow.DataType::Utf8, false),
    ("pk_column_name", @arrow.DataType::Utf8, false),
    ("fk_catalog_name", @arrow.DataType::Utf8, true),
    ("fk_db_schema_name", @arrow.DataType::Utf8, true),
    ("fk_table_name", @arrow.DataType::Utf8, false),
    ("fk_column_name", @arrow.DataType::Utf8, false),
    ("key_sequence", @arrow.DataType::Int32, false),
    ("fk_key_name", @arrow.DataType::Utf8, true),
    ("pk_key_name", @arrow.DataType::Utf8, true),
    ("update_rule", @arrow.DataType::UInt8, false),
    ("delete_rule", @arrow.DataType::UInt8, false),
  ])
  let rows = []
  for part in result.parts {
    for batch in part.batches {
      for i in 0..<batch.num_rows {
        match batch.get_row(i) {
          [
            pk_cat,
            pk_schema,
            Some(@arrow.Value::Utf8Value(pk_table)),
            Some(@arrow.Value::Utf8Value(pk_column)),
            fk_cat,
            fk_schema,
            Some(@arrow.Value::Utf8Value(fk_table)),
            Some(@arrow.Value::Utf8Value(fk_column)),
            Some(@arrow.Value::Int32Value(sequence)),
            fk_key,
            pk_key,
            Some(@arrow.Value::UInt8Value(update)),
            Some(@arrow.Value::UInt8Value(delete)),
          ] =>
            rows.push({
              pk_catalog_name: optional_text(pk_cat),
              pk_schema_name: optional_text(pk_schema),
              pk_table_name: pk_table,
              pk_column_name: pk_column,
              fk_catalog_name: optional_text(fk_cat),
              fk_schema_name: optional_text(fk_schema),
              fk_table_name: fk_table,
              fk_column_name: fk_column,
              key_sequence: sequence,
              fk_key_name: optional_text(fk_key),
              pk_key_name: optional_text(pk_key),
              update_rule: update,
              delete_rule: delete,
            })
          _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
        }
      }
    }
  }
  rows
}

///|
/// Retrieve keys imported by a table. Optional filters preserve empty strings.
pub async fn Client::imported_keys(
  self : Client,
  table : String,
  catalog? : String,
  schema? : String,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[ForeignKey] {
  self.foreign_keys(
    "CommandGetImportedKeys",
    table,
    catalog,
    schema,
    timeout_ms~,
    metadata~,
  )
}

///|
/// Retrieve keys exported by a table. Optional filters preserve empty strings.
pub async fn Client::exported_keys(
  self : Client,
  table : String,
  catalog? : String,
  schema? : String,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[ForeignKey] {
  self.foreign_keys(
    "CommandGetExportedKeys",
    table,
    catalog,
    schema,
    timeout_ms~,
    metadata~,
  )
}
