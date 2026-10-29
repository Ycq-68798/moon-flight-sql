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
/// `include_schema=true`; call `decode_schema` for an explicit validation.
pub(all) struct TableInfo {
  catalog_name : String?
  schema_name : String?
  table_name : String
  table_type : String
  table_schema : Bytes?
} derive(Eq, Debug)

///|
/// Decode one encapsulated Arrow IPC schema message, rejecting trailing bytes.
/// Legacy four-byte and modern continuation-prefixed framing are accepted.
pub fn decode_table_schema(data : Bytes) -> @arrow.Schema raise {
  if data.length() < 4 {
    raise FlightError::Protocol("truncated table schema IPC prefix")
  }
  let continuation = data[0] == b'\xff' &&
    data[1] == b'\xff' &&
    data[2] == b'\xff' &&
    data[3] == b'\xff'
  let prefix = if continuation { 8 } else { 4 }
  if data.length() < prefix {
    raise FlightError::Protocol("truncated table schema IPC prefix")
  }
  let pos = prefix - 4
  let size = data[pos].to_int64() |
    (data[pos + 1].to_int64() << 8) |
    (data[pos + 2].to_int64() << 16) |
    (data[pos + 3].to_int64() << 24)
  if size <= 0L ||
    size > 2147483647L ||
    size != (data.length() - prefix).to_int64() {
    raise FlightError::Protocol("invalid table schema IPC message boundary")
  }
  @arrow.StreamReader::new(data).schema
}

///|
/// Decode the included schema, or return None when it was not requested.
pub fn TableInfo::decode_schema(self : TableInfo) -> @arrow.Schema? raise {
  match self.table_schema {
    None => None
    Some(data) => Some(decode_table_schema(data))
  }
}

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
  descriptor : Bytes,
  timeout_ms~ : Int32,
  metadata~ : Array[Metadata],
) -> Array[ForeignKey] {
  let result = self.execute_descriptor(descriptor, timeout_ms~, metadata~)
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
    foreign_keys_descriptor("CommandGetImportedKeys", table, catalog, schema),
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
    foreign_keys_descriptor("CommandGetExportedKeys", table, catalog, schema),
    timeout_ms~,
    metadata~,
  )
}

///|
fn cross_reference_descriptor(
  pk_table : String,
  fk_table : String,
  pk_catalog : String?,
  pk_schema : String?,
  fk_catalog : String?,
  fk_schema : String?,
) -> Bytes {
  let payload = @proto.PbWriter::new()
  if pk_catalog is Some(value) {
    payload.string_(1, value)
  }
  if pk_schema is Some(value) {
    payload.string_(2, value)
  }
  payload.string_(3, pk_table)
  if fk_catalog is Some(value) {
    payload.string_(4, value)
  }
  if fk_schema is Some(value) {
    payload.string_(5, value)
  }
  payload.string_(6, fk_table)
  command_descriptor(
    "type.googleapis.com/arrow.flight.protocol.sql.CommandGetCrossReference",
    payload.to_bytes(),
  )
}

///|
/// Find foreign keys linking the named parent and foreign tables. Empty optional
/// catalog/schema filters select entries with no catalog/schema; omission leaves
/// the corresponding filter unspecified.
pub async fn Client::cross_reference(
  self : Client,
  pk_table : String,
  fk_table : String,
  pk_catalog? : String,
  pk_schema? : String,
  fk_catalog? : String,
  fk_schema? : String,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[ForeignKey] {
  self.foreign_keys(
    cross_reference_descriptor(
      pk_table, fk_table, pk_catalog, pk_schema, fk_catalog, fk_schema,
    ),
    timeout_ms~,
    metadata~,
  )
}

///|
/// XDBC type information returned by the server. Numeric code values are
/// preserved as sent, including vendor-specific or future values.
pub(all) struct XdbcTypeInfo {
  type_name : String
  data_type : Int32
  column_size : Int32?
  literal_prefix : String?
  literal_suffix : String?
  create_params : Array[String]?
  nullable : Int32
  case_sensitive : Bool
  searchable : Int32
  unsigned_attribute : Bool?
  fixed_prec_scale : Bool
  auto_increment : Bool?
  local_type_name : String?
  minimum_scale : Int32?
  maximum_scale : Int32?
  sql_data_type : Int32
  datetime_subcode : Int32?
  num_prec_radix : Int32?
  interval_precision : Int32?
} derive(Debug)

///|
pub extend XdbcTypeInfo with @debug.Debug::{to_repr}

///|
fn xdbc_descriptor(data_type : Int32?) -> Bytes {
  let payload = @proto.PbWriter::new()
  if data_type is Some(value) {
    payload.int32(1, value)
  }
  command_descriptor(
    "type.googleapis.com/arrow.flight.protocol.sql.CommandGetXdbcTypeInfo",
    payload.to_bytes(),
  )
}

///|
fn xdbc_optional_int(value : @arrow.Value?) -> Int32? raise FlightError {
  match value {
    None => None
    Some(@arrow.Value::Int32Value(v)) => Some(v)
    _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
  }
}

///|
fn xdbc_optional_bool(value : @arrow.Value?) -> Bool? raise FlightError {
  match value {
    None => None
    Some(@arrow.Value::BooleanValue(v)) => Some(v)
    _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
  }
}

///|
fn xdbc_params(value : @arrow.Value?) -> Array[String]? raise FlightError {
  match value {
    None => None
    Some(@arrow.Value::ListValue(items)) => {
      let values = []
      for item in items {
        match item {
          Some(@arrow.Value::Utf8Value(text)) => values.push(text)
          _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
        }
      }
      Some(values)
    }
    _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
  }
}

///|
/// Query supported XDBC types. Omission requests all types; explicit zero is
/// sent as a filter, and negative standard XDBC codes are supported.
pub async fn Client::xdbc_type_info(
  self : Client,
  data_type? : Int32,
  timeout_ms~ : Int32 = 30000,
  metadata~ : Array[Metadata] = [],
) -> Array[XdbcTypeInfo] {
  let result = self.execute_descriptor(
    xdbc_descriptor(data_type),
    timeout_ms~,
    metadata~,
  )
  let schema = match result.schema {
    Some(value) => value
    None => raise FlightError::Protocol("missing Flight SQL metadata schema")
  }
  let expected : Array[(String, @arrow.DataType, Bool)] = [
    ("type_name", @arrow.DataType::Utf8, false),
    ("data_type", @arrow.DataType::Int32, false),
    ("column_size", @arrow.DataType::Int32, true),
    ("literal_prefix", @arrow.DataType::Utf8, true),
    ("literal_suffix", @arrow.DataType::Utf8, true),
    ("create_params", @arrow.DataType::List, true),
    ("nullable", @arrow.DataType::Int32, false),
    ("case_sensitive", @arrow.DataType::Boolean, false),
    ("searchable", @arrow.DataType::Int32, false),
    ("unsigned_attribute", @arrow.DataType::Boolean, true),
    ("fixed_prec_scale", @arrow.DataType::Boolean, false),
    ("auto_increment", @arrow.DataType::Boolean, true),
    ("local_type_name", @arrow.DataType::Utf8, true),
    ("minimum_scale", @arrow.DataType::Int32, true),
    ("maximum_scale", @arrow.DataType::Int32, true),
    ("sql_data_type", @arrow.DataType::Int32, false),
    ("datetime_subcode", @arrow.DataType::Int32, true),
    ("num_prec_radix", @arrow.DataType::Int32, true),
    ("interval_precision", @arrow.DataType::Int32, true),
  ]
  if schema.fields.length() != expected.length() {
    raise FlightError::Protocol("invalid Flight SQL metadata schema")
  }
  for i in 0..<expected.length() {
    let field = schema.fields[i]
    let (name, kind, nullable) = expected[i]
    if field.name != name ||
      field.data_type != kind ||
      field.nullable != nullable {
      raise FlightError::Protocol("invalid Flight SQL metadata schema")
    }
    if i == 5 {
      if field.children.length() != 1 ||
        field.children[0].data_type != @arrow.DataType::Utf8 ||
        field.children[0].nullable ||
        !field.children[0].children.is_empty() {
        raise FlightError::Protocol("invalid Flight SQL metadata schema")
      }
    } else if !field.children.is_empty() {
      raise FlightError::Protocol("invalid Flight SQL metadata schema")
    }
  }
  let rows = []
  for part in result.parts {
    for batch in part.batches {
      for i in 0..<batch.num_rows {
        let row = batch.get_row(i)
        match row {
          [
            Some(@arrow.Value::Utf8Value(name)),
            Some(@arrow.Value::Int32Value(kind)),
            column_size,
            prefix,
            suffix,
            params,
            Some(@arrow.Value::Int32Value(nullable)),
            Some(@arrow.Value::BooleanValue(case_sensitive)),
            Some(@arrow.Value::Int32Value(searchable)),
            unsigned,
            Some(@arrow.Value::BooleanValue(fixed)),
            increment,
            local_name,
            min_scale,
            max_scale,
            Some(@arrow.Value::Int32Value(sql_type)),
            subcode,
            radix,
            precision,
          ] =>
            rows.push({
              type_name: name,
              data_type: kind,
              column_size: xdbc_optional_int(column_size),
              literal_prefix: optional_text(prefix),
              literal_suffix: optional_text(suffix),
              create_params: xdbc_params(params),
              nullable,
              case_sensitive,
              searchable,
              unsigned_attribute: xdbc_optional_bool(unsigned),
              fixed_prec_scale: fixed,
              auto_increment: xdbc_optional_bool(increment),
              local_type_name: optional_text(local_name),
              minimum_scale: xdbc_optional_int(min_scale),
              maximum_scale: xdbc_optional_int(max_scale),
              sql_data_type: sql_type,
              datetime_subcode: xdbc_optional_int(subcode),
              num_prec_radix: xdbc_optional_int(radix),
              interval_precision: xdbc_optional_int(precision),
            })
          _ => raise FlightError::Protocol("invalid Flight SQL metadata value")
        }
      }
    }
  }
  rows
}
