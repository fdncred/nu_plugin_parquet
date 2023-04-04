use bytes::Bytes;
use chrono::{DateTime, Duration, FixedOffset, TimeZone};
use indexmap::map::IndexMap;
use nu_plugin::LabeledError;
use nu_protocol::{ShellError, Span, Spanned, Value};
use parquet::basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType};
use parquet::file::metadata::{KeyValue, RowGroupMetaData};
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::record::{Field, Row};
use parquet::schema::types::{SchemaDescriptor, Type};
use std::convert::TryInto;
use std::ops::Add;

fn convert_to_nu(field: &Field, span: Span) -> Value {
    let epoch: DateTime<FixedOffset> = match FixedOffset::west_opt(0)
        .expect("This should never fail, said the naive person.")
        .with_ymd_and_hms(1970, 1, 1, 0, 0, 0)
    {
        chrono::LocalResult::Single(dt) => dt,
        _ => panic!("This should never fail, said the other naive person."),
    };
    match field {
        Field::Null => Value::nothing(span),
        Field::Bool(b) => Value::boolean(*b, span),
        Field::Byte(b) => Value::binary(vec![*b as u8], span),
        Field::UByte(b) => Value::binary(vec![*b], span),
        Field::Short(s) => Value::int((*s).into(), span),
        Field::UShort(s) => Value::int((*s).into(), span),
        Field::Int(i) => Value::int((*i).into(), span),
        Field::UInt(i) => Value::int((*i).into(), span),
        Field::Long(l) => Value::int(*l, span),
        Field::ULong(l) => (*l)
            .try_into()
            .map(|l| Value::int(l, span))
            .unwrap_or_else(|e| Value::Error {
                error: Box::new(ShellError::CantConvert {
                    to_type: "i64".into(),
                    from_type: "u64".into(),
                    span,
                    help: Some(e.to_string()),
                }),
            }),
        Field::Float(f) => Value::float((*f).into(), span),
        Field::Double(f) => Value::float(*f, span),
        Field::Str(s) => Value::string(s, span),
        Field::Bytes(bytes) => Value::binary(bytes.data().to_vec(), span),
        Field::Date(days_since_epoch) => {
            let val = epoch.add(Duration::days(*days_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::TimestampMillis(millis_since_epoch) => {
            let val = epoch.add(Duration::milliseconds(*millis_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::TimestampMicros(micros_since_epoch) => {
            let val = epoch.add(Duration::microseconds(*micros_since_epoch as i64));
            Value::Date { val, span }
        }
        Field::Decimal(_d) => {
            unimplemented!("Parquet DECIMAL is not handled yet")
        }
        Field::Group(_row) => {
            unimplemented!("Nested structs not supported yet")
        }
        Field::ListInternal(_list) => {
            unimplemented!("Lists not supported yet")
        }
        Field::MapInternal(_map) => {
            unimplemented!("Maps not supported yet")
        }
    }
}

fn convert_parquet_row(row: Row, span: Span) -> Value {
    let mut cols = vec![];
    let mut vals = vec![];
    for (name, field) in row.get_column_iter() {
        cols.push(name.clone());
        vals.push(convert_to_nu(field, span));
    }
    Value::Record { cols, vals, span }
}

pub fn from_parquet_bytes(bytes: Vec<u8>, span: Span) -> Result<Value, LabeledError> {
    let cursor = Bytes::from(bytes);
    match SerializedFileReader::new(cursor) {
        Ok(reader) => match reader.get_row_iter(None) {
            Ok(iter) => {
                let mut vals = Vec::new();
                for record in iter {
                    let row = convert_parquet_row(record, span);
                    vals.push(row);
                }
                Ok(Value::List { vals, span })
            }
            Err(e) => Err(LabeledError {
                label: "Could not read rows".into(),
                msg: format!("{}", e),
                span: Some(span),
            }),
        },
        Err(e) => Err(LabeledError {
            label: "Could not read Parquet file".into(),
            msg: format!("{}", e),
            span: Some(span),
        }),
    }
}

pub fn metadata_from_parquet_bytes(bytes: Vec<u8>, span: Span) -> Result<Value, LabeledError> {
    let cursor = Bytes::from(bytes);
    match SerializedFileReader::new(cursor) {
        Ok(reader) => {
            let metadata = reader.metadata();
            let mut val = IndexMap::new();
            let file_metadata = metadata.file_metadata();
            val.insert(
                "version".to_string(),
                Value::int(file_metadata.version() as i64, span),
            );
            val.insert(
                "creator".to_string(),
                Value::string(file_metadata.created_by().unwrap_or(""), span),
            );
            val.insert(
                "num_rows".to_string(),
                Value::int(file_metadata.num_rows() as i64, span),
            );
            val.insert(
                "key_values".to_string(),
                key_value_metadata_to_value(file_metadata.key_value_metadata(), span),
            );
            val.insert(
                "schema".to_string(),
                schema_descriptor_to_value(file_metadata.schema_descr(), span),
            );
            val.insert(
                "row_groups".to_string(),
                row_groups_to_value(metadata.row_groups(), span),
            );

            Ok(Value::from(Spanned { item: val, span }))
        }
        Err(e) => Err(LabeledError {
            label: "Could not read Parquet file".into(),
            msg: format!("{}", e),
            span: Some(span),
        }),
    }
}

fn key_value_metadata_to_value(key_value_metadata: Option<&Vec<KeyValue>>, span: Span) -> Value {
    let mut vals = Vec::new();
    if let Some(key_value_metadata) = key_value_metadata {
        for key_value in key_value_metadata {
            let mut val = IndexMap::new();
            val.insert(
                "key".to_string(),
                Value::string(key_value.key.clone(), span),
            );
            val.insert(
                "value".to_string(),
                Value::string(key_value.value.clone().unwrap_or("".to_string()), span),
            );
            vals.push(Value::from(Spanned { item: val, span }));
        }
    }
    Value::List { vals, span }
}

fn schema_descriptor_to_value(schema: &SchemaDescriptor, span: Span) -> Value {
    let mut val = IndexMap::new();
    val.insert("name".to_string(), Value::string(schema.name(), span));
    val.insert(
        "num_columns".to_string(),
        Value::int(schema.num_columns() as i64, span),
    );
    val.insert(
        "schema".to_string(),
        schema_to_value(schema.root_schema(), span),
    );
    Value::from(Spanned { item: val, span })
}

fn schema_to_value(tp: &Type, span: Span) -> Value {
    match *tp {
        Type::PrimitiveType {
            ref basic_info,
            physical_type,
            type_length,
            scale,
            precision,
        } => {
            let mut val = IndexMap::new();
            val.insert(
                "name".to_string(),
                Value::string(basic_info.name().clone(), span),
            );
            val.insert(
                "repetition".to_string(),
                Value::string(basic_info.repetition().to_string(), span),
            );
            val.insert(
                "type".to_string(),
                Value::string(physical_type.to_string(), span),
            );
            val.insert(
                "type_length".to_string(),
                match physical_type {
                    PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => {
                        Value::int(type_length as i64, span)
                    }
                    _ => Value::nothing(span),
                },
            );
            val.insert(
                "logical_type".to_string(),
                Value::string(
                    logical_or_converted_type_to_string(
                        basic_info.logical_type(),
                        basic_info.converted_type(),
                        precision,
                        scale,
                    ),
                    span,
                ),
            );
            Value::from(Spanned { item: val, span })
        }
        Type::GroupType {
            basic_info: _,
            ref fields,
        } => {
            let mut vals = Vec::new();
            for field in fields {
                vals.push(schema_to_value(field, span));
            }
            Value::List { vals, span }
        }
    }
}

fn logical_or_converted_type_to_string(
    logical_type: Option<LogicalType>,
    converted_type: ConvertedType,
    precision: i32,
    scale: i32,
) -> String {
    match logical_type {
        Some(logical_type) => match logical_type {
            LogicalType::Bson => "BSON".to_string(),
            LogicalType::Date => "DATE".to_string(),
            LogicalType::Decimal { precision, scale } => {
                format!("DECIMAL({},{})", precision, scale)
            }
            LogicalType::Enum => "ENUM".to_string(),
            LogicalType::Integer {
                bit_width,
                is_signed,
            } => {
                format!("INTEGER({},{})", bit_width, is_signed)
            }
            LogicalType::Json => "JSON".to_string(),
            LogicalType::List => "LIST".to_string(),
            LogicalType::Map => "MAP".to_string(),
            LogicalType::String => "STRING".to_string(),
            LogicalType::Time {
                is_adjusted_to_u_t_c,
                unit,
            } => {
                format!(
                    "TIME({},{})",
                    time_unit_to_string(unit),
                    is_adjusted_to_u_t_c
                )
            }
            LogicalType::Timestamp {
                is_adjusted_to_u_t_c,
                unit,
            } => {
                format!(
                    "TIMESTAMP({},{})",
                    time_unit_to_string(unit),
                    is_adjusted_to_u_t_c
                )
            }
            LogicalType::Uuid => "UUID".to_string(),
            LogicalType::Unknown => "UNKNOWN".to_string(),
        },
        None => match converted_type {
            ConvertedType::BSON => "BSON".to_string(),
            ConvertedType::DATE => "DATE".to_string(),
            ConvertedType::DECIMAL => format!("DECIMAL({},{})", precision, scale),
            ConvertedType::ENUM => "ENUM".to_string(),
            ConvertedType::INTERVAL => "INTERVAL".to_string(),
            ConvertedType::INT_16 => "INT_16".to_string(),
            ConvertedType::INT_32 => "INT_32".to_string(),
            ConvertedType::INT_64 => "INT_64".to_string(),
            ConvertedType::INT_8 => "INT_8".to_string(),
            ConvertedType::JSON => "JSON".to_string(),
            ConvertedType::LIST => "LIST".to_string(),
            ConvertedType::MAP => "MAP".to_string(),
            ConvertedType::MAP_KEY_VALUE => "MAP_KEY_VALUE".to_string(),
            ConvertedType::NONE => "".to_string(),
            ConvertedType::TIMESTAMP_MICROS => "TIMESTAMP_MICROS".to_string(),
            ConvertedType::TIMESTAMP_MILLIS => "TIMESTAMP_MILLIS".to_string(),
            ConvertedType::TIME_MICROS => "TIME_MICROS".to_string(),
            ConvertedType::TIME_MILLIS => "TIME_MILLIS".to_string(),
            ConvertedType::UINT_16 => "UINT_16".to_string(),
            ConvertedType::UINT_32 => "UINT_32".to_string(),
            ConvertedType::UINT_64 => "UINT_64".to_string(),
            ConvertedType::UINT_8 => "UINT_8".to_string(),
            ConvertedType::UTF8 => "UTF8".to_string(),
        },
    }
}

fn time_unit_to_string(unit: TimeUnit) -> String {
    match unit {
        TimeUnit::MILLIS(_) => "MILLISECONDS".to_string(),
        TimeUnit::MICROS(_) => "MICROSECONDS".to_string(),
        TimeUnit::NANOS(_) => "NANOSECONDS".to_string(),
    }
}

fn row_groups_to_value(row_groups: &[RowGroupMetaData], span: Span) -> Value {
    let mut vals = Vec::new();
    for (_, row_group) in row_groups.iter().enumerate() {
        let mut val = IndexMap::new();
        val.insert(
            "num_rows".to_string(),
            Value::int(row_group.num_rows() as i64, span),
        );
        val.insert(
            "total_byte_size".to_string(),
            Value::int(row_group.total_byte_size() as i64, span),
        );
        vals.push(Value::from(Spanned { item: val, span }));
    }
    Value::List { vals, span }
}
