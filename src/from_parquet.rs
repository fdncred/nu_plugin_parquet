use bytes::Bytes;
use chrono::{DateTime, Duration, FixedOffset, TimeZone};
use nu_protocol::Type as NuType;
use nu_protocol::record::Columns;
use nu_protocol::{LabeledError, Record, ShellError, Span, Value, record};
use parquet::basic::Repetition;
use parquet::basic::{ConvertedType, LogicalType, TimeUnit, Type as PhysicalType};
use parquet::column::writer::ColumnWriter;
use parquet::data_type::{AsBytes, ByteArray, Decimal};
use parquet::file::metadata::{KeyValue, RowGroupMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::reader::FileReader;
use parquet::file::serialized_reader::SerializedFileReader;
use parquet::file::writer::SerializedFileWriter;
use parquet::record::{Field, Row};
use parquet::schema::types::{SchemaDescriptor, Type};
use std::convert::TryInto;
use std::io::Cursor;
use std::ops::Add;
use std::sync::Arc;

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
        Field::Bool(b) => Value::bool(*b, span),
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
            .unwrap_or_else(|e| {
                Value::error(
                    ShellError::CantConvert {
                        to_type: "i64".into(),
                        from_type: "u64".into(),
                        span,
                        help: Some(e.to_string()),
                    },
                    span,
                )
            }),
        Field::Float16(f16) => Value::float((*f16).into(), span),
        Field::Float(f) => Value::float((*f).into(), span),
        Field::Double(f) => Value::float(*f, span),
        Field::Str(s) => Value::string(s, span),
        Field::Bytes(bytes) => Value::binary(bytes.data().to_vec(), span),
        Field::Date(days_since_epoch) => {
            let val = epoch.add(Duration::days(*days_since_epoch as i64));
            Value::date(val, span)
        }
        Field::TimestampMillis(millis_since_epoch) => {
            let val = epoch.add(Duration::milliseconds(*millis_since_epoch as i64));
            Value::date(val, span)
        }
        Field::TimestampMicros(micros_since_epoch) => {
            let val = epoch.add(Duration::microseconds(*micros_since_epoch as i64));
            Value::date(val, span)
        }
        Field::Decimal(d) => Value::string(decimal_to_string(d), span),
        Field::Group(row) => {
            let val = Record::from_iter(
                row.clone()
                    .into_columns()
                    .iter()
                    .map(|col| (col.0.clone(), convert_to_nu(&col.1, span))),
            );
            Value::record(val, span)
        }
        Field::ListInternal(list) => {
            let val = list
                .elements()
                .iter()
                .map(|e| convert_to_nu(e, span))
                .collect();
            Value::list(val, span)
        }
        Field::MapInternal(_map) => {
            unimplemented!("Maps not supported yet")
        }
        Field::TimeMillis(millis) => Value::duration((*millis * 1_000_000) as i64, span),
        Field::TimeMicros(micros) => Value::duration((*micros * 1000) as i64, span),
    }
}

fn convert_parquet_row(row: Row, span: Span) -> Value {
    let mut rec = Record::new();
    for (name, field) in row.get_column_iter() {
        rec.push(name.clone(), convert_to_nu(field, span));
    }
    Value::record(rec, span)
}

fn decimal_to_string(decimal: &Decimal) -> String {
    let str = match decimal {
        Decimal::Int32 {
            value,
            precision: _,
            scale: _,
        } => {
            let num = u32::from_be_bytes(*value);
            format!("{}", num)
        }
        Decimal::Int64 {
            value,
            precision: _,
            scale: _,
        } => {
            let num = u64::from_be_bytes(*value);
            format!("{}", num)
        }
        Decimal::Bytes {
            value,
            precision: _,
            scale: _,
        } => value
            .as_bytes()
            .iter()
            .map(|b| format!("{:x}", b))
            .collect(),
    };

    if decimal.scale() == 0 {
        str
    } else if str.len() <= decimal.scale() as usize {
        let mut s = String::new();
        s.push_str("0.");
        for _ in 0..(decimal.scale() as usize - str.len()) {
            s.push_str("0");
        }
        s.push_str(&str);
        s
    } else {
        let mut s = str;
        s.insert(s.len() - decimal.scale() as usize, '.');
        s
    }
}

pub fn from_parquet_bytes(bytes: Vec<u8>, span: Span) -> Result<Value, LabeledError> {
    let cursor = Bytes::from(bytes);
    match SerializedFileReader::new(cursor) {
        Ok(reader) => match reader.get_row_iter(None) {
            Ok(iter) => {
                let mut vals = Vec::new();
                for record in iter {
                    match record {
                        Ok(rec) => {
                            let row = convert_parquet_row(rec, span);
                            vals.push(row);
                        }
                        Err(e) => {
                            return Err(LabeledError::new(format!("{}", e))
                                .with_label("Could not read rows", span));
                        }
                    }
                }
                Ok(Value::list(vals, span))
            }
            Err(e) => {
                Err(LabeledError::new(format!("{}", e)).with_label("Could not read rows", span))
            }
        },
        Err(e) => {
            Err(LabeledError::new(format!("{}", e)).with_label("Could not read Parquet file", span))
        }
    }
}

pub fn metadata_from_parquet_bytes(bytes: Vec<u8>, span: Span) -> Result<Value, LabeledError> {
    let cursor = Bytes::from(bytes);
    match SerializedFileReader::new(cursor) {
        Ok(reader) => {
            let metadata = reader.metadata();
            let file_metadata = metadata.file_metadata();
            let rec = record!(
                "version" => Value::int(file_metadata.version() as i64, span),
                "creator" => Value::string(file_metadata.created_by().unwrap_or(""), span),
                "num_rows" => Value::int(file_metadata.num_rows() as i64, span),
                "key_values" => key_value_metadata_to_value(file_metadata.key_value_metadata(), span),
                "schema" => schema_descriptor_to_value(file_metadata.schema_descr(), span),
                "row_groups" => row_groups_to_value(metadata.row_groups(), span)
            );
            Ok(Value::record(rec, span))
        }
        Err(e) => {
            Err(LabeledError::new(format!("{}", e)).with_label("Could not read Parquet file", span))
        }
    }
}

fn key_value_metadata_to_value(key_value_metadata: Option<&Vec<KeyValue>>, span: Span) -> Value {
    let mut vals = Record::new();

    if let Some(key_value_metadata) = key_value_metadata {
        for key_value in key_value_metadata {
            vals.push(
                "key".to_string(),
                Value::string(key_value.key.clone(), span),
            );
            vals.push(
                "value".to_string(),
                Value::string(key_value.value.clone().unwrap_or("".to_string()), span),
            );
        }
    }
    Value::record(vals, span)
}

fn schema_descriptor_to_value(schema: &SchemaDescriptor, span: Span) -> Value {
    let rec = record!(
        "name" => Value::string(schema.name(), span),
        "num_columns" => Value::int(schema.num_columns() as i64, span),
        "schema" => schema_to_value(schema.root_schema(), span)
    );
    Value::record(rec, span)
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
            let rec = record!(
                "name" => Value::string(basic_info.name(), span),
                "repetition" => Value::string(basic_info.repetition().to_string(), span),
                "type" => Value::string(physical_type.to_string(), span),
                "type_length" => match physical_type {
                    PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY => Value::int(type_length as i64, span),
                    _ => Value::nothing(span)
                },
                "logical_type" => Value::string(logical_or_converted_type_to_string(basic_info.logical_type_ref().cloned(), basic_info.converted_type(), precision, scale), span)
            );
            Value::record(rec, span)
        }
        Type::GroupType {
            basic_info: _,
            ref fields,
        } => {
            let mut vals = Vec::new();
            for field in fields {
                vals.push(schema_to_value(field, span));
            }
            Value::list(vals, span)
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
            LogicalType::File => "FILE".to_string(),
            LogicalType::Bson => "BSON".to_string(),
            LogicalType::Date => "DATE".to_string(),
            LogicalType::Decimal(decimal) => {
                format!("DECIMAL({},{})", decimal.precision, decimal.scale)
            }
            LogicalType::Enum => "ENUM".to_string(),
            LogicalType::Integer(integer) => {
                format!("INTEGER({},{})", integer.bit_width, integer.is_signed)
            }
            LogicalType::Json => "JSON".to_string(),
            LogicalType::List => "LIST".to_string(),
            LogicalType::Map => "MAP".to_string(),
            LogicalType::String => "STRING".to_string(),
            LogicalType::Time(time) => {
                format!(
                    "TIME({},{})",
                    time_unit_to_string(time.unit),
                    time.is_adjusted_to_u_t_c
                )
            }
            LogicalType::Timestamp(timestamp) => {
                format!(
                    "TIMESTAMP({},{})",
                    time_unit_to_string(timestamp.unit),
                    timestamp.is_adjusted_to_u_t_c
                )
            }
            LogicalType::Uuid => "UUID".to_string(),
            LogicalType::Unknown => "UNKNOWN".to_string(),
            LogicalType::Float16 => "FLOAT_16".to_string(),
            LogicalType::Variant { .. } => "Variant".to_string(),
            LogicalType::Geometry { .. } => "Geometry".to_string(),
            LogicalType::Geography { .. } => "Geography".to_string(),
            LogicalType::_Unknown { .. } => "UNKNOWN".to_string(),
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
        TimeUnit::MILLIS => "MILLISECONDS".to_string(),
        TimeUnit::MICROS => "MICROSECONDS".to_string(),
        TimeUnit::NANOS => "NANOSECONDS".to_string(),
    }
}

fn row_groups_to_value(row_groups: &[RowGroupMetaData], span: Span) -> Value {
    let mut vals = Record::new();
    for (_, row_group) in row_groups.iter().enumerate() {
        vals.push(
            "num_rows".to_string(),
            Value::int(row_group.num_rows() as i64, span),
        );
        vals.push(
            "total_byte_size".to_string(),
            Value::int(row_group.total_byte_size() as i64, span),
        );
    }
    Value::record(vals, span)
}

pub fn to_parquet_bytes(table: &Vec<Value>, span: Span) -> Result<Value, LabeledError> {
    let first_record = match table.first() {
        Some(Value::Record {
            val,
            internal_span: _,
            ..
        }) => val,
        Some(_) => return Err(LabeledError::new("Not a Table")),
        None => return Err(LabeledError::new("Empty table")),
    };

    let schema = infer_schema(&first_record)?;

    // TODO use streaming plugin's protocol instead of doing everything at once?
    let mut output_buffer = Vec::new();

    let records = table
        .into_iter()
        .map(|r| match r {
            Value::Record {
                val,
                internal_span: _,
                ..
            } => Ok(val.clone().into_owned()), // TODO return &Record ?
            _ => Err(LabeledError::new("Not a table")),
        })
        .collect::<Result<Vec<_>, LabeledError>>()?;

    let cursor = Cursor::new(&mut output_buffer);
    write_records_to_parquet(schema, first_record.columns(), &records, cursor)?;

    Ok(Value::binary(output_buffer, span))
}

fn write_records_to_parquet(
    schema: Type,
    mut columns: Columns,
    records: &Vec<Record>,
    cursor: Cursor<&mut Vec<u8>>,
) -> Result<(), LabeledError> {
    let props = Arc::new(WriterProperties::builder().build());
    let mut writer = SerializedFileWriter::new(cursor, Arc::new(schema), props)
        .map_err(|e| LabeledError::new(format!("Cannot create file writer: {}", e.to_string())))?;
    let mut row_writer = writer
        .next_row_group()
        .map_err(|e| LabeledError::new(format!("Cannot create row writer: {}", e.to_string())))?;

    while let Some(mut col_writer) = row_writer
        .next_column()
        .map_err(|e| LabeledError::new(e.to_string()))?
    {
        let column_name = columns.next().ok_or(LabeledError::new("No more column"))?;
        let column_data = records
            .into_iter()
            .map(|r| {
                r.get(column_name)
                    .ok_or(LabeledError::new("No data in column"))
            })
            .collect::<Result<Vec<&Value>, _>>()?
            .into_iter();

        // Get the correct writer for each type
        match col_writer.untyped() {
            ColumnWriter::BoolColumnWriter(w) => {
                let values = column_data
                    .map(|v| {
                        v.as_bool()
                            .map_err(|_| LabeledError::new("Cannot convert to bool"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                w.write_batch(&values, None, None)
                    .map_err(|e| LabeledError::new(e.to_string()))?;
            }
            ColumnWriter::Int64ColumnWriter(w) => {
                let values = column_data
                    .map(|v| {
                        v.as_int()
                            .or_else(|_| v.as_filesize().map(|fs| fs.into())) // TODO : we loose the info that it was a file size
                            .map_err(|_| LabeledError::new("Cannot convert to int"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                w.write_batch(&values, None, None)
                    .map_err(|e| LabeledError::new(e.to_string()))?;
            }
            ColumnWriter::DoubleColumnWriter(w) => {
                let values = column_data
                    .map(|v| {
                        v.as_float()
                            .map_err(|_| LabeledError::new("Cannot convert to float"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                w.write_batch(&values, None, None)
                    .map_err(|e| LabeledError::new(e.to_string()))?;
            }
            ColumnWriter::ByteArrayColumnWriter(w) => {
                let values = column_data
                    .map(|v| {
                        v.as_str()
                            .map(|s| ByteArray::from(s))
                            .map_err(|_| LabeledError::new("Cannot convert to byte array"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                w.write_batch(&values, None, None)
                    .map_err(|e| LabeledError::new(e.to_string()))?;
            }
            _ => return Err(LabeledError::new("Type not supported")),
        }
        col_writer
            .close()
            .map_err(|e| LabeledError::new(e.to_string()))?;
    }

    row_writer
        .close()
        .map_err(|e| LabeledError::new(e.to_string()))?;
    writer
        .close()
        .map_err(|e| LabeledError::new(e.to_string()))?;

    Ok(())
}

fn infer_schema(record: &Record) -> Result<Type, LabeledError> {
    let types: Vec<Arc<Type>> = record
        .into_iter()
        .map(|(column, value)| value_to_type(column, value))
        // .map(Arc::new)
        .map(|t| t.map(|i| Arc::new(i)))
        .collect::<Result<_, _>>()?;
    let schema = Type::group_type_builder("schema")
        .with_fields(types)
        .build()
        .unwrap();
    Ok(schema)
}

fn value_to_type(column_name: &str, value: &Value) -> Result<Type, LabeledError> {
    let t = match value.get_type() {
        NuType::Bool | NuType::Int | NuType::Float => {
            let t = match value.get_type() {
                NuType::Bool => PhysicalType::BOOLEAN,
                NuType::Int => PhysicalType::INT64,
                NuType::Float => PhysicalType::DOUBLE,
                _ => panic!("Impossible"),
            };
            Type::primitive_type_builder(column_name, t)
        }
        NuType::String => Type::primitive_type_builder(column_name, PhysicalType::BYTE_ARRAY)
            .with_converted_type(ConvertedType::UTF8),
        NuType::Date => Type::primitive_type_builder(column_name, PhysicalType::INT64)
            .with_converted_type(ConvertedType::DATE),
        NuType::Filesize => Type::primitive_type_builder(column_name, PhysicalType::INT64),
        _ => {
            return Err(LabeledError::new(format!(
                "Cannot store {} type (not supported)",
                value.get_type()
            )));
        }
    };
    Ok(t.with_repetition(Repetition::REQUIRED).build().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use parquet::data_type::ByteArray;
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;

    #[test]
    fn test_decimal_to_string() {
        let decimal = Decimal::from_i32(123, 5, 0);
        assert_eq!(decimal_to_string(&decimal), "123");
        let decimal = Decimal::from_i64(123, 5, 0);
        assert_eq!(decimal_to_string(&decimal), "123");
        let decimal = Decimal::from_bytes(ByteArray::from(vec![1, 2, 3]), 5, 0);
        assert_eq!(decimal_to_string(&decimal), "123");

        let decimal = Decimal::from_i32(123, 5, 2);
        assert_eq!(decimal_to_string(&decimal), "1.23");
        let decimal = Decimal::from_i64(123, 5, 2);
        assert_eq!(decimal_to_string(&decimal), "1.23");
        let decimal = Decimal::from_bytes(ByteArray::from(vec![1, 2, 3]), 5, 2);
        assert_eq!(decimal_to_string(&decimal), "1.23");

        let decimal = Decimal::from_i32(123, 5, 5);
        assert_eq!(decimal_to_string(&decimal), "0.00123");
        let decimal = Decimal::from_i64(123, 5, 5);
        assert_eq!(decimal_to_string(&decimal), "0.00123");
        let decimal = Decimal::from_bytes(ByteArray::from(vec![1, 2, 3]), 5, 5);
        assert_eq!(decimal_to_string(&decimal), "0.00123");

        let decimal = Decimal::from_i32(0, 5, 5);
        assert_eq!(decimal_to_string(&decimal), "0.00000");
        let decimal = Decimal::from_i64(0, 5, 5);
        assert_eq!(decimal_to_string(&decimal), "0.00000");
        let decimal = Decimal::from_bytes(ByteArray::from(vec![]), 5, 5);
        assert_eq!(decimal_to_string(&decimal), "0.00000");
    }

    #[test]
    fn from_parquet_should_convert_list_of_strings_to_nu_list() {
        let bytes = std::fs::read("tests/list_strings.parquet").unwrap();
        let reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
        for row in reader.get_row_iter(None).unwrap() {
            let columns = row.unwrap().into_columns();
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "languages");
            let list = convert_to_nu(&columns[0].1, Span::test_data());
            let inner_list = list.as_list().unwrap();
            assert_eq!(inner_list.len(), 2);
            assert_eq!(inner_list[0].as_str().unwrap(), "Rust");
            assert_eq!(inner_list[1].as_str().unwrap(), "Nushell");
        }
    }

    #[test]
    fn from_parquet_should_convert_list_of_ints_to_nu_list() {
        let bytes = std::fs::read("tests/list_ints.parquet").unwrap();
        let reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
        for row in reader.get_row_iter(None).unwrap() {
            let columns = row.unwrap().into_columns();
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "numbers");
            let list = convert_to_nu(&columns[0].1, Span::test_data());
            let inner_list = list.as_list().unwrap();
            assert_eq!(inner_list.len(), 3);
            assert_eq!(inner_list[0].as_int().unwrap(), 1);
            assert_eq!(inner_list[1].as_int().unwrap(), 2);
            assert_eq!(inner_list[2].as_int().unwrap(), 3);
        }
    }

    #[test]
    fn from_parquet_should_convert_list_of_floats_to_nu_list() {
        let bytes = std::fs::read("tests/list_floats.parquet").unwrap();
        let reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
        for row in reader.get_row_iter(None).unwrap() {
            let columns = row.unwrap().into_columns();
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "floats");
            let list = convert_to_nu(&columns[0].1, Span::test_data());
            let inner_list = list.as_list().unwrap();
            assert_eq!(inner_list.len(), 2);
            assert_eq!(inner_list[0].as_float().unwrap(), 1.23);
            assert_eq!(inner_list[1].as_float().unwrap(), 3.456);
        }
    }

    #[test]
    fn from_parquet_should_convert_nested_struct_to_nu_record() {
        let bytes = std::fs::read("tests/nested_struct.parquet").unwrap();
        let reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
        for row in reader.get_row_iter(None).unwrap() {
            let columns = row.unwrap().into_columns();
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "person");
            let record = convert_to_nu(&columns[0].1, Span::test_data());
            let inner_record = record.as_record().unwrap();
            assert_eq!(inner_record.len(), 2);
            let first_name = inner_record
                .get_index(inner_record.index_of("first_name").unwrap())
                .unwrap();
            assert_eq!("first_name", first_name.0);
            assert_eq!("Greatest", first_name.1.as_str().unwrap());
            let last_name = inner_record
                .get_index(inner_record.index_of("last_name").unwrap())
                .unwrap();
            assert_eq!("last_name", last_name.0);
            assert_eq!("Ever", last_name.1.as_str().unwrap());
        }
    }

    #[test]
    fn from_parquet_should_convert_nested_struct_with_nested_struct_to_nu_record() {
        let bytes = std::fs::read("tests/nested_nested_struct.parquet").unwrap();
        let reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
        for row in reader.get_row_iter(None).unwrap() {
            let columns = row.unwrap().into_columns();
            assert_eq!(columns.len(), 1);
            assert_eq!(columns[0].0, "person");
            let record = convert_to_nu(&columns[0].1, Span::test_data());
            let inner_record = record.as_record().unwrap();
            assert_eq!(inner_record.len(), 3);
            let name = inner_record
                .get_index(inner_record.index_of("name").unwrap())
                .unwrap();
            assert_eq!("name", name.0);
            assert_eq!("Jason Bourne", name.1.as_str().unwrap());
            let age = inner_record
                .get_index(inner_record.index_of("age").unwrap())
                .unwrap();
            assert_eq!("age", age.0);
            assert_eq!(70, age.1.as_int().unwrap());
            let address = inner_record
                .get_index(inner_record.index_of("address").unwrap())
                .unwrap();
            assert_eq!("address", address.0);
            let inner_address = address.1.as_record().unwrap();
            assert_eq!(inner_address.len(), 4);
            let street = inner_address
                .get_index(inner_address.index_of("street").unwrap())
                .unwrap();
            assert_eq!("street", street.0);
            assert_eq!("123 Main St", street.1.as_str().unwrap());
        }
    }

    #[test]
    fn from_parquet_should_convert_list_of_nested_structs_to_nu_list() {
        let bytes = std::fs::read("tests/list_nested_nested_struct_list.parquet").unwrap();
        let reader = SerializedFileReader::new(Bytes::from(bytes)).unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 1);
        let mut iter = reader.get_row_iter(None).unwrap();
        let first_row = iter.next().unwrap().unwrap();
        let second_row = iter.next().unwrap().unwrap();

        let first_columns = first_row.into_columns();
        assert_eq!(first_columns.len(), 1);
        assert_eq!(first_columns[0].0, "person");
        let first_record = convert_to_nu(&first_columns[0].1, Span::test_data());
        let first_inner_record = first_record.as_record().unwrap();
        assert_eq!(first_inner_record.len(), 4);
        let movies = first_inner_record
            .get_index(first_inner_record.index_of("movies").unwrap())
            .unwrap()
            .1
            .as_list()
            .unwrap();
        assert_eq!(movies.len(), 3);
        assert_eq!("The Bourne Identity", movies[0].as_str().unwrap());

        let second_columns = second_row.into_columns();
        assert_eq!(second_columns.len(), 1);
        assert_eq!(second_columns[0].0, "person");
        let second_record = convert_to_nu(&second_columns[0].1, Span::test_data());
        let second_inner_record = second_record.as_record().unwrap();
        assert_eq!(second_inner_record.len(), 4);
        let movies = second_inner_record
            .get_index(second_inner_record.index_of("movies").unwrap())
            .unwrap()
            .1
            .as_list()
            .unwrap();
        assert_eq!(movies.len(), 3);
        assert_eq!("Rocky", movies[0].as_str().unwrap());
    }
}
