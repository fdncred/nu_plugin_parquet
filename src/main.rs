mod from_parquet;

use nu_plugin::{
    serve_plugin, EngineInterface, EvaluatedCall, MsgPackSerializer, Plugin, PluginCommand,
    SimplePluginCommand,
};
use nu_protocol::{Category, Example, LabeledError, Signature, Type, Value};

pub struct ParquetPlugin;

impl Plugin for ParquetPlugin {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        vec![Box::new(FromParquet), Box::new(ToParquet)]
    }
}

struct FromParquet;

impl SimplePluginCommand for FromParquet {
    type Plugin = ParquetPlugin;

    fn name(&self) -> &str {
        "from parquet"
    }

    fn usage(&self) -> &str {
        "Convert from .parquet binary into table"
    }
    fn signature(&self) -> Signature {
        Signature::build(PluginCommand::name(self))
            .switch(
                "metadata",
                "Convert metadata from .parquet binary into table",
                Some('m'),
            )
            .allow_variants_without_examples(true)
            .input_output_types(vec![(Type::Binary, Type::Any)])
            .category(Category::Experimental)
            .filter()
    }

    fn examples(&self) -> Vec<Example> {
        vec![
            Example {
                description: "Convert from .parquet binary into table".into(),
                example: "open --raw file.parquet | from parquet".into(),
                result: None,
            },
            Example {
                description: "Convert from .parquet binary into table".into(),
                example: "open file.parquet".into(),
                result: None,
            },
            Example {
                description: "Convert metadata from .parquet binary into table".into(),
                example: "open -r file.parquet | from parquet --metadata".into(),
                result: None,
            },
        ]
    }

    fn run(
        &self,
        _plugin: &ParquetPlugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let span = input.span();
        match input {
            Value::Binary { val, .. } => match call.has_flag("metadata")? {
                true => crate::from_parquet::metadata_from_parquet_bytes(val.clone(), span),
                false => crate::from_parquet::from_parquet_bytes(val.clone(), span),
            },
            v => {
                return Err(LabeledError::new(format!(
                    "requires binary input, got {}",
                    v.get_type()
                ))
                .with_label("Expected binary from pipeline", call.head))
            }
        }
    }
}

struct ToParquet;

impl SimplePluginCommand for ToParquet {
    type Plugin = ParquetPlugin;

    fn name(&self) -> &str {
        "to parquet".into()
    }

    fn usage(&self) -> &str {
        "Convert from table to .parquet binary"
    }

    fn signature(&self) -> Signature {
        Signature::build(PluginCommand::name(self))
            .allow_variants_without_examples(true)
            .input_output_types(vec![(Type::Any, Type::Binary)])
            .category(Category::Experimental)
            .filter()
    }

    fn examples(&self) -> Vec<Example> {
        vec![
            Example {
                description: "Convert from table into parquet binary".into(),
                example: "[{a:1}, {a: 2}] | to parquet".into(),
                result: None,
            },
            Example {
                description: "Store as parquet file".into(),
                example: "[{a:1}, {a: 2}] | save file.parquet".into(),
                result: None,
            },
        ]
    }

    fn run(
        &self,
        _plugin: &Self::Plugin,
        _engine: &EngineInterface,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        let span = input.span();
        match input {
            Value::List { vals, .. } => crate::from_parquet::to_parquet_bytes(vals, span),
            v => {
                return Err(LabeledError::new(format!(
                    "requires table input, got {}",
                    v.get_type()
                ))
                .with_label("Expected table from pipeline", call.head))
            }
        }
    }
}

fn main() {
    serve_plugin(&mut ParquetPlugin, MsgPackSerializer {});
}
