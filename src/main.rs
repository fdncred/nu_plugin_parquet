mod from_parquet;

use nu_plugin::{
    serve_plugin, EngineInterface, EvaluatedCall, MsgPackSerializer, Plugin, PluginCommand,
    SimplePluginCommand,
};
use nu_protocol::{Category, Example, LabeledError, Signature, Type, Value};

pub struct FromParquetPlugin;

impl Plugin for FromParquetPlugin {
    fn version(&self) -> String {
        env!("CARGO_PKG_VERSION").into()
    }

    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        vec![Box::new(FromParquet)]
    }
}

struct FromParquet;

impl SimplePluginCommand for FromParquet {
    type Plugin = FromParquetPlugin;

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
        _plugin: &FromParquetPlugin,
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

fn main() {
    serve_plugin(&mut FromParquetPlugin, MsgPackSerializer {});
}
