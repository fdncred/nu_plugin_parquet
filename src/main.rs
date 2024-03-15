mod from_parquet;

use nu_plugin::{
    serve_plugin, EngineInterface, EvaluatedCall, LabeledError, MsgPackSerializer, Plugin,
    PluginCommand, SimplePluginCommand,
};
use nu_protocol::{Category, PluginExample, PluginSignature, Type, Value};

pub struct FromParquetPlugin;

impl Plugin for FromParquetPlugin {
    fn commands(&self) -> Vec<Box<dyn PluginCommand<Plugin = Self>>> {
        vec![Box::new(FromParquet)]
    }
}
struct FromParquet;

impl FromParquet {
    fn new() -> Self {
        Self {}
    }
}

impl SimplePluginCommand for FromParquet {
    type Plugin = FromParquetPlugin;

    fn signature(&self) -> PluginSignature {
        PluginSignature::build("from parquet")
            .usage("Convert from .parquet binary into table")
            .switch(
                "metadata",
                "Convert metadata from .parquet binary into table",
                Some('m'),
            )
            .allow_variants_without_examples(true)
            .input_output_types(vec![(Type::Binary, Type::Any)])
            .category(Category::Experimental)
            .filter()
            .plugin_examples(vec![
                PluginExample {
                    description: "Convert from .parquet binary into table".into(),
                    example: "open --raw file.parquet | from parquet".into(),
                    result: None,
                },
                PluginExample {
                    description: "Convert from .parquet binary into table".into(),
                    example: "open file.parquet".into(),
                    result: None,
                },
                PluginExample {
                    description: "Convert metadata from .parquet binary into table".into(),
                    example: "open -r file.parquet | from parquet --metadata".into(),
                    result: None,
                },
            ])
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
            v => Err(LabeledError {
                label: "Expected binary from pipeline".into(),
                msg: format!("requires binary input, got {}", v.get_type()),
                span: Some(call.head),
            }),
        }
    }
}

fn main() {
    serve_plugin(&mut FromParquetPlugin, MsgPackSerializer {});
}
