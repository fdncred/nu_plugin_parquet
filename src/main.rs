mod from_parquet;

use nu_plugin::{serve_plugin, Plugin, JsonSerializer, EvaluatedCall, LabeledError};
use nu_protocol::{Signature, Value};

struct FromParquet;

impl FromParquet {
    fn new() -> Self {
        Self {}
    }
}

impl Plugin for FromParquet {
    fn signature(&self) -> Vec<Signature> {
        vec![
            Signature::build("from parquet")
            .usage("Convert from .parquet binary into table")
            .filter()
        ]
    }

    fn run(
        &mut self, 
        name: &str,
        call: &EvaluatedCall,
        input: &Value,
    ) -> Result<Value, LabeledError> {
        assert_eq!(name, "from parquet");
        match input {
            Value::Binary { val, span } => {
                Ok(crate::from_parquet::from_parquet_bytes(val.clone(), span.clone()))
            }
            v => {
                return Err(LabeledError {
                    label: "Expected binary from pipeline".into(),
                    msg: format!("requires binary input, got {}", v.get_type()),
                    span: Some(call.head),
                });
            }
        }
    }
}

fn main() {
    serve_plugin(&mut FromParquet::new(), JsonSerializer);
}
