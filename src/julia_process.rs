use arrow::array::{StringArray, Float64Array, Int32Array, BooleanArray, ArrayRef, Array};
use arrow::{datatypes::{DataType, Field, Schema}, error::ArrowError, record_batch::RecordBatch};
use std::sync::Arc;
use arrow::error::Result as ArrowResult;
use crate::input_data;
use std::collections::HashMap;
use std::error::Error;
use arrow::ipc::writer::StreamWriter;
use base64::{encode};
use std::io::{BufWriter, Write};
use std::process::{Command, Stdio};
use std::env;

pub struct JuliaProcess {
    stdin: BufWriter<std::process::ChildStdin>,
}

impl JuliaProcess {
    pub fn new(julia_script_path: &str) -> Result<Self, Box<dyn Error>> {
        let current_dir = env::current_dir()?; // Get the current working directory
        let script_path = current_dir.join(julia_script_path); // Build the full script path

        let mut child = Command::new("julia")
            .arg(script_path.to_str().unwrap()) // Convert PathBuf to str for .arg()
            .current_dir(current_dir) // Optionally set the current directory
            .stdin(Stdio::piped())
            .spawn()?;

        let stdin = child.stdin.take().ok_or("Failed to open stdin")?;
        Ok(JuliaProcess {
            stdin: BufWriter::new(stdin),
        })
    }

    pub fn send_data(&mut self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        // No need to encode as `data` is already expected to be a base64-encoded string
        self.stdin.write_all(&data)?;
        self.stdin.flush()?;
        Ok(())
    }

    pub fn terminate(&mut self) -> Result<(), Box<dyn Error>> {
        writeln!(self.stdin, "exit")?;
        self.stdin.flush()?;
        Ok(())
    }
}