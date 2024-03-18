
//use crate::predicer;
use crate::input_data::InputData;
use crate::errors::ModelDataError;
use crate::errors::CustomError;
use serde_yaml;
use bincode;
use serde::{Serialize, Deserialize};
use std::error::Error;
use std::path::Path;
use std::fs::File;
use std::io::Read;
use std::process::{Command, Stdio};
use std::io::{BufWriter, Write};
use arrow::{array::{Int32Array, StringArray}, datatypes::{DataType, Field, Schema}, ipc::writer::StreamWriter, record_batch::RecordBatch};
use std::sync::Arc;
use std::io::Cursor;
use base64::{encode};
use std::env;


pub fn create_arrow_data_buffer() -> Result<Vec<u8>, Box<dyn Error>> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let id_values = vec![1, 2, 3, 4];
    let name_values = vec!["Alice", "Bob", "Cindy", "David"];
    let ids = Int32Array::from(id_values);
    let names = StringArray::from(name_values);

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)])?;

    let mut buffer: Vec<u8> = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buffer, &schema)?;
        writer.write(&batch)?;
        writer.finish()?;
    }

    Ok(buffer)
}


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

    // Adjusted to accept Vec<u8> directly and encode it
    pub fn send_data(&mut self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let encoded = encode(data); // Directly encode the Arrow binary data to base64
        writeln!(self.stdin, "data:{}", encoded)?;
        self.stdin.flush()?;
        Ok(())
    }

    pub fn terminate(&mut self) -> Result<(), Box<dyn Error>> {
        writeln!(self.stdin, "exit")?;
        self.stdin.flush()?;
        Ok(())
    }
}


// Assuming `input_data` is an instance of `InputData`
pub fn serialize_input_data(input_data: &InputData) -> Result<Vec<u8>, Box<dyn Error>> {
    let serialized_data = bincode::serialize(input_data)?;
    Ok(serialized_data)
}

pub fn read_input_data_from_yaml<P: AsRef<Path>>(path: P) -> Result<InputData, Box<dyn Error>> {
    // Open the file
    let mut file = File::open(path)?;
    
    // Read the contents into a string
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;
    
    // Deserialize the YAML string into InputData
    let input_data: InputData = serde_yaml::from_str(&contents)?;
    
    Ok(input_data)
}

