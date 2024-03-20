
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
use arrow::{array::{ArrayRef, Int32Array, StringArray}, datatypes::{DataType, Field, Schema}, ipc::writer::StreamWriter, record_batch::RecordBatch};
use std::sync::Arc;
use std::io::Cursor;
use base64::{encode};
use std::env;
use std::collections::HashMap;
use serde_arrow;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestNode {
    pub name: String,
    pub value: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct TestInputData {
    pub nodes: HashMap<String, TestNode>,
    pub metadata: String,
}

pub fn create_example_input_data() -> TestInputData {
    let nodes = HashMap::from([
        ("node1".to_string(), TestNode { name: "Node One".to_string(), value: 100 }),
        ("node2".to_string(), TestNode { name: "Node Two".to_string(), value: 200 }),
    ]);
    TestInputData {
        nodes,
        metadata: "Example Metadata".to_string(),
    }
}

/* 
pub fn convert_test_input_data_to_record_batch(input_data: &TestInputData) -> Result<RecordBatch, Box<dyn std::error::Error>> {
    let node_keys: Vec<String> = input_data.nodes.keys().cloned().collect();
    let node_names: Vec<String> = input_data.nodes.values().map(|v| v.name.clone()).collect();
    let node_values: Vec<i32> = input_data.nodes.values().map(|v| v.value).collect();

    // Metadata is repeated for each node to keep the table size consistent
    let metadata: Vec<String> = vec![input_data.metadata.clone(); input_data.nodes.len()];

    let node_keys_array: ArrayRef = Arc::new(StringArray::from(node_keys));
    let node_names_array: ArrayRef = Arc::new(StringArray::from(node_names));
    let node_values_array: ArrayRef = Arc::new(Int32Array::from(node_values));
    let metadata_array: ArrayRef = Arc::new(StringArray::from(metadata));

    let fields = vec![
        Field::new("node_key", DataType::Utf8, false),
        Field::new("node_name", DataType::Utf8, false),
        Field::new("node_value", DataType::Int32, false),
        Field::new("metadata", DataType::Utf8, false),
    ];
    let schema = Arc::new(Schema::new(fields));

    let record_batch = RecordBatch::try_new(schema, vec![node_keys_array, node_names_array, node_values_array, metadata_array])?;

    Ok(record_batch)
}
*/

pub fn serialize_record_batch_to_vec(batch: &RecordBatch) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut buf: Vec<u8> = Vec::new();
    {
        // Place the writer in a scoped block
        let mut writer = StreamWriter::try_new(&mut buf, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
        // `writer` gets dropped here, at the end of the scoped block, releasing the borrow on `buf`
    }
    // Now that the writer is dropped, it's safe to move `buf`
    println!("Buffer size after serialization: {}", buf.len());
    Ok(buf)
}

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

