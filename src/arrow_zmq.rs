use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time;
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use std::env;
use crate::errors::FileReadError;
use std::process::ExitStatus;
use std::time::Duration;



pub fn run_server() -> std::thread::JoinHandle<()> {
    let zmq_context = Arc::new(zmq::Context::new());
    let context_clone = Arc::clone(&zmq_context);

    let handle = thread::spawn(move || {
        let (schema, batch, buffer_ref) = prepare_arrow_data();
        let responder = context_clone.socket(zmq::REP).unwrap();
        assert!(responder.bind("tcp://*:5555").is_ok());

        let mut is_running = true;
        let receive_flags = 0;
        let send_flags = 0;

        while is_running {
            let request_result = responder.recv_string(receive_flags);
            match request_result {
                Ok(inner_result) => {
                    match inner_result {
                        Ok(command) => {
                            is_running = handle_command(command, &responder, send_flags, &context_clone, &buffer_ref);
                        }
                        Err(_) => println!("Received absolute gibberish"),
                    }
                }
                Err(_) => {
                    println!("Failed to receive data");
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    });

    handle
}

pub fn prepare_arrow_data() -> (Schema, RecordBatch, Vec<u8>) {
    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false)
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(id_array)]
    ).unwrap();

    let buffer: Vec<u8> = Vec::new();
    let mut stream_writer = StreamWriter::try_new(buffer, &schema).unwrap();
    stream_writer.write(&batch).expect("failed to write batch record");
    stream_writer.finish().expect("failed to wrap up writing batch record");
    let buffer_ref = stream_writer.get_ref().clone();

    (schema, batch, buffer_ref)
}

pub fn handle_command(command: String, responder: &zmq::Socket, send_flags: i32, zmq_context: &zmq::Context, buffer_ref: &Vec<u8>) -> bool {
    if command == "Hello" {
        println!("Received Hello");
        responder.send(&buffer_ref, send_flags).unwrap();
    } else if command.starts_with("Take this!") {
        let endpoint = command.strip_prefix("Take this! ").expect("cannot decipher endpoint");
        responder.send("ready to receive", send_flags).expect("failed to confirm readiness for input");
        pull_data(endpoint, &zmq_context);
    } else if command == "Quit" {
        println!("Received request to quit");
        return false;
    } else {
        println!("Received unknown command {}", command);
    }
    true
}

pub fn pull_data(endpoint: &str, zmq_context: &zmq::Context) {
    let receiver = zmq_context.socket(zmq::PULL).unwrap();
    assert!(receiver.connect(endpoint).is_ok());
    let flags = 0;
    let pull_result = receiver.recv_bytes(flags);
    match pull_result {
        Ok(bytes) => {
            let reader = StreamReader::try_new(bytes.as_slice(), None).expect("Failed to construct Arrow reader");
            for record_batch_result in reader {
                let record_batch = record_batch_result.expect("Failed to read record batch");
                println!("Received record batch, columns {} rows {}", record_batch.num_columns(), record_batch.num_rows());
            }
        }
        Err(_) => println!("Failed to pull data"),
    }
}

pub fn start_julia_local() -> Result<ExitStatus, std::io::Error> {
    let push_port = find_available_port();
    std::env::set_var("PUSH_PORT", push_port.to_string());

    let mut julia_command = Command::new("C:\\Users\\enessi\\AppData\\Local\\Microsoft\\WindowsApps\\julia.exe");
    julia_command.arg("--project=C:\\users\\enessi\\Documents\\hertta");
    julia_command.arg("C:\\users\\enessi\\Documents\\hertta\\src\\Pr_ArrowConnection.jl");
    julia_command.status()
}

pub fn find_available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("Failed to bind to address");
    listener.local_addr().unwrap().port()
}

pub fn start_julia_process() {
    let julia_path = env::var("JULIA_PATH").expect("JULIA_PATH environment variable not set");
    let project_path = env::var("PROJECT_PATH").expect("PROJECT_PATH environment variable not set");
    let script_path = env::var("SCRIPT_PATH").expect("SCRIPT_PATH environment variable not set");

    let mut julia_command = Command::new(julia_path);
    julia_command.arg(format!("--project={}", project_path));
    julia_command.arg(script_path);
    julia_command.status().expect("Julia process failed to execute");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use zmq;
    use std::net::TcpListener;

    // Function to find an available port
    fn find_available_port() -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Failed to bind to address");
        listener.local_addr().unwrap().port()
    }

    #[test]
    fn test_server() {
        // Start the server in a separate thread
        let server_handle = run_server();
        
        // Give the server some time to start
        thread::sleep(Duration::from_secs(1));
        
        // Context for the client
        let zmq_context = zmq::Context::new();
        let requester = zmq_context.socket(zmq::REQ).unwrap();
        assert!(requester.connect("tcp://localhost:5555").is_ok());

        // Test "Hello" command
        requester.send("Hello", 0).expect("Failed to send Hello");
        let reply = requester.recv_bytes(0).expect("Failed to receive reply");
        assert!(!reply.is_empty(), "Received empty reply for Hello");

        // Find an available port for the push socket
        let push_port = find_available_port();
        println!("Selected push port: {}", push_port); // Debug print
        let endpoint = format!("tcp://127.0.0.1:{}", push_port);  // Change to use 127.0.0.1
        let push_socket = zmq_context.socket(zmq::PUSH).unwrap();

        // Attempt to bind and print any errors
        match push_socket.bind(&endpoint) {
            Ok(_) => println!("Successfully bound to {}", endpoint),
            Err(e) => panic!("Failed to bind push_socket to {}: {:?}", endpoint, e),
        }

        requester.send(&format!("Take this! {}", endpoint), 0).expect("Failed to send Take this!");
        let ready_confirmation = requester.recv_string(0).expect("Failed to receive readiness confirmation");
        assert_eq!(ready_confirmation.unwrap(), "ready to receive");

        // Prepare data using prepare_arrow_data function
        let (_, _, buffer) = prepare_arrow_data();

        // Send data
        push_socket.send(&buffer, 0).expect("Failed to send data");

        // Test "Quit" command
        requester.send("Quit", 0).expect("Failed to send Quit");
        thread::sleep(Duration::from_secs(1));

        // Stop the server
        server_handle.join().expect("Failed to join server thread");
    }

    #[test]
    fn test_start_julia_local() {
        match start_julia_local() {
            Ok(status) => {
                assert!(status.success(), "Julia process did not exit successfully");
            },
            Err(e) => {
                panic!("Failed to start Julia: {:?}", e);
            }
        }
    }
}
