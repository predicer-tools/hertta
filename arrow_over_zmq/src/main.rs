use std::process::Command;
use std::sync::Arc;
use std::thread;
use std::time;
use arrow::array::Int32Array;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;

fn main() {
    let thread_join_handle = thread::Builder::new().name("Predicer data server".to_string()).spawn(move || {
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
        let buffer_ref = stream_writer.get_ref();
        let zmq_context = zmq::Context::new();
        let responder = zmq_context.socket(zmq::REP).unwrap();
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
                            if command == "Hello" {
                                println!("Received Hello");
                                responder.send(buffer_ref, send_flags).unwrap();
                            }
                            else if command.starts_with("Take this!") {
                                let endpoint = command.strip_prefix("Take this! ").expect("cannot dechipher endpoint");
                                responder.send("ready to receive", send_flags).expect("failed to confirm readiness for input");
                                pull_data(endpoint, &zmq_context);
                            }
                            else if command == "Quit" {
                                println!("Received request to quit");
                                is_running = false;
                            }
                            else {
                                println!("Received unknown command {command}");
                            }
                        }
                        Err(_) => println!("Received absolute gibberish"),
                    }
                }
                Err(_) => {
                    println!("Failed to receive data");
                    thread::sleep(time::Duration::from_secs(1));
                }
            }
        }
    }).expect("failed to start server thread");
    let mut julia_command = Command::new("C:\\users\\ajsanttij\\AppData\\Local\\julias\\julia-1.10\\bin\\julia.exe");
    julia_command.arg("--project=C:\\users\\ajsanttij\\sources\\rust_projects\\arrow_over_zmq");
    julia_command.arg("C:\\users\\ajsanttij\\sources\\rust_projects\\arrow_over_zmq\\ReceiveArrow.jl");
    julia_command.status().expect("Julia process failed to execute");
    let _ = thread_join_handle.join().expect("failed to join with server thread");
}


fn pull_data(endpoint: &str, zmq_context: &zmq::Context) {
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
        Err(_) => println!("Failed to pull data")
    }
}
