use base64::encode;
use std::error::Error;
use std::io::{BufWriter, Write, Read, Error as IoError};
use std::net::TcpStream;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::thread::sleep;

const PROJECT_DIR: &str = "Predicer/src";

pub struct JuliaProcess {
    stdin: BufWriter<std::process::ChildStdin>,
    child: std::process::Child,
    port: u16,
}

impl JuliaProcess {
    pub fn new(julia_script: &str, port: u16) -> Result<Self, Box<dyn Error>> {
        // Construct the full path to the Julia script
        let current_dir = std::env::current_dir()?; // Get the current working directory
        let script_path = PathBuf::from(PROJECT_DIR).join(julia_script);

        println!("Running Julia script from: {}", script_path.display());

        let mut child = Command::new("julia")
            .arg(script_path.to_str().unwrap()) // Convert PathBuf to str for .arg()
            .arg(port.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()?;

        let stdin = child.stdin.take().ok_or("Failed to open stdin")?;
        Ok(JuliaProcess {
            stdin: BufWriter::new(stdin),
            child,
            port,
        })
    }

    pub fn send_data(&mut self, data: Vec<u8>) -> Result<(), Box<dyn Error>> {
        let server_address = format!("127.0.0.1:{}", self.port);
        let mut stream = TcpStream::connect(&server_address)?;

        // Base64 encode the data
        let encoded_data = encode(data);

        // Send the data
        stream.write_all(format!("data:{}", encoded_data).as_bytes())?;
        stream.flush()?;
        stream.shutdown(std::net::Shutdown::Both)?;

        Ok(())
    }

    pub fn wait_for_server_ready(&self, timeout: Duration) -> Result<(), Box<dyn Error>> {
        let server_address = format!("127.0.0.1:{}", self.port);
        let start_time = Instant::now();

        while start_time.elapsed() < timeout {
            match TcpStream::connect(&server_address) {
                Ok(mut stream) => {
                    let mut response = String::new();
                    let mut buffer = [0; 1024];
                    while let Ok(count) = stream.read(&mut buffer) {
                        if count == 0 {
                            break; // End of stream
                        }
                        response.push_str(&String::from_utf8_lossy(&buffer[..count]));
                        if response.contains("\n") {
                            break; // We expect a "ready\n" signal and then break
                        }
                    }

                    if response.trim() == "ready" {
                        return Ok(());
                    }
                }
                Err(e) => {
                    println!("Connection attempt failed, retrying... Error: {}", e);
                    sleep(Duration::from_secs(1)); // Sleep for a second before retrying
                }
            }
        }

        Err("Timeout waiting for server to become ready".into())
    }

    pub fn terminate(&mut self) -> Result<(), Box<dyn Error>> {
        self.child.kill()?;
        Ok(())
    }
}