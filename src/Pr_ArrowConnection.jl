import Pkg

# Activate the project environment
Pkg.activate("C:\\users\\enessi\\Documents\\hertta")

# Instantiate all dependencies
Pkg.instantiate()

println("Running Julia script with all dependencies activated and instantiated.")

using Arrow
using DataFrames
using ZMQ

# Navigate to Predicer directory and activate the environment
cd("C:\\users\\enessi\\Documents\\hertta\\Predicer")
Pkg.activate(".")
Pkg.instantiate()

# Use the Predicer module
using Predicer

# Call the test function from Predicer
Predicer.test_function()

zmq_context = Context()

println("Connecting to server...")
socket = Socket(zmq_context, REQ)
ZMQ.connect(socket, "tcp://localhost:5555")
println("Sending request...")
ZMQ.send(socket, "Hello")

# Loop to receive multiple tables
while true
    data = ZMQ.recv(socket)
    if isempty(data) || String(data) == "END"  # Check for end signal
        break
    end
    table = Arrow.Table(IOBuffer(data, read=true, write=false))
    df = DataFrame(table)  # Convert Arrow table to DataFrame
    println("Received DataFrame:")
    println(df)  # Print the DataFrame
    # Send acknowledgment
    ZMQ.send(socket, "ACK")
end

# Send a random DataFrame back to Rust
push_port = 5237
endpoint = "tcp://localhost:$push_port"
push_socket = Socket(zmq_context, PUSH)
ZMQ.bind(push_socket, "tcp://*:$push_port")
ZMQ.send(socket, "Take this! $(endpoint)")
ready_confirmation = String(ZMQ.recv(socket))
if ready_confirmation == "ready to receive"
    out_data = DataFrame("customer age" => [15, 20, 25],
                         "first name" => ["Rohit", "Rahul", "Akshat"])
    buffer = IOBuffer(read=true, write=true)
    Arrow.write(buffer, out_data)
    ZMQ.send(push_socket, take!(buffer))
else
    println("receiver not ready to receive $ready_confirmation")
end
ZMQ.close(push_socket)

# Send Quit command to the server
ZMQ.send(socket, "Quit")

ZMQ.close(socket)
ZMQ.close(zmq_context)
