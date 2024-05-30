import Pkg

# Activate the project environment
Pkg.activate("C:\\users\\enessi\\Documents\\hertta")

# Instantiate all dependencies
Pkg.instantiate()

# Your existing Julia code
println("Running Julia script with all dependencies activated and instantiated.")

using Arrow
using DataFrames
using ZMQ

zmq_context = Context()

println("Connecting to server...")
socket = Socket(zmq_context, REQ)
ZMQ.connect(socket, "tcp://127.0.0.1:5555")  # Change to use 127.0.0.1
println("Sending request...")
ZMQ.send(socket, "Hello")
table = Arrow.Table(IOBuffer(ZMQ.recv(socket), read=true, write=false))
println(table)

push_port = ENV["PUSH_PORT"]
println("Using push port: $push_port") # Debug print
endpoint = "tcp://127.0.0.1:$push_port"  # Change to use 127.0.0.1
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

ZMQ.send(socket, "Quit")

ZMQ.close(socket)
ZMQ.close(zmq_context)
