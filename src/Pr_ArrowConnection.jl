import Pkg

# Activate the project environment
Pkg.activate("C:\\users\\enessi\\Documents\\hertta")

# Instantiate all dependencies
Pkg.instantiate()

println("Running Julia script with all dependencies activated and instantiated.")

using Arrow
using DataFrames
using ZMQ

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
    println("Received table:")
    println(table)
    # Send acknowledgment
    ZMQ.send(socket, "ACK")
end

# Send Quit command to the server
ZMQ.send(socket, "Quit")

ZMQ.close(socket)
ZMQ.close(zmq_context)
