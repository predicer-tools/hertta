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

zmq_context = Context()

println("Connecting to server...")
socket = Socket(zmq_context, REQ)
ZMQ.connect(socket, "tcp://localhost:5555")
println("Sending request...")
ZMQ.send(socket, "Hello")

# Initialize a list to store the received data tables temporarily
received_tables = []

# Keywords list
keywords = [
    "setup",
    "nodes",
    "processes",
    "groups",
    "process_topology",
    "node_history",
    "node_delay",
    "node_diffusion",
    "inflow_blocks",
    "markets",
    "reserve_realisation",
    "reserve_activation_price",
    "scenarios",
    "efficiencies",
    "reserve_type",
    "risk",
    "cap_ts",
    "gen_constraint",
    "constraints",
    "bid_slots",
    "cf",
    "inflow",
    "market_prices",
    "price",
    "eff_ts",
    "fixed_ts",
    "balance_prices"
]

# Loop to receive multiple tables
while true
    data = ZMQ.recv(socket)
    if isempty(data) || String(data) == "END"  # Check for end signal
        break
    end
    table = Arrow.Table(IOBuffer(data, read=true, write=false))
    df = DataFrame(table)  # Convert Arrow table to DataFrame
    push!(received_tables, df)
    println("Received DataFrame:")
    println(df)  # Print the DataFrame
    # Send acknowledgment
    ZMQ.send(socket, "ACK")
end

# Check if the number of received tables matches the number of keywords
if length(received_tables) == length(keywords)
    # Initialize a dictionary to store the data tables with their keywords
    data_tables = Dict{String, DataFrame}()
    
    for (i, keyword) in enumerate(keywords)
        data_tables[keyword] = received_tables[i]
    end

    println("All DataFrames received and paired with keywords.")
else
    println("Mismatch between number of received DataFrames and keywords.")
end

# Call the test function from Predicer
Predicer.test_function()

# Example: Access a DataFrame by its name
#println("Accessing 'setup' DataFrame:")
#println(data_tables["setup"])

# Here you can use the data_tables dictionary for further processing

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
