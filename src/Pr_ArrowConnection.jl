import Pkg

# Activate the project environment
Pkg.activate("C:\\users\\enessi\\Documents\\hertta")

# Instantiate all dependencies
Pkg.instantiate()

println("Running Julia script with all dependencies activated and instantiated.")

using Arrow
using DataFrames
using ZMQ
using OrderedCollections
using TimeZones
using Dates

# Navigate to Predicer directory and activate the environment
cd("C:\\users\\enessi\\Documents\\hertta\\Predicer")

Pkg.activate(".")
Pkg.instantiate()

# Use the Predicer module
using Predicer

zmq_context = Context()

function main()
    println("Connecting to server...")
    socket = Socket(zmq_context, REQ)
    ZMQ.connect(socket, "tcp://localhost:5555")
    println("Sending request...")
    ZMQ.send(socket, "Hello")

    # Keywords list
    keywords = [
        "temps",
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

    # Define sheetname categories
    sheetnames_system = [
        "setup", "nodes", "processes", "groups", "process_topology", 
        "node_history", "node_delay", "node_diffusion", "inflow_blocks", 
        "markets", "scenarios", "efficiencies", "reserve_type", "risk", 
        "cap_ts", "gen_constraint", "constraints", "bid_slots"
    ]
    sheetnames_timeseries = [
        "cf", "inflow", "market_prices", "reserve_realisation", 
        "reserve_activation_price", "price", "eff_ts", "fixed_ts", 
        "balance_prices"
    ]

    # Initialize data structures to store the data tables
    data_dict = OrderedDict{String, DataFrame}()

    # Loop to receive multiple tables
    while true
        println("Waiting to receive data...")
        data = ZMQ.recv(socket)
        println("Received data, checking if it is END signal...")
        if isempty(data) || String(data) == "END"  # Check for end signal
            println("Received END signal or empty data, breaking the loop.")
            break
        end
        println("Processing received data...")
        table = Arrow.Table(IOBuffer(data, read=true, write=false))
        df = DataFrame(table)  # Convert Arrow table to DataFrame
        push!(data_dict, (keywords[length(data_dict) + 1] => df))
        println("Received DataFrame for keyword $(keywords[length(data_dict)]):")
        println(df)  # Print the DataFrame
        # Send acknowledgment
        println("Sending acknowledgment for keyword $(keywords[length(data_dict)])...")
        ZMQ.send(socket, "ACK")
    end

    # Function to convert 't' column to DateTime in a DataFrame
    function convert_t_to_datetime!(df::DataFrame)
        if hasproperty(df, :t)
            if eltype(df.t) == String
                df.t = DateTime.(df.t, "yyyy-mm-ddTHH:MM:SS.s")  # Adjust format if necessary
            elseif eltype(df.t) == Union{Missing, String}
                df.t = coalesce.(DateTime.(df.t, "yyyy-mm-ddTHH:MM:SS.s"), missing)  # Convert missing values if present
            end
            println("Converted 't' column in DataFrame to DateTime.")
        else
            println("No 't' column found in DataFrame.")
        end
    end

    # Separate temps and other dataframes
    global temporals = String[]
    global system_data = OrderedDict()
    global timeseries_data = OrderedDict()
    timeseries_data["scenarios"] = OrderedDict()

    # Iterate over data_dict and populate system_data and timeseries_data
    for (key, df) in data_dict
        if key == "temps"
            println("Converting temps DataFrame to temporals vector.")
            temporals = collect(df.t)
            # Add +00:00 to each timestamp in temporals
            temporals = [t * "+00:00" for t in temporals]
            println("Converted temporals data to String with +00:00: ", temporals)
        elseif key in sheetnames_system
            system_data[key] = df
            println("Added to system_data: ", key)
            
            # Convert 't' column to DateTime in system_data
            convert_t_to_datetime!(system_data[key])
        elseif key in sheetnames_timeseries
            timeseries_data[key] = df
            println("Added to timeseries_data: ", key)
            
            # Convert 't' column to DateTime in timeseries_data
            convert_t_to_datetime!(timeseries_data[key])
        else
            println("Unknown keyword: ", key)
        end
    end

    println("All DataFrames received and paired with keywords.")

    # Print system_data
    println("system_data:")
    for (key, df) in system_data
        println("DataFrame for sheet: $key")
        println(df)
        println()
    end

    # Print timeseries_data
    println("timeseries_data:")
    for (key, df) in timeseries_data
        println("DataFrame for sheet: $key")
        println(df)
        println()
    end

    # Print temps
    println("temps:")
    println(temporals)
    println()

    input_data = Predicer.compile_input_data(system_data, timeseries_data, temporals)
    mc, input_data = Predicer.generate_model(input_data)
    Predicer.solve_model(mc)
    result_dataframes = Predicer.get_all_result_dataframes(mc, input_data)
    v_flow_df = result_dataframes["v_flow"]
    show(v_flow_df, allcols=true, allrows=true)


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
end

main()
