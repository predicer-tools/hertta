import Pkg

predicer_project_path = ARGS[1]
zmq_port = ARGS[2]

Pkg.instantiate()
Pkg.add("Arrow")
Pkg.add("ZMQ")
Pkg.add("OrderedCollections")
Pkg.add("TimeZones")
println("Running Julia script with all dependencies activated and instantiated.")

using Arrow
using DataFrames
using ZMQ
using OrderedCollections
using TimeZones
using Dates

cd(predicer_project_path)
Pkg.activate(".")
Pkg.instantiate()
using Predicer

zmq_context = Context()

function send_acknowledgement(socket::Socket)
    ZMQ.send(socket, "Ok")
end

function receive_acknowledgement(socket::Socket)
    acknowledgement = String(ZMQ.recv(socket))
    if acknowledgement != "Ok"
        eprintln("unknown acknowledgement $acknowledgement")
    end
end

function receive_data(socket::Socket)
    data_dict = OrderedDict{String, DataFrame}()
    while true
        data = ZMQ.recv(socket)
        if isempty(data)
            break
        end
        message = String(data)
        if message == "End"
            break
        end
        send_acknowledgement(socket)
        message, key = split(message)
        if message != "Receive"
            eprintln!("expected 'Receive', got '$message' from Hertta")
            continue
        end
        data = ZMQ.recv(socket)
        send_acknowledgement(socket)
        table = Arrow.Table(IOBuffer(data, read=true, write=false))
        df = DataFrame(table)
        push!(data_dict, (key => df))
    end
    data_dict
end

function convert_t_to_datetime!(df::DataFrame)
    if hasproperty(df, :t)
        println("Found 't' column with values: ", df.t)
        if eltype(df.t) == String
            try
                df.t = DateTime.(df.t, "yyyy-mm-ddTHH:MM:SS.s")  # Adjust format if necessary
                println("Converted 't' column to DateTime successfully.")
            catch e
                println("Error converting 't' column: ", e)
            end
        elseif eltype(df.t) == Union{Missing, String}
            try
                df.t = coalesce.(DateTime.(df.t, "yyyy-mm-ddTHH:MM:SS.s"), missing)  # Convert missing values if present
                println("Converted 't' column to DateTime successfully with missing values.")
            catch e
                println("Error converting 't' column with missing values: ", e)
            end
        end
    else
        println("No 't' column found in DataFrame.")
    end
end

function split_data_to_system_and_time_series(data::OrderedDict{String, DataFrame})
    sheetnames_system = Set([
        "setup", "nodes", "processes", "groups", "process_topology",
        "node_history", "node_delay", "node_diffusion", "inflow_blocks",
        "markets", "scenarios", "efficiencies", "reserve_type", "risk",
        "cap_ts", "gen_constraint", "constraints", "bid_slots"
    ])
    sheetnames_timeseries = Set([
        "cf", "inflow", "market_prices", "reserve_realisation",
        "reserve_activation_price", "price", "eff_ts", "fixed_ts",
        "balance_prices"
    ])
    system_data = OrderedDict()
    timeseries_data = OrderedDict()
    for (key, df) in data
        convert_t_to_datetime!(df)
        if key in sheetnames_system
            delete!(sheetnames_system, key)
            system_data[key] = df
            println("Added to system_data: ", key)
        elseif key in sheetnames_timeseries
            delete!(sheetnames_timeseries, key)
            timeseries_data[key] = df
            println("Added to timeseries_data: ", key)
        else
            eprintln("Unknown keyword: ", key)
        end
    end
    if !isempty(sheetnames_system) || !isempty(sheetnames_timeseries)
        eprintln("did not receive all data frames")
    end
    system_data, timeseries_data
end

function send_results(socket::Socket, results::Dict{Any, Any})
    ZMQ.send(socket, "Ready to receive?")
    receive_acknowledgement(socket)
    for (type, df) in results
        ZMQ.send(socket, "Receive $(type)")
        df = results[type]
        buffer = IOBuffer()
        Arrow.write(buffer, df)
        receive_acknowledgement(socket)
        ZMQ.send(socket, take!(buffer))
        receive_acknowledgement(socket)
    end
    ZMQ.send(socket, "End")
    receive_acknowledgement(socket)
end

function main()
    println("Connecting to server...")
    socket = Socket(zmq_context, REQ)
    ZMQ.connect(socket, "tcp://localhost:$zmq_port")
    ZMQ.send(socket, "Hello")
    data_dict = receive_data(socket)
    println("All data received.")
    temporals = collect(pop!(data_dict, "temps").t)
    (system_data, timeseries_data) = split_data_to_system_and_time_series(data_dict)
    input_data = Predicer.compile_input_data(system_data, timeseries_data, temporals)
    mc, input_data = Predicer.generate_model(input_data)
    Predicer.solve_model(mc)
    result_dataframes = Predicer.get_all_result_dataframes(mc, input_data)
    send_results(socket, result_dataframes)
    ZMQ.close(socket)
    ZMQ.close(zmq_context)
end

main()
