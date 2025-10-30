import Pkg

predicer_project_path = ARGS[1]
zmq_port = ARGS[2]

Pkg.instantiate()
Pkg.add("Arrow")
Pkg.add("DataFrames")
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
time_stamp_format = dateformat"yyyy-mm-ddTHH:MM:SS.s"

function send_acknowledgement(socket::Socket)
    ZMQ.send(socket, "Ok")
end

function receive_acknowledgement(socket::Socket)
    acknowledgement = String(ZMQ.recv(socket))
    if acknowledgement != "Ok"
        println(stderr, "unknown acknowledgement $acknowledgement")
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
        elseif message == "Abort"
            println(stderr, "aborted")
            exit()
        end
        send_acknowledgement(socket)
        message, key = split(message)
        if message != "Receive"
            println(stderr, "expected 'Receive', got '$message' from Hertta")
            continue
        end
        data = ZMQ.recv(socket)
        send_acknowledgement(socket)
        table = Arrow.Table(IOBuffer(data, read=true, write=false))
        df = DataFrame(table)
        println(df)
        push!(data_dict, (key => df))
    end
    data_dict
end

function convert_time_stamps!(df::DataFrame)
    if hasproperty(df, :t)
        if eltype(df.t) == DateTime
            try
                df.t = Dates.format.(df.t, time_stamp_format)
            catch e
                println(stderr, "error converting 't' column: ", e)
            end
        elseif eltype(df.t) == Union{Missing, DateTime}
            try
                df.t = coalesce.(Dates.format.(df.t, time_stamp_format), missing)
            catch e
                println(stderr, "error converting 't' column with missing values: ", e)
            end
        end
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
        # convert_time_stamps!(df)
        if key in sheetnames_system
            delete!(sheetnames_system, key)
            system_data[key] = df
        elseif key in sheetnames_timeseries
            delete!(sheetnames_timeseries, key)
            timeseries_data[key] = df
        else
            println("unknown keyword: ", key)
        end
    end
    if !isempty(sheetnames_system) || !isempty(sheetnames_timeseries)
        println(stderr, "did not receive all data frames")
    end
    system_data, timeseries_data

end

function send_results(socket::Socket, results::Dict{Any, Any})
    ZMQ.send(socket, "Ready to receive?")
    receive_acknowledgement(socket)
    result_time_stamp_format = dateformat"yyyy-mm-ddTHH:MM:SSzzzz"
    for (type, df) in results
        if type != "v_flow"
            continue
        end
        df[!,:t] = (t -> ZonedDateTime(t, result_time_stamp_format)).(df[!,:t])
        ZMQ.send(socket, "Receive $(type)")
        buffer = IOBuffer()
        Arrow.write(buffer, df)
        receive_acknowledgement(socket)
        ZMQ.send(socket, take!(buffer))
        receive_acknowledgement(socket)
        break
    end
    ZMQ.send(socket, "End")
    receive_acknowledgement(socket)
end

function send_failure(socket::Socket)
    ZMQ.send(socket, "Failed")
end

function main()
    println("Connecting to server...")
    socket = Socket(zmq_context, REQ)
    ZMQ.connect(socket, "tcp://localhost:$zmq_port")
    ZMQ.send(socket, "Hello")
    data_dict = receive_data(socket)
    println("All data received.")
    temporals = string.(ZonedDateTime.(pop!(data_dict, "temps").t, tz"UTC"))
    (system_data, timeseries_data) = split_data_to_system_and_time_series(data_dict)
    result_dataframes = nothing
    try
        @show methods(Predicer.compile_input_data)
        input_data = Predicer.compile_input_data(system_data, timeseries_data, temporals)
        @show typeof(input_data)   # DEBUG
        mc, input_data = Predicer.generate_model(input_data)
        Predicer.solve_model(mc)
        result_dataframes = Predicer.get_all_result_dataframes(mc, input_data)
        Predicer.dfs_to_xlsx(result_dataframes,"", "all_results")
    catch error
        send_failure(socket)
        rethrow()
    else
        send_results(socket, result_dataframes)
    finally
        ZMQ.close(socket)
        ZMQ.close(zmq_context)
    end
end

main()