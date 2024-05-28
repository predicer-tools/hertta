using Arrow
using ZMQ

zmq_context = Context()

println("Connecting to server...")
socket = Socket(zmq_context, REQ)
ZMQ.connect(socket, "tcp://localhost:5555")
for request in 1:10
    println("Sending request $request ...")
    ZMQ.send(socket, "Hello")
    table = Arrow.Table(IOBuffer(ZMQ.recv(socket), read=true, write=false))
    println(table)
end
ZMQ.send(socket, "Quit")

ZMQ.close(socket)
ZMQ.close(zmq_context)
