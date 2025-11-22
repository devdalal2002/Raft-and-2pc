import grpc
import raft_pb2, raft_pb2_grpc

channel = grpc.insecure_channel("localhost:9000")
stub = raft_pb2_grpc.RaftClientStub(channel)

response = stub.ClientRequest(
    raft_pb2.ClientCommand(command="hello-raft")
)

print(response)
channel.close()
