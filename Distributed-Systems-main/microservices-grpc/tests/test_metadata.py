import grpc
import queue_pb2
import queue_pb2_grpc
import sys

def main():
    target = sys.argv[1] if len(sys.argv) > 1 else 'nginx-grpc:50051'
    channel = grpc.insecure_channel(target)
    stub = queue_pb2_grpc.QueueServiceStub(channel)

    # Add a track with duration
    track = queue_pb2.Track(id="104", title="MetaSong", artist="Meta", votes=0, duration=321)
    stub.AddTrack(track)

    # Get metadata
    action = queue_pb2.TrackId(id="104")
    meta = stub.GetMetadata(action)
    assert meta.id == "104" and meta.title == "MetaSong" and meta.duration == 321, "Metadata incorrect"
    print("test_metadata: PASS")

if __name__ == '__main__':
    main()
