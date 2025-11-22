
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import grpc
import queue_pb2
import queue_pb2_grpc

def main():
    target = sys.argv[1] if len(sys.argv) > 1 else 'nginx-grpc:50051'
    channel = grpc.insecure_channel(target)
    stub = queue_pb2_grpc.QueueServiceStub(channel)

    # Add a track
    track = queue_pb2.Track(id="101", title="TestAdd", artist="Tester", votes=0)
    resp = stub.AddTrack(track)
    assert any(t.id == "101" for t in resp.queue), "Track not added"

    # Remove the track
    action = queue_pb2.TrackId(id="101")
    resp = stub.RemoveTrack(action)
    assert all(t.id != "101" for t in resp.queue), "Track not removed"
    print("test_add_remove: PASS")

if __name__ == '__main__':
    main()
