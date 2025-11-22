import grpc
import queue_pb2
import queue_pb2_grpc
import sys

def main():
    # Test sync by adding to one node and checking another
    # In Docker Compose, all replicas are accessed via 'nginx-grpc:50051' (load balanced)
    target = sys.argv[1] if len(sys.argv) > 1 else 'nginx-grpc:50051'
    channel = grpc.insecure_channel(target)
    stub = queue_pb2_grpc.QueueServiceStub(channel)


    # Add a track
    track = queue_pb2.Track(id="103", title="SyncSong", artist="Syncer", votes=0)
    stub.AddTrack(track)

    # Check queue (load balanced)
    queue = stub.GetQueue(queue_pb2.Empty())
    assert any(t.id == "103" for t in queue.queue), "Track not in queue (sync failed)"
    print("test_sync: PASS")

if __name__ == '__main__':
    main()
