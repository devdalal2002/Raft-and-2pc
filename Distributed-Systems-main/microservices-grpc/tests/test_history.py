import grpc
import queue_pb2
import queue_pb2_grpc
import sys

def main():
    # Clear Redis state for test isolation
    import redis
    r = redis.Redis(host='redis', port=6379, decode_responses=False)
    r.delete('queue')
    r.delete('history')
    target = sys.argv[1] if len(sys.argv) > 1 else 'nginx-grpc:50051'
    channel = grpc.insecure_channel(target)
    stub = queue_pb2_grpc.QueueServiceStub(channel)

    # Add a track and play it
    track = queue_pb2.Track(id="105", title="HistSong", artist="Hist", votes=0)
    stub.AddTrack(track)
    stub.PlayNext(queue_pb2.Empty())

    # Check history
    history = stub.GetHistory(queue_pb2.Empty())
    assert any(t.id == "105" for t in history.queue), "Track not in history"
    print("test_history: PASS")

if __name__ == '__main__':
    main()
