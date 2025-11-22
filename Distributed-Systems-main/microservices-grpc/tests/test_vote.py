import grpc
import queue_pb2
import queue_pb2_grpc
import sys

def main():
    target = sys.argv[1] if len(sys.argv) > 1 else 'nginx-grpc:50051'
    channel = grpc.insecure_channel(target)
    stub = queue_pb2_grpc.QueueServiceStub(channel)


    # Add a track
    track = queue_pb2.Track(id="102", title="VoteSong", artist="Voter", votes=0)
    stub.AddTrack(track)


    # Upvote
    vote = queue_pb2.VoteRequest(id="102", up=True)
    resp = stub.VoteTrack(vote)
    assert any(t.id == "102" and t.votes == 1 for t in resp.queue), "Upvote failed"

    # Downvote
    vote = queue_pb2.VoteRequest(id="102", up=False)
    resp = stub.VoteTrack(vote)
    assert any(t.id == "102" and t.votes == 0 for t in resp.queue), "Downvote failed"
    print("test_vote: PASS")

if __name__ == '__main__':
    main()
