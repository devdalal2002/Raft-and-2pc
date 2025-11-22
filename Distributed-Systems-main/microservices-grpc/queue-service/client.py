import argparse
import grpc
import queue_pb2
import queue_pb2_grpc
import sys

HOST = 'nginx-grpc:50051'

def add_track(stub, args):
    resp = stub.AddTrack(queue_pb2.Track(
        id=args.id, title=args.title, artist=args.artist, duration=args.duration
    ))
    print("AddTrack response:", resp)

def play_next(stub, args):
    resp = stub.PlayNext(queue_pb2.Empty())
    print("PlayNext response:", resp)

def get_history(stub, args):
    resp = stub.GetHistory(queue_pb2.Empty())
    print("History:")
    for track in resp.queue:
        print(track)

def get_queue(stub, args):
    resp = stub.GetQueue(queue_pb2.Empty())
    print("Current Queue:")
    for track in resp.queue:
        print(track)

def get_metadata(stub, args):
    resp = stub.GetMetadata(queue_pb2.TrackId(id=args.id))
    print("Track Metadata:")
    print(resp)

def vote_track(stub, args):
    # Accept --up as string for CLI compatibility
    up = args.up
    if isinstance(up, str):
        up = up.lower() in ("true", "1", "yes", "y")
    resp = stub.VoteTrack(queue_pb2.VoteRequest(id=args.id, up=up))
    print("VoteTrack response:", resp)

def remove_track(stub, args):
    resp = stub.RemoveTrack(queue_pb2.TrackId(id=args.id))
    print("RemoveTrack response:", resp)

def main():
    parser = argparse.ArgumentParser(description="gRPC Music Queue Client")
    subparsers = parser.add_subparsers(dest="command", required=True)

    add = subparsers.add_parser("add", help="Add a track")
    add.add_argument("--id", type=str, required=True)
    add.add_argument("--title", required=True)
    add.add_argument("--artist", required=True)
    add.add_argument("--duration", type=int, required=True)

    play = subparsers.add_parser("play", help="Play next track")
    hist = subparsers.add_parser("history", help="Show play history")

    queue_cmd = subparsers.add_parser("queue", help="Show current queue")
    metadata = subparsers.add_parser("metadata", help="Get track metadata")
    metadata.add_argument("--id", type=str, required=True)

    vote = subparsers.add_parser("vote", help="Vote for a track")
    vote.add_argument("--id", type=str, required=True)
    vote.add_argument("--up", required=True, help="true for upvote, false for downvote")

    remove = subparsers.add_parser("remove", help="Remove a track")
    remove.add_argument("--id", type=str, required=True)

    args = parser.parse_args()
    channel = grpc.insecure_channel(HOST)
    stub = queue_pb2_grpc.QueueServiceStub(channel)

    if args.command == "add":
        add_track(stub, args)
    elif args.command == "play":
        play_next(stub, args)
    elif args.command == "history":
        get_history(stub, args)
    elif args.command == "queue":
        get_queue(stub, args)
    elif args.command == "metadata":
        get_metadata(stub, args)
    elif args.command == "vote":
        vote_track(stub, args)
    elif args.command == "remove":
        remove_track(stub, args)
    else:
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()
