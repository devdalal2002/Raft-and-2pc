import argparse
import uuid

import grpc

from . import two_pc_pb2, two_pc_pb2_grpc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a two-phase commit transaction")
    parser.add_argument("coordinator", help="Coordinator host:port for control service")
    parser.add_argument(
        "--operation",
        default="demo-operation",
        help=(
            "Human-readable description of the transaction (logged only, does not need to match "
            "a real business action)"
        ),
    )
    parser.add_argument(
        "--participants",
        nargs="*",
        default=[],
        help="Participant node identifiers to include in the transaction",
    )
    parser.add_argument(
        "--transaction-id",
        default=None,
        help="Transaction identifier (defaults to generated UUID)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    transaction_id = args.transaction_id or str(uuid.uuid4())
    host, port = args.coordinator.split(":")
    channel = grpc.insecure_channel(f"{host}:{port}")
    stub = two_pc_pb2_grpc.CoordinatorControlStub(channel)
    request = two_pc_pb2.BeginRequest(
        transaction_id=transaction_id,
        operation=args.operation,
        participant_ids=args.participants,
    )
    response = stub.Begin(request)
    outcome = "committed" if response.commit else "aborted"
    print(f"Transaction {response.transaction_id} {outcome} in coordinator term {response.coordinator_id}")


if __name__ == "__main__":
    main()
