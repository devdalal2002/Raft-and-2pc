import os
import threading
import time
from concurrent import futures
from typing import Dict, List, Set

import grpc

from . import two_pc_pb2, two_pc_pb2_grpc


def log_client(phase_name: str, node_id: str, rpc_name: str, target_phase: str, target_node: str) -> None:
    print(
        f"Phase {phase_name} of Node {node_id} sends RPC {rpc_name} to Phase {target_phase} of Node {target_node}.",
        flush=True  # ADDED: Force immediate output to Docker logs
    )


def log_server(phase_name: str, node_id: str, rpc_name: str, caller_phase: str, caller_node: str) -> None:
    print(
        f"Phase {phase_name} of Node {node_id} runs RPC {rpc_name} called by Phase {caller_phase} of Node {caller_node}.",
        flush=True  # ADDED: Force immediate output to Docker logs
    )


class TwoPCState:
    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self.default_vote = os.environ.get("DEFAULT_VOTE", "commit").lower()
        self.abort_transactions: Set[str] = {
            t.strip() for t in os.environ.get("ABORT_TRANSACTIONS", "").split(",") if t.strip()
        }
        self.prepared: Set[str] = set()
        self.committed: Set[str] = set()
        self.aborted: Set[str] = set()
        self.lock = threading.Lock()

    def should_commit(self, transaction_id: str) -> bool:
        if transaction_id in self.abort_transactions:
            return False
        return self.default_vote != "abort"

    def record_prepared(self, transaction_id: str, prepared: bool) -> None:
        with self.lock:
            if prepared:
                self.prepared.add(transaction_id)
                self.aborted.discard(transaction_id)
            else:
                self.prepared.discard(transaction_id)
                self.aborted.add(transaction_id)

    def apply_decision(self, transaction_id: str, commit: bool) -> str:
        with self.lock:
            self.prepared.discard(transaction_id)
            if commit:
                self.committed.add(transaction_id)
                self.aborted.discard(transaction_id)
                return "committed"
            self.committed.discard(transaction_id)
            self.aborted.add(transaction_id)
            return "aborted"


class DecisionPhaseServicer(two_pc_pb2_grpc.DecisionPhaseServicer):
    def __init__(self, state: TwoPCState, node_id: str):
        self.state = state
        self.node_id = node_id

    def GlobalDecision(self, request: two_pc_pb2.Decision, context: grpc.ServicerContext) -> two_pc_pb2.Ack:
        log_server("decision", self.node_id, "GlobalDecision", "decision", request.coordinator_id or "coordinator")
        outcome = self.state.apply_decision(request.transaction_id, request.commit)
        message = f"Transaction {request.transaction_id} {outcome}"
        return two_pc_pb2.Ack(node_id=self.node_id, message=message)

    def RecordPrepared(
        self, request: two_pc_pb2.PreparedState, context: grpc.ServicerContext
    ) -> two_pc_pb2.Ack:
        log_server("decision", self.node_id, "RecordPrepared", "vote", self.node_id)
        self.state.record_prepared(request.transaction_id, request.prepared)
        status = "prepared" if request.prepared else "aborted"
        return two_pc_pb2.Ack(node_id=self.node_id, message=f"Transaction {request.transaction_id} {status}")


class VotePhaseServicer(two_pc_pb2_grpc.VotePhaseServicer):
    def __init__(self, state: TwoPCState, node_id: str, decision_port: int):
        self.state = state
        self.node_id = node_id
        self.decision_port = decision_port
        self._decision_channel = grpc.insecure_channel(f"localhost:{decision_port}")
        self._decision_stub = two_pc_pb2_grpc.DecisionPhaseStub(self._decision_channel)

    def RequestVote(self, request: two_pc_pb2.Transaction, context: grpc.ServicerContext) -> two_pc_pb2.VoteReply:
        caller = request.coordinator_id or "unknown"
        log_server("vote", self.node_id, "RequestVote", "vote", caller)
        should_commit = self.state.should_commit(request.transaction_id)
        log_client("vote", self.node_id, "RecordPrepared", "decision", self.node_id)
        self._decision_stub.RecordPrepared(
            two_pc_pb2.PreparedState(transaction_id=request.transaction_id, prepared=should_commit)
        )
        reason = "Prepared successfully" if should_commit else "Vote to abort"
        return two_pc_pb2.VoteReply(node_id=self.node_id, commit=should_commit, reason=reason)


class CoordinatorControlServicer(two_pc_pb2_grpc.CoordinatorControlServicer):
    def __init__(
        self,
        node_id: str,
        vote_port: int,
        decision_port: int,
        participants: List[str],
        peer_hosts: Dict[str, str],
    ):
        self.node_id = node_id
        self.vote_port = vote_port
        self.decision_port = decision_port
        self.participants = participants
        self.peer_hosts = peer_hosts
        self._decision_channel = grpc.insecure_channel(f"localhost:{decision_port}")
        self._decision_stub = two_pc_pb2_grpc.DecisionPhaseStub(self._decision_channel)

    def Begin(self, request: two_pc_pb2.BeginRequest, context: grpc.ServicerContext) -> two_pc_pb2.Decision:
        transaction = two_pc_pb2.Transaction(
            transaction_id=request.transaction_id,
            operation=request.operation,
            coordinator_id=self.node_id,
        )
        voters = request.participant_ids or self.participants
        vote_results = []
        for participant in voters:
            if participant == self.node_id:
                continue
            target_vote_port = base_vote_port() + int(participant)
            host = self.peer_hosts.get(participant, "localhost")
            channel = grpc.insecure_channel(f"{host}:{target_vote_port}")
            stub = two_pc_pb2_grpc.VotePhaseStub(channel)
            log_client("vote", self.node_id, "RequestVote", "vote", participant)
            try:
                reply = stub.RequestVote(transaction)
                vote_results.append(reply.commit)
            finally:
                channel.close()
        local_state = True
        log_client("vote", self.node_id, "RecordPrepared", "decision", self.node_id)
        self._decision_stub.RecordPrepared(
            two_pc_pb2.PreparedState(transaction_id=request.transaction_id, prepared=local_state)
        )
        final_decision = all(vote_results) and local_state
        decision = two_pc_pb2.Decision(
            transaction_id=request.transaction_id,
            commit=final_decision,
            coordinator_id=self.node_id,
        )
        targets = set(voters)
        targets.add(self.node_id)
        for participant in targets:
            decision_port = base_decision_port() + int(participant)
            host = self.peer_hosts.get(participant, "localhost")
            channel = grpc.insecure_channel(f"{host}:{decision_port}")
            stub = two_pc_pb2_grpc.DecisionPhaseStub(channel)
            log_client("decision", self.node_id, "GlobalDecision", "decision", participant)
            try:
                stub.GlobalDecision(decision)
            finally:
                channel.close()
        return decision


def base_vote_port() -> int:
    return int(os.environ.get("VOTE_BASE_PORT", "6000"))


def base_decision_port() -> int:
    return int(os.environ.get("DECISION_BASE_PORT", "7000"))


def parse_peers() -> Dict[str, str]:
    peers = {}
    raw = os.environ.get("PEER_MAP", "")
    for entry in raw.split(","):
        if not entry:
            continue
        node_id, host = entry.split(":")
        peers[node_id.strip()] = host.strip()
    return peers


def serve() -> None:
    node_id = os.environ.get("NODE_ID", "0")
    peers = parse_peers()
    peers.setdefault(node_id, os.environ.get("HOST_ADDRESS", "localhost"))
    host = "0.0.0.0"
    vote_port = base_vote_port() + int(node_id)
    decision_port = base_decision_port() + int(node_id)

    state = TwoPCState(node_id=node_id)

    decision_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    decision_servicer = DecisionPhaseServicer(state, node_id)
    two_pc_pb2_grpc.add_DecisionPhaseServicer_to_server(decision_servicer, decision_server)
    decision_server.add_insecure_port(f"{host}:{decision_port}")

    vote_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    vote_servicer = VotePhaseServicer(state, node_id, decision_port)
    two_pc_pb2_grpc.add_VotePhaseServicer_to_server(vote_servicer, vote_server)
    vote_server.add_insecure_port(f"{host}:{vote_port}")

    coordinator_service = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    participant_ids = sorted(set(peers.keys()) | {node_id})
    control_servicer = CoordinatorControlServicer(
        node_id, vote_port, decision_port, participant_ids, peers
    )
    two_pc_pb2_grpc.add_CoordinatorControlServicer_to_server(control_servicer, coordinator_service)
    coordinator_service.add_insecure_port(f"{host}:{vote_port + 100}")

    decision_server.start()
    vote_server.start()
    coordinator_service.start()

    print(
        f"Node {node_id} started vote phase on port {vote_port}, decision phase on port {decision_port}, "
        f"control on port {vote_port + 100}.",
        flush=True  # ADDED: Force immediate output to Docker logs
    )

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        decision_server.stop(0)
        vote_server.stop(0)
        coordinator_service.stop(0)


if __name__ == "__main__":
    serve()