import os
import random
import threading
import time
from concurrent import futures
from typing import Dict, List, Optional

import grpc

from . import raft_pb2, raft_pb2_grpc


def log_client(node_id: str, rpc_name: str, target_node: str) -> None:
    print(f"Node {node_id} sends RPC {rpc_name} to Node {target_node}.")


def log_server(node_id: str, rpc_name: str, caller_node: str) -> None:
    print(f"Node {node_id} runs RPC {rpc_name} called by Node {caller_node}.")


class RaftNode(raft_pb2_grpc.RaftConsensusServicer, raft_pb2_grpc.RaftClientServicer):
    def __init__(self, node_id: str, peers: Dict[str, str], port: int):
        self.node_id = node_id
        self.peers = peers
        self.port = port
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.state = "follower"
        self.leader_id: Optional[str] = None
        self.log: List[raft_pb2.LogEntry] = []
        self.commit_index = 0
        self.last_applied = 0
        self.state_machine: List[str] = []
        self.lock = threading.RLock()
        self.last_heartbeat = time.monotonic()
        self.election_timeout = self._new_election_timeout()
        self.heartbeat_interval = float(os.environ.get("HEARTBEAT_INTERVAL", "1"))
        self._stop = threading.Event()
        self._election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self._heartbeat_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)

    # Utility helpers -------------------------------------------------
    def _new_election_timeout(self) -> float:
        return random.uniform(1.5, 3.0)

    def _majority(self) -> int:
        return len(self.peers) // 2 + 1

    def _last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    def _last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0

    def _become_follower(self, term: int, leader_id: Optional[str]) -> None:
        self.state = "follower"
        self.current_term = term
        self.leader_id = leader_id
        self.voted_for = None
        self.last_heartbeat = time.monotonic()
        self.election_timeout = self._new_election_timeout()

    # RPC handlers ----------------------------------------------------
    def RequestVote(self, request: raft_pb2.VoteRequest, context: grpc.ServicerContext) -> raft_pb2.VoteReply:
        with self.lock:
            log_server(self.node_id, "RequestVote", request.candidate_id)
            if request.term < self.current_term:
                return raft_pb2.VoteReply(voter_id=self.node_id, term=self.current_term, vote_granted=False)
            if request.term > self.current_term:
                self._become_follower(request.term, None)
            up_to_date = (request.last_log_term > self._last_log_term()) or (
                request.last_log_term == self._last_log_term() and request.last_log_index >= self._last_log_index()
            )
            grant = False
            if (self.voted_for in (None, request.candidate_id)) and up_to_date:
                self.voted_for = request.candidate_id
                grant = True
                self.last_heartbeat = time.monotonic()
                self.election_timeout = self._new_election_timeout()
            return raft_pb2.VoteReply(voter_id=self.node_id, term=self.current_term, vote_granted=grant)

    def AppendEntries(
        self, request: raft_pb2.AppendEntriesRequest, context: grpc.ServicerContext
    ) -> raft_pb2.AppendEntriesReply:
        with self.lock:
            log_server(self.node_id, "AppendEntries", request.leader_id)
            if request.term < self.current_term:
                return raft_pb2.AppendEntriesReply(
                    follower_id=self.node_id, term=self.current_term, success=False
                )
            if request.term >= self.current_term:
                self._become_follower(request.term, request.leader_id)
            self.log = [raft_pb2.LogEntry(term=e.term, index=e.index, command=e.command) for e in request.entries]
            self._apply_commits(request.commit_index)
            return raft_pb2.AppendEntriesReply(follower_id=self.node_id, term=self.current_term, success=True)

    def ClientRequest(self, request: raft_pb2.ClientCommand, context: grpc.ServicerContext) -> raft_pb2.ClientReply:
        forward_channel: Optional[grpc.Channel] = None
        forward_target: Optional[str] = None
        with self.lock:
            log_server(self.node_id, "ClientRequest", self.node_id)
            if self.state != "leader":
                leader = self.leader_id or ""
                if leader == "" or leader == self.node_id:
                    deadline = time.monotonic() + 3.0
                    while (leader == "" or leader == self.node_id) and time.monotonic() < deadline:
                        time.sleep(0.1)
                        leader = self.leader_id or ""
                if leader and leader != self.node_id:
                    host = self.peers.get(leader, "localhost")
                    forward_channel = grpc.insecure_channel(f"{host}:{base_port() + int(leader)}")
                    forward_target = leader
                else:
                    return raft_pb2.ClientReply(
                        success=False,
                        leader_id=leader,
                        commit_index=self.commit_index,
                        message="No leader available",
                    )
            else:
                entry = raft_pb2.LogEntry(
                    term=self.current_term, index=self._last_log_index() + 1, command=request.command
                )
                self.log.append(entry)
                acks = self._broadcast_append_entries()
                deadline = time.monotonic() + 3.0
                while acks < self._majority() and time.monotonic() < deadline:
                    time.sleep(0.2)
                    acks = self._broadcast_append_entries()
                if acks >= self._majority():
                    self.commit_index = len(self.log)
                    self._apply_commits(self.commit_index)
                    return raft_pb2.ClientReply(
                        success=True,
                        leader_id=self.node_id,
                        commit_index=self.commit_index,
                        message="Committed",
                    )
                return raft_pb2.ClientReply(
                    success=False,
                    leader_id=self.node_id,
                    commit_index=self.commit_index,
                    message="Failed to reach majority",
                )
        assert forward_channel is not None and forward_target is not None
        stub = raft_pb2_grpc.RaftClientStub(forward_channel)
        log_client(self.node_id, "ClientRequest", forward_target)
        try:
            return stub.ClientRequest(request)
        finally:
            forward_channel.close()

    # Internal mechanics ----------------------------------------------
    def _apply_commits(self, commit_index: int) -> None:
        while self.last_applied < commit_index and self.last_applied < len(self.log):
            entry = self.log[self.last_applied]
            self.state_machine.append(entry.command)
            self.last_applied += 1

    def _broadcast_append_entries(self) -> int:
        with self.lock:
            term = self.current_term
            entries = [raft_pb2.LogEntry(term=e.term, index=e.index, command=e.command) for e in self.log]
            commit_index = self.commit_index
        acknowledgements = 1
        for peer_id in self.peers:
            if peer_id == self.node_id:
                continue
            response = self._send_append_entries(peer_id, term, entries, commit_index)
            if response and response.success:
                acknowledgements += 1
            elif response and response.term > term:
                with self.lock:
                    self._become_follower(response.term, response.follower_id or None)
                break
        return acknowledgements

    def _send_append_entries(
        self, peer_id: str, term: int, entries: List[raft_pb2.LogEntry], commit_index: int
    ) -> Optional[raft_pb2.AppendEntriesReply]:
        host = self.peers.get(peer_id, "localhost")
        channel = grpc.insecure_channel(f"{host}:{base_port() + int(peer_id)}")
        stub = raft_pb2_grpc.RaftConsensusStub(channel)
        request = raft_pb2.AppendEntriesRequest(
            leader_id=self.node_id, term=term, entries=entries, commit_index=commit_index
        )
        log_client(self.node_id, "AppendEntries", peer_id)
        try:
            return stub.AppendEntries(request)
        except grpc.RpcError:
            return None
        finally:
            channel.close()

    def _request_vote(self, peer_id: str, term: int, last_log_index: int, last_log_term: int) -> Optional[raft_pb2.VoteReply]:
        host = self.peers.get(peer_id, "localhost")
        channel = grpc.insecure_channel(f"{host}:{base_port() + int(peer_id)}")
        stub = raft_pb2_grpc.RaftConsensusStub(channel)
        request = raft_pb2.VoteRequest(
            candidate_id=self.node_id,
            term=term,
            last_log_index=last_log_index,
            last_log_term=last_log_term,
        )
        log_client(self.node_id, "RequestVote", peer_id)
        try:
            return stub.RequestVote(request)
        except grpc.RpcError:
            return None
        finally:
            channel.close()

    def _election_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(0.1)
            with self.lock:
                if self.state == "leader":
                    continue
                elapsed = time.monotonic() - self.last_heartbeat
                timeout = self.election_timeout
            if elapsed >= timeout:
                self._start_election()

    def _start_election(self) -> None:
        with self.lock:
            self.state = "candidate"
            self.current_term += 1
            term = self.current_term
            self.voted_for = self.node_id
            self.last_heartbeat = time.monotonic()
            self.election_timeout = self._new_election_timeout()
            last_log_index = self._last_log_index()
            last_log_term = self._last_log_term()
        votes = 1
        for peer_id in self.peers:
            if peer_id == self.node_id:
                continue
            reply = self._request_vote(peer_id, term, last_log_index, last_log_term)
            if reply is None:
                continue
            if reply.term > term:
                with self.lock:
                    self._become_follower(reply.term, None)
                return
            if reply.vote_granted:
                votes += 1
        if votes >= self._majority():
            with self.lock:
                self.state = "leader"
                self.leader_id = self.node_id
                self.voted_for = None
                self.last_heartbeat = time.monotonic()
                self.election_timeout = self._new_election_timeout()
                print(f"Node {self.node_id} became leader for term {self.current_term} with {votes} votes")
        else:
            with self.lock:
                self.state = "follower"
                self.voted_for = None
                self.election_timeout = self._new_election_timeout()

    def _heartbeat_loop(self) -> None:
        while not self._stop.is_set():
            time.sleep(self.heartbeat_interval)
            with self.lock:
                if self.state != "leader":
                    continue
                term = self.current_term
            acks = self._broadcast_append_entries()
            if acks >= self._majority():
                with self.lock:
                    self.commit_index = max(self.commit_index, len(self.log))
                    self._apply_commits(self.commit_index)

    # Lifecycle -------------------------------------------------------
    def start(self) -> None:
        self._election_thread.start()
        self._heartbeat_thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._election_thread.join(timeout=1)
        self._heartbeat_thread.join(timeout=1)


def base_port() -> int:
    return int(os.environ.get("RAFT_BASE_PORT", "9000"))


def parse_peers() -> Dict[str, str]:
    mapping: Dict[str, str] = {}
    raw = os.environ.get("PEER_MAP", "")
    for entry in raw.split(","):
        if not entry:
            continue
        node_id, host = entry.split(":")
        mapping[node_id.strip()] = host.strip()
    return mapping


def serve() -> None:
    node_id = os.environ.get("NODE_ID", "0")
    peers = parse_peers()
    peers.setdefault(node_id, os.environ.get("HOST_ADDRESS", "localhost"))
    port = base_port() + int(node_id)
    host = "0.0.0.0"

    node = RaftNode(node_id, peers, port)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
    raft_pb2_grpc.add_RaftConsensusServicer_to_server(node, server)
    raft_pb2_grpc.add_RaftClientServicer_to_server(node, server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    node.start()
    print(f"Raft node {node_id} serving on port {port}")

    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        node.stop()
        server.stop(0)


if __name__ == "__main__":
    serve()
