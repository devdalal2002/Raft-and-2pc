import os
import random
import time
import unittest
from typing import Dict, List, Tuple

import grpc
from concurrent import futures

from consensus.raft import RaftNode, raft_pb2, raft_pb2_grpc


class RaftCluster:
    def __init__(self, size: int = 5, base: int | None = None):
        self.size = size
        self.base_port = base or random.randint(20000, 40000)
        self.nodes: Dict[str, RaftNode] = {}
        self.servers: Dict[str, grpc.Server] = {}
        self.channels: List[grpc.Channel] = []
        os.environ["RAFT_BASE_PORT"] = str(self.base_port)
        peer_map = {str(i): "127.0.0.1" for i in range(size)}
        for node_id, host in peer_map.items():
            peers = dict(peer_map)
            node = RaftNode(node_id=node_id, peers=peers, port=self.base_port + int(node_id))
            server = grpc.server(futures.ThreadPoolExecutor(max_workers=50))
            raft_pb2_grpc.add_RaftConsensusServicer_to_server(node, server)
            raft_pb2_grpc.add_RaftClientServicer_to_server(node, server)
            server.add_insecure_port(f"{host}:{self.base_port + int(node_id)}")
            server.start()
            node.start()
            self.nodes[node_id] = node
            self.servers[node_id] = server

    def wait_for_leader(self, timeout: float = 10.0) -> Tuple[str, RaftNode]:
        deadline = time.time() + timeout
        while time.time() < deadline:
            for node_id, node in self.nodes.items():
                with node.lock:
                    if node.state == "leader":
                        return node_id, node
            time.sleep(0.1)
        raise TimeoutError("Leader election timed out")

    def stop_node(self, node_id: str) -> None:
        node = self.nodes.pop(node_id)
        server = self.servers.pop(node_id)
        node.stop()
        server.stop(0)

    def stop(self) -> None:
        for node in list(self.nodes.values()):
            node.stop()
        for server in list(self.servers.values()):
            server.stop(0)
        self.nodes.clear()
        self.servers.clear()
        for channel in self.channels:
            channel.close()
        self.channels.clear()

    def client_stub(self, node_id: str) -> raft_pb2_grpc.RaftClientStub:
        host = "127.0.0.1"
        port = self.base_port + int(node_id)
        channel = grpc.insecure_channel(f"{host}:{port}")
        self.channels.append(channel)
        return raft_pb2_grpc.RaftClientStub(channel)


class TestRaftCluster(unittest.TestCase):
    def setUp(self) -> None:
        self.cluster = RaftCluster(size=5)

    def tearDown(self) -> None:
        self.cluster.stop()

    def test_leader_elected(self) -> None:
        leader_id, _ = self.cluster.wait_for_leader()
        self.assertIsNotNone(leader_id)

    def test_heartbeat_stabilizes(self) -> None:
        leader_id, leader = self.cluster.wait_for_leader()
        time.sleep(2)
        with leader.lock:
            self.assertEqual(leader.state, "leader")
        for node_id, node in self.cluster.nodes.items():
            with node.lock:
                self.assertEqual(node.leader_id, leader_id)

    def test_client_command_replicated(self) -> None:
        leader_id, _ = self.cluster.wait_for_leader()
        stub = self.cluster.client_stub(leader_id)
        response = stub.ClientRequest(raft_pb2.ClientCommand(command="play song"))
        self.assertTrue(response.success)
        time.sleep(1.5)
        for node in self.cluster.nodes.values():
            with node.lock:
                self.assertIn("play song", node.state_machine)

    def test_client_forwarding(self) -> None:
        leader_id, _ = self.cluster.wait_for_leader()
        time.sleep(1)
        follower_id = next(n for n in self.cluster.nodes if n != leader_id)
        stub = self.cluster.client_stub(follower_id)
        response = stub.ClientRequest(raft_pb2.ClientCommand(command="skip"))
        self.assertTrue(response.success)
        time.sleep(1.5)
        for node in self.cluster.nodes.values():
            with node.lock:
                self.assertIn("skip", node.state_machine)

    def test_leader_failover(self) -> None:
        leader_id, _ = self.cluster.wait_for_leader()
        self.cluster.stop_node(leader_id)
        new_leader_id, _ = self.cluster.wait_for_leader(timeout=10)
        self.assertNotEqual(leader_id, new_leader_id)


if __name__ == "__main__":
    unittest.main()
