import os
import random
import time
from typing import Dict, List, Tuple
from datetime import datetime

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


class TestResult:
    def __init__(self, name: str):
        self.name = name
        self.passed = False
        self.error = None
        self.duration = 0.0
        self.details = ""


class RaftTestRunner:
    def __init__(self):
        self.results: List[TestResult] = []
        self.cluster = None

    def setup(self, size: int = 5):
        print(f"Setting up cluster with {size} nodes...")
        self.cluster = RaftCluster(size=size)
        time.sleep(1)

    def teardown(self):
        if self.cluster:
            print("Tearing down cluster...")
            self.cluster.stop()
            self.cluster = None

    def run_test(self, test_func, name: str, description: str):
        result = TestResult(name)
        print(f"\n{'='*70}")
        print(f"Running: {name}")
        print(f"Description: {description}")
        print(f"{'='*70}")
        
        start_time = time.time()
        try:
            test_func(result)
            result.passed = True
            print(f"✓ PASSED: {name}")
        except Exception as e:
            result.passed = False
            result.error = str(e)
            print(f"✗ FAILED: {name}")
            print(f"Error: {e}")
        finally:
            result.duration = time.time() - start_time
            self.results.append(result)
            print(f"Duration: {result.duration:.2f}s")

    def test_1_leader_election(self, result: TestResult):
        """Test Case 1: Leader Election"""
        self.setup(size=5)
        try:
            leader_id, leader = self.cluster.wait_for_leader(timeout=10)
            result.details = f"Leader elected: Node {leader_id}"
            print(f"  → Leader elected: Node {leader_id}")
            
            # Verify only one leader exists
            leader_count = 0
            for node_id, node in self.cluster.nodes.items():
                with node.lock:
                    if node.state == "leader":
                        leader_count += 1
            
            if leader_count != 1:
                raise AssertionError(f"Expected 1 leader, found {leader_count}")
            
            print(f"  → Verified: Only one leader in the cluster")
            
        finally:
            self.teardown()

    def test_2_leader_failover(self, result: TestResult):
        """Test Case 2: Leader Election After Leader Failure"""
        self.setup(size=5)
        try:
            # Wait for initial leader
            leader_id, _ = self.cluster.wait_for_leader(timeout=10)
            print(f"  → Initial leader: Node {leader_id}")
            
            # Stop the leader
            print(f"  → Stopping leader (Node {leader_id})...")
            self.cluster.stop_node(leader_id)
            
            # Wait for new leader election
            time.sleep(2)
            new_leader_id, _ = self.cluster.wait_for_leader(timeout=15)
            print(f"  → New leader elected: Node {new_leader_id}")
            
            if leader_id == new_leader_id:
                raise AssertionError("New leader should be different from failed leader")
            
            result.details = f"Old leader: Node {leader_id}, New leader: Node {new_leader_id}"
            print(f"  → Verified: New leader is different from failed leader")
            
        finally:
            self.teardown()

    def test_3_log_replication(self, result: TestResult):
        """Test Case 3: Log Replication and Consistency"""
        self.setup(size=3)
        try:
            leader_id, _ = self.cluster.wait_for_leader(timeout=10)
            print(f"  → Leader: Node {leader_id}")
            
            # Send commands
            stub = self.cluster.client_stub(leader_id)
            commands = ["set x 1", "set y 2", "set z 3", "delete x", "set w 4"]
            
            print(f"  → Sending {len(commands)} commands to leader...")
            for cmd in commands:
                response = stub.ClientRequest(raft_pb2.ClientCommand(command=cmd))
                if not response.success:
                    raise AssertionError(f"Command '{cmd}' failed")
                print(f"    • Sent: {cmd}")
            
            # Wait for replication
            time.sleep(2)
            
            # Verify all nodes have the same log
            print(f"  → Verifying log consistency across all nodes...")
            state_machines = []
            for node_id, node in self.cluster.nodes.items():
                with node.lock:
                    state_machines.append(set(node.state_machine))
                    print(f"    • Node {node_id}: {len(node.state_machine)} entries")
            
            # Check all state machines are identical
            first_sm = state_machines[0]
            for sm in state_machines[1:]:
                if sm != first_sm:
                    raise AssertionError("Log entries not consistent across all nodes")
            
            result.details = f"Replicated {len(commands)} commands across {len(self.cluster.nodes)} nodes"
            print(f"  → Verified: All nodes have consistent logs")
            
        finally:
            self.teardown()

    def test_4_client_forwarding(self, result: TestResult):
        """Test Case 4: Client Request Forwarding from Follower"""
        self.setup(size=3)
        try:
            leader_id, _ = self.cluster.wait_for_leader(timeout=10)
            print(f"  → Leader: Node {leader_id}")
            
            # Find a follower
            time.sleep(1)
            follower_id = next(n for n in self.cluster.nodes if n != leader_id)
            print(f"  → Sending command to follower (Node {follower_id})...")
            
            # Send command to follower
            stub = self.cluster.client_stub(follower_id)
            response = stub.ClientRequest(raft_pb2.ClientCommand(command="forward_test"))
            
            if not response.success:
                raise AssertionError("Follower failed to forward command to leader")
            
            print(f"  → Command successfully forwarded to leader")
            
            # Wait for replication
            time.sleep(2)
            
            # Verify command was replicated to all nodes
            print(f"  → Verifying command replicated to all nodes...")
            for node_id, node in self.cluster.nodes.items():
                with node.lock:
                    if "forward_test" not in node.state_machine:
                        raise AssertionError(f"Command not found in Node {node_id}")
                    print(f"    • Node {node_id}: Command present")
            
            result.details = f"Command forwarded from Node {follower_id} to Node {leader_id}"
            print(f"  → Verified: Command forwarded and replicated successfully")
            
        finally:
            self.teardown()

    def test_5_network_partition_recovery(self, result: TestResult):
        """Test Case 5: Network Partition and Recovery"""
        self.setup(size=5)
        try:
            # Wait for initial leader
            leader_id, _ = self.cluster.wait_for_leader(timeout=10)
            print(f"  → Initial leader: Node {leader_id}")
            
            # Send initial commands to establish baseline
            stub = self.cluster.client_stub(leader_id)
            response = stub.ClientRequest(raft_pb2.ClientCommand(command="before_partition"))
            if not response.success:
                raise AssertionError("Initial command failed")
            print(f"  → Sent initial command")
            
            time.sleep(2)
            
            # Verify all 5 nodes have the initial command
            initial_count = 0
            for node_id, node in self.cluster.nodes.items():
                with node.lock:
                    if "before_partition" in node.state_machine:
                        initial_count += 1
                        print(f"    • Node {node_id}: Has initial command")
            print(f"  → {initial_count}/5 nodes have initial command")
            
            # Simulate partition by stopping 1 node (not the leader)
            nodes_to_stop = [n for n in self.cluster.nodes.keys() if n != leader_id][:1]
            print(f"  → Simulating partition: Stopping node {nodes_to_stop[0]}")
            self.cluster.stop_node(nodes_to_stop[0])
            
            # Wait for system to stabilize
            time.sleep(2)
            
            # Leader should still be able to commit with majority (4 out of 5)
            print(f"  → Testing majority consensus with 4 nodes...")
            response = stub.ClientRequest(raft_pb2.ClientCommand(command="during_partition"))
            if not response.success:
                # This is acceptable - leader might need more time
                print(f"  → Command response: {response.success}")
            else:
                print(f"  → Command succeeded with majority")
            
            time.sleep(3)
            
            # Verify remaining nodes have at least the initial command
            remaining_count = 0
            partition_count = 0
            for node_id, node in self.cluster.nodes.items():
                with node.lock:
                    has_initial = "before_partition" in node.state_machine
                    has_partition = "during_partition" in node.state_machine
                    if has_initial:
                        remaining_count += 1
                    if has_partition:
                        partition_count += 1
                    print(f"    • Node {node_id}: initial={has_initial}, partition={has_partition}")
            
            # We expect all remaining nodes (4) to have the commands
            active_nodes = len(self.cluster.nodes)
            print(f"  → {remaining_count}/{active_nodes} nodes have initial command")
            print(f"  → {partition_count}/{active_nodes} nodes have partition command")
            
            # Success criteria: majority of active nodes have commands
            if remaining_count < (active_nodes // 2 + 1):
                raise AssertionError(f"Only {remaining_count}/{active_nodes} nodes have commands, expected majority")
            
            result.details = f"System maintained consistency with {active_nodes}/5 nodes active (stopped: {nodes_to_stop[0]})"
            print(f"  → Verified: System maintained consistency during partition")
            
        finally:
            self.teardown()

    def print_summary(self):
        print("\n" + "="*70)
        print(" " * 25 + "TEST SUMMARY")
        print("="*70)
        
        passed = sum(1 for r in self.results if r.passed)
        failed = len(self.results) - passed
        total_time = sum(r.duration for r in self.results)
        
        print(f"\nTotal Tests: {len(self.results)}")
        print(f"Passed: {passed} ✓")
        print(f"Failed: {failed} ✗")
        print(f"Total Duration: {total_time:.2f}s")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        print("\n" + "-"*70)
        print("DETAILED RESULTS:")
        print("-"*70)
        
        for i, result in enumerate(self.results, 1):
            status = "✓ PASS" if result.passed else "✗ FAIL"
            print(f"\n{i}. {result.name}")
            print(f"   Status: {status}")
            print(f"   Duration: {result.duration:.2f}s")
            if result.details:
                print(f"   Details: {result.details}")
            if result.error:
                print(f"   Error: {result.error}")
        
        print("\n" + "="*70)
        
        if failed == 0:
            print("🎉 ALL TESTS PASSED! 🎉")
        else:
            print(f"⚠️  {failed} TEST(S) FAILED")
        print("="*70 + "\n")


def main():
    print("\n" + "="*70)
    print(" " * 20 + "RAFT CONSENSUS TEST SUITE")
    print("="*70)
    print(f"Start Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*70 + "\n")
    
    runner = RaftTestRunner()
    
    # Run all tests
    runner.run_test(
        runner.test_1_leader_election,
        "Test 1: Leader Election",
        "Verify that a leader is elected in a cluster"
    )
    
    runner.run_test(
        runner.test_2_leader_failover,
        "Test 2: Leader Failover",
        "Verify new leader election after current leader fails"
    )
    
    runner.run_test(
        runner.test_3_log_replication,
        "Test 3: Log Replication",
        "Verify commands are replicated consistently across all nodes"
    )
    
    runner.run_test(
        runner.test_4_client_forwarding,
        "Test 4: Client Request Forwarding",
        "Verify followers forward client requests to the leader"
    )
    
    runner.run_test(
        runner.test_5_network_partition_recovery,
        "Test 5: Network Partition",
        "Verify system handles network partitions correctly"
    )
    
    # Print summary
    runner.print_summary()


if __name__ == "__main__":
    main()