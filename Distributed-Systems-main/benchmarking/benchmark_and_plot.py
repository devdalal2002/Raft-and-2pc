import requests
import subprocess
import time
import threading
import grpc
import matplotlib.pyplot as plt
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../microservices-grpc/tests')))
import queue_pb2
import queue_pb2_grpc

REST_URL = "http://localhost:8080/add_track"
GRPC_HOST = "nginx-grpc:50051"

# --- REST Benchmark ---
def rest_worker(n, payload):
    for _ in range(n):
        try:
            requests.post(REST_URL, json=payload)
        except Exception:
            pass

def bench_rest(concurrency=20, total=200):
    payload = {"id": 1, "title": "Song", "artist": "A", "duration": 200}
    per_thread = total // concurrency
    threads = []
    start = time.time()
    for _ in range(concurrency):
        t = threading.Thread(target=rest_worker, args=(per_thread, payload))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.time() - start
    return total/elapsed, elapsed/total*1000

# --- gRPC Benchmark ---
def grpc_worker(n):
    channel = grpc.insecure_channel(GRPC_HOST)
    stub = queue_pb2_grpc.QueueServiceStub(channel)
    for _ in range(n):
        try:
            stub.AddTrack(queue_pb2.Track(id="1", title="Song", artist="A", duration=200))
        except Exception:
            pass

def bench_grpc(concurrency=20, total=200):
    per_thread = total // concurrency
    threads = []
    start = time.time()
    for _ in range(concurrency):
        t = threading.Thread(target=grpc_worker, args=(per_thread,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    elapsed = time.time() - start
    return total/elapsed, elapsed/total*1000

# --- Main Experiment ---
def run_experiment():
    node_counts = [1, 2, 3, 5, 10]
    concurrency = 20
    total = 200
    rest_results = []
    grpc_results = []
    num_trials = 3
    for nodes in node_counts:
        # REST trials
        rest_rps_trials = []
        rest_lat_trials = []
        for trial in range(num_trials):
            print(f"\nTesting REST with {nodes} nodes (trial {trial+1}/{num_trials})...")
            subprocess.run(["docker", "compose", "-f", "layered-rest/docker-compose.yml", "down"], stdout=subprocess.DEVNULL)
            subprocess.run(["docker", "compose", "-f", "layered-rest/docker-compose.yml", "up", "--build", f"--scale", f"node={nodes}", "-d"], stdout=subprocess.DEVNULL)
            time.sleep(10)
            rps, latency = bench_rest(concurrency, total)
            rest_rps_trials.append(rps)
            rest_lat_trials.append(latency)
            subprocess.run(["docker", "compose", "-f", "layered-rest/docker-compose.yml", "down"], stdout=subprocess.DEVNULL)
        avg_rest_rps = sum(rest_rps_trials) / num_trials
        avg_rest_lat = sum(rest_lat_trials) / num_trials
        rest_results.append((nodes, avg_rest_rps, avg_rest_lat))

        # gRPC trials
        grpc_rps_trials = []
        grpc_lat_trials = []
        for trial in range(num_trials):
            print(f"\nTesting gRPC with {nodes} nodes (trial {trial+1}/{num_trials})...")
            subprocess.run(["docker", "compose", "-f", "microservices-grpc/docker-compose.yml", "down"], stdout=subprocess.DEVNULL)
            subprocess.run(["docker", "compose", "-f", "microservices-grpc/docker-compose.yml", "up", "--build", f"--scale", f"queue-service={nodes}", "-d"], stdout=subprocess.DEVNULL)
            time.sleep(10)
            rps, latency = bench_grpc(concurrency, total)
            grpc_rps_trials.append(rps)
            grpc_lat_trials.append(latency)
            subprocess.run(["docker", "compose", "-f", "microservices-grpc/docker-compose.yml", "down"], stdout=subprocess.DEVNULL)
        avg_grpc_rps = sum(grpc_rps_trials) / num_trials
        avg_grpc_lat = sum(grpc_lat_trials) / num_trials
        grpc_results.append((nodes, avg_grpc_rps, avg_grpc_lat))
    return rest_results, grpc_results

# --- Plotting ---
def plot_results(rest_results, grpc_results, outdir):
    nodes = [x[0] for x in rest_results]
    rest_rps = [x[1] for x in rest_results]
    grpc_rps = [x[1] for x in grpc_results]
    rest_lat = [x[2] for x in rest_results]
    grpc_lat = [x[2] for x in grpc_results]

    plt.figure(figsize=(10,4))
    plt.subplot(1,2,1)
    plt.plot(nodes, rest_rps, 'o-', label='REST')
    plt.plot(nodes, grpc_rps, 'o-', label='gRPC')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Throughput (req/s)')
    plt.title('Throughput vs Nodes')
    plt.legend()

    plt.subplot(1,2,2)
    plt.plot(nodes, rest_lat, 'o-', label='REST')
    plt.plot(nodes, grpc_lat, 'o-', label='gRPC')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Avg Latency (ms)')
    plt.title('Latency vs Nodes')
    plt.legend()

    plt.tight_layout()
    os.makedirs(outdir, exist_ok=True)
    plt.savefig(os.path.join(outdir, 'performance_figures.png'))
    print(f"Figures saved to {os.path.join(outdir, 'performance_figures.png')}")

    # Also save raw data
    with open(os.path.join(outdir, 'results.csv'), 'w') as f:
        f.write('nodes,rest_rps,rest_latency,grpc_rps,grpc_latency\n')
        for i in range(len(nodes)):
            f.write(f"{nodes[i]},{rest_rps[i]},{rest_lat[i]},{grpc_rps[i]},{grpc_lat[i]}\n")
    print(f"Raw results saved to {os.path.join(outdir, 'results.csv')}")

if __name__ == '__main__':
    print("Starting distributed system benchmark...")
    rest, grpc = run_experiment()
    plot_results(rest, grpc, outdir="benchmarking/experimental")
    print("Done.")
