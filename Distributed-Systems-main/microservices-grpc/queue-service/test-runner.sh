#!/bin/sh
set -e



# Wait for queue-service to be ready (load balancer for all replicas)
svc=queue-service:50051
echo "Waiting for $svc to be ready..."
for i in $(seq 1 30); do
  if python -c "import grpc; grpc.insecure_channel('$svc').channel_ready_future().result(timeout=1)" 2>/dev/null; then
    echo "$svc is ready!"
    break
  fi
  sleep 1
done

# Run the test runner script (Python)
echo "Running test suite..."
python3 /app/test_runner.py
