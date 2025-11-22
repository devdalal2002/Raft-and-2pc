#!/bin/sh
set -x

# Entrypoint script for dynamic peer discovery
#
# Usage:
# - By default, discovers peers as node-1, node-2, ... (excluding self) using Docker DNS.
# - If PEER_NODES is set in the environment, uses that value instead (comma-separated URLs).
#   Example: PEER_NODES="http://node-2:8000,http://node-3:8000"

# Wait for DNS to propagate
sleep 2

# Get the hostname of this container
SELF_HOSTNAME=$(hostname)



# If PEER_NODES is set in the environment, use it. Otherwise, use Docker Compose DNS round-robin.
if [ -n "$PEER_NODES" ]; then
  echo "[entrypoint] Using PEER_NODES from environment: $PEER_NODES"
else
  export PEER_NODES="http://node:8000"
  echo "[entrypoint] Using default PEER_NODES: $PEER_NODES"
fi

echo "[entrypoint] Starting FastAPI app with uvicorn..."
exec uvicorn main:app --host 0.0.0.0 --port 8000

