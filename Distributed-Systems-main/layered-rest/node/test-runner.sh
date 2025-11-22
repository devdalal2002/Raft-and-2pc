#!/bin/sh
set -e

# Wait for API to be ready
API_URL=${API_URL:-http://node:8000/queue}
for i in $(seq 1 30); do
  if curl -s "$API_URL" >/dev/null; then
    echo "API is ready!"
    break
  fi
  sleep 1
done

# Run all tests
python run_all_tests.py
