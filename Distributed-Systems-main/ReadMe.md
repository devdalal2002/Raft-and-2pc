# Distributed Systems Consensus Project

This project contains containerized implementations of two consensus algorithms: **Two-Phase Commit (2PC)** and **Raft**. Both protocols are implemented using gRPC for inter-node communication and Docker for containerized deployment.

---

## Two-Phase Commit (2PC)

- **Rebuild and start the 2PC cluster:**
  ```bash
  docker compose -f consensus/two_pc/docker-compose.yml up --build -d
  ```
- **Wait for cluster startup:** (optional)
  ```cmd
  timeout /t 5
  ```
- **Run a transaction (with participants 1,2,3,4):**
  ```bash
  python -m consensus.two_pc.manager localhost:6100 --participants 1 2 3 4
  ```
- **Check logs on coordinator and participant1:**
  ```bash
  docker logs twopc-coordinator
  docker logs twopc-participant1
  ```
- **Search logs for RPC messages (Phases and Nodes):**
  ```bash
  docker compose -f consensus/two_pc/docker-compose.yml logs | findstr "Phase"
  docker compose -f consensus/two_pc/docker-compose.yml logs | findstr "Node"
  ```
- **Shut down the 2PC cluster:**
  ```bash
  docker compose -f consensus/two_pc/docker-compose.yml down
  ```

Environment variables (e.g., `DEFAULT_VOTE=abort`) can be used on individual nodes to simulate abort scenarios.

---

## Raft Consensus Algorithm

- **Build Docker image:**
  ```bash
  docker compose -f consensus/raft/docker-compose.yml build
  ```
- **Start a 5-node Raft cluster:**
  ```bash
  docker compose -f consensus/raft/docker-compose.yml up -d
  ```
- **Run automated Raft tests:**
  ```bash
  python raft_automated_tests.py
  ```
- **Tear down the cluster:**
  ```bash
  docker compose -f consensus/raft/docker-compose.yml down
  ```

All consensus RPCs emit logs indicating node interactions for easy debugging.

---

## GitHub Repository

_Source code and project files are available at:_

`https://github.com/devdalal2002/Raft-and-2pc.git`  

---
