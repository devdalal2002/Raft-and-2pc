
import os
import requests
import redis
import json
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List


# Redis connection
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

app = FastAPI()

# Models
class Track(BaseModel):
    id: int
    title: str
    artist: str
    duration: int  # seconds
    votes: int = 0

class TrackAction(BaseModel):
    id: int


# Redis-backed storage
QUEUE_KEY = "music_queue"
HISTORY_KEY = "music_history"


def get_peers():
    peers = os.getenv("PEER_NODES", "")
    print(f"[DEBUG] PEER_NODES: {peers}")
    return [p.strip() for p in peers.split(",") if p.strip()]


def get_queue():
    data = redis_client.lrange(QUEUE_KEY, 0, -1)
    return [Track(**json.loads(item)) for item in data]

def set_queue(tracks: List[Track]):
    redis_client.delete(QUEUE_KEY)
    if tracks:
        redis_client.rpush(QUEUE_KEY, *[t.json() for t in tracks])

def get_history():
    data = redis_client.lrange(HISTORY_KEY, 0, -1)
    return [Track(**json.loads(item)) for item in data]

def add_to_history(track: Track):
    redis_client.rpush(HISTORY_KEY, track.json())

def broadcast_queue():
    peers = get_peers()
    queue = get_queue()
    print(f"[DEBUG] Broadcasting queue to peers: {peers}")
    for peer in peers:
        try:
            print(f"[DEBUG] Sending sync to {peer}/sync with queue: {[t.dict() for t in queue]}")
            resp = requests.post(f"{peer}/sync", json=[t.dict() for t in queue], timeout=3)
            print(f"[DEBUG] Sync response from {peer}: {resp.status_code}")
        except Exception as e:
            print(f"[ERROR] Failed to sync with {peer}: {e}")



@app.post("/add_track")
def add_track(track: Track):
    queue = get_queue()
    queue.append(track)
    # Always sort by votes descending, then by id ascending for stability
    queue.sort(key=lambda x: (-x.votes, x.id))
    set_queue(queue)
    broadcast_queue()
    return {"message": "Track added", "queue": queue}


@app.post("/remove_track")
def remove_track(action: TrackAction):
    queue = get_queue()
    queue = [t for t in queue if t.id != action.id]
    set_queue(queue)
    broadcast_queue()
    return {"message": "Track removed", "queue": queue}


@app.post("/vote")

@app.post("/vote")
def vote_track(action: TrackAction, up: bool = True):
    queue = get_queue()
    for t in queue:
        if t.id == action.id:
            t.votes += 1 if up else -1
    # Always sort by votes descending, then by id ascending for stability
    queue.sort(key=lambda x: (-x.votes, t.id))
    set_queue(queue)
    broadcast_queue()
    return {"queue": queue}


@app.get("/queue")
def api_get_queue():
    return get_queue()


@app.get("/metadata/{track_id}")
def get_metadata(track_id: int):
    queue = get_queue()
    for t in queue:
        if t.id == track_id:
            return t
    raise HTTPException(status_code=404, detail="Track not found")


@app.post("/play_next")
def play_next():
    queue = get_queue()
    if not queue:
        raise HTTPException(status_code=400, detail="Queue empty")
    track = queue.pop(0)
    set_queue(queue)
    add_to_history(track)
    broadcast_queue()
    return {"now_playing": track}


@app.get("/history")
def api_get_history():
    return get_history()


# Sync endpoint for receiving queue updates from peers
# Sync endpoint for receiving queue updates from peers
@app.post("/sync")
def sync_queue(new_queue: List[Track]):
    print(f"[DEBUG] Received sync: {new_queue}")
    queue = [Track(**t.dict()) if isinstance(t, Track) else Track(**t) for t in new_queue]
    set_queue(queue)
    print(f"[DEBUG] Queue after sync: {queue}")
    return {"message": "Queue synchronized", "queue": queue}


# Test utility endpoint to clear queue and history (for test isolation)
@app.post("/clear")
def clear_all():
    redis_client.delete(QUEUE_KEY)
    redis_client.delete(HISTORY_KEY)
    return {"message": "Queue and history cleared"}
