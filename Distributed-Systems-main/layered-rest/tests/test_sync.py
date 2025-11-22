import requests
import time

base_url = "http://nginx:8080"

def clear_queue():
    requests.post(f"{base_url}/clear")

def test_sync():
    clear_queue()
    track = {"id": 1, "title": "SyncTest", "artist": "A", "duration": 100}
    requests.post(f"{base_url}/add_track", json=track)
    time.sleep(3)
    # Check queue multiple times to hit different nodes
    all_ok = True
    for i in range(10):
        queue = requests.get(f"{base_url}/queue").json()
        if not any(t["id"] == 1 for t in queue):
            all_ok = False
            print(f"FAIL: Node {i+1} missing track")
        time.sleep(0.2)
    assert all_ok, "Not all nodes have the track after sync"
    print("test_sync: PASS")

if __name__ == "__main__":
    test_sync()
