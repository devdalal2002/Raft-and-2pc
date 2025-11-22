import requests
import time

base_url = "http://nginx:8080"

def clear_queue():
    requests.post(f"{base_url}/clear")

def test_vote():
    clear_queue()
    tracks = [
        {"id": 1, "title": "Song1", "artist": "A", "duration": 100},
        {"id": 2, "title": "Song2", "artist": "B", "duration": 120}
    ]
    for t in tracks:
        requests.post(f"{base_url}/add_track", json=t)
    # Vote up track 2
    requests.post(f"{base_url}/vote", json={"id": 2}, params={"up": True})
    # Wait for sync
    time.sleep(2)
    queue = requests.get(f"{base_url}/queue").json()
    assert queue[0]["id"] == 2, "Voting did not move track to top"
    print("test_vote: PASS")

if __name__ == "__main__":
    test_vote()
