import requests
import time

base_url = "http://nginx:8080"

def clear_queue():
    requests.post(f"{base_url}/clear")

def test_history():
    clear_queue()
    track = {"id": 1, "title": "HistorySong", "artist": "Hist", "duration": 99}
    requests.post(f"{base_url}/add_track", json=track)
    time.sleep(1)
    requests.post(f"{base_url}/play_next")
    time.sleep(1)
    resp = requests.get(f"{base_url}/history")
    assert resp.status_code == 200, "History endpoint failed"
    data = resp.json()
    assert any(t["id"] == 1 for t in data), "Track not in history after play_next"
    print("test_history: PASS")

if __name__ == "__main__":
    test_history()
