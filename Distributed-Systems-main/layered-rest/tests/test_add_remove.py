import requests
import time

base_url = "http://nginx:8080"

# Clean up: remove all tracks (if API supported, otherwise skip)
def clear_queue():
    requests.post(f"{base_url}/clear")

def test_add_remove():
    clear_queue()
    track = {"id": 1, "title": "Song1", "artist": "A", "duration": 100}
    r = requests.post(f"{base_url}/add_track", json=track)
    assert r.status_code == 200, "Add failed"
    queue = requests.get(f"{base_url}/queue").json()
    assert any(t["id"] == 1 for t in queue), "Track not in queue after add"
    r = requests.post(f"{base_url}/remove_track", json={"id": 1})
    assert r.status_code == 200, "Remove failed"
    queue = requests.get(f"{base_url}/queue").json()
    assert not any(t["id"] == 1 for t in queue), "Track still in queue after remove"
    print("test_add_remove: PASS")

if __name__ == "__main__":
    test_add_remove()
