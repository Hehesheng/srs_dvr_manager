from fastapi.testclient import TestClient

import api


class FakeRecordManager:
    async def init(self):
        pass

    async def close(self):
        pass

    async def list_records(self, name):
        return []


def _client(monkeypatch):
    monkeypatch.setattr(api, "WebDavRecordManager", FakeRecordManager)
    return TestClient(api.app)


def _create_room(client):
    response = client.post(
        "/hall/api/rooms",
        json={
            "name": "test room",
            "media": {
                "kind": "host_file",
                "name": "movie.mp4",
                "size": 1_000_000,
                "mime": "video/mp4",
            },
        },
    )
    assert response.status_code == 200, response.text
    return response.json()


def test_room_lifecycle_and_signature(monkeypatch):
    with _client(monkeypatch) as client:
        data = _create_room(client)
        room_id = data["room_id"]
        code = data["room_code"]
        media_url = data["media_url"]

        assert client.get(f"/hall/api/rooms/{room_id}").status_code == 200
        assert client.get(f"/hall/api/rooms/{code}").status_code == 200

        joined = client.post(f"/hall/api/rooms/{code}/join", json={"name": "alice"})
        assert joined.status_code == 200
        assert joined.json()["media_url"].startswith("/hall/media/")

        bad = client.get(
            f"/hall/media/{room_id}/{data['media_id']}?exp=1&sig=nope",
            headers={"Range": "bytes=0-1"},
        )
        assert bad.status_code == 403

        assert client.head(media_url).status_code == 200
        assert client.head(media_url).headers["content-length"] == "1000000"

        # Host is not connected yet, so a Range request must fail cleanly.
        offline = client.get(media_url, headers={"Range": "bytes=0-1023"})
        assert offline.status_code == 503
        assert offline.json()["code"] == "SOURCE_OFFLINE"

        wrong = client.delete(f"/hall/api/rooms/{room_id}", headers={"X-Host-Token": "nope"})
        assert wrong.status_code == 403

        ended = client.delete(
            f"/hall/api/rooms/{room_id}", headers={"X-Host-Token": data["host_token"]}
        )
        assert ended.status_code == 200
        assert client.get(media_url, headers={"Range": "bytes=0-1"}).status_code == 410


def test_websocket_auth_and_member_readonly(monkeypatch):
    with _client(monkeypatch) as client:
        data = _create_room(client)
        room_id = data["room_id"]
        joined = client.post(f"/hall/api/rooms/{room_id}/join", json={"name": "bob"}).json()

        with client.websocket_connect(f"/hall/ws/{room_id}") as host:
            host.send_json({"type": "auth", "role": "host", "token": data["host_token"]})
            auth = host.receive_json()
            assert auth["type"] == "auth_ok" and auth["role"] == "host"

            with client.websocket_connect(f"/hall/ws/{room_id}") as member:
                member.send_json(
                    {"type": "auth", "role": "member", "token": joined["member_token"]}
                )
                assert member.receive_json()["type"] == "auth_ok"

                member.send_json({"type": "seek", "position": 99})
                error = None
                for _ in range(10):
                    message = member.receive_json()
                    if message.get("type") == "error":
                        error = message
                        break
                assert error is not None
                assert error["code"] == "MEMBER_READONLY"

                member.send_json({"type": "ping", "t0": 1234})
                pong = None
                for _ in range(10):
                    message = member.receive_json()
                    if message.get("type") == "pong":
                        pong = message
                        break
                assert pong is not None and pong["t0"] == 1234
