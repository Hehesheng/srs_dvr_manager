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


def test_room_list_shows_active_rooms(monkeypatch):
    with _client(monkeypatch) as client:
        first = _create_room(client)
        second = _create_room(client)
        client.post(f"/hall/api/rooms/{second['room_code']}/join", json={"name": "bob"})

        listed = client.get("/hall/api/rooms").json()["rooms"]
        by_code = {room["room_code"]: room for room in listed}
        assert set(by_code) == {first["room_code"], second["room_code"]}
        assert by_code[second["room_code"]]["member_count"] == 1
        assert by_code[first["room_code"]]["host_online"] is False
        assert by_code[first["room_code"]]["media_name"] == "movie.mp4"
        assert "host_token" not in by_code[first["room_code"]]

        client.delete(
            f"/hall/api/rooms/{first['room_code']}",
            headers={"X-Host-Token": first["host_token"]},
        )
        remaining = client.get("/hall/api/rooms").json()["rooms"]
        assert [room["room_code"] for room in remaining] == [second["room_code"]]


def test_host_can_change_media(monkeypatch):
    with _client(monkeypatch) as client:
        room = _create_room(client)
        member = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "alice"}
        ).json()
        old_media_id = room["media_id"]
        old_media_url = room["media_url"]

        new_payload = {
            "kind": "host_file",
            "name": "second.mp4",
            "size": 2_000_000,
            "mime": "video/mp4",
        }
        denied = client.post(
            f"/hall/api/rooms/{room['room_code']}/media",
            json=new_payload,
            headers={"X-Host-Token": member["member_token"]},
        )
        assert denied.status_code == 403

        with client.websocket_connect(f"/hall/ws/{room['room_id']}") as ws:
            ws.send_json(
                {"type": "auth", "role": "member", "token": member["member_token"]}
            )
            assert ws.receive_json()["type"] == "auth_ok"

            changed = client.post(
                f"/hall/api/rooms/{room['room_code']}/media",
                json=new_payload,
                headers={"X-Host-Token": room["host_token"]},
            )
            assert changed.status_code == 200, changed.text
            data = changed.json()
            assert data["media"]["name"] == "second.mp4"
            assert data["media_id"] != old_media_id
            assert "/hall/media/" in data["media_url"]

            followed = None
            for _ in range(10):
                message = ws.receive_json()
                if (
                    message.get("type") == "room_state"
                    and message.get("media", {}).get("media_id") == data["media_id"]
                ):
                    followed = message
                    break
            assert followed is not None
            assert followed["media_url"].startswith("/hall/media/")
            assert followed["media"]["name"] == "second.mp4"

        info = client.get(f"/hall/api/rooms/{room['room_code']}").json()
        assert info["media"]["name"] == "second.mp4"
        assert client.get(old_media_url, headers={"Range": "bytes=0-1"}).status_code == 404

        client.delete(
            f"/hall/api/rooms/{room['room_code']}",
            headers={"X-Host-Token": room["host_token"]},
        )
        ended = client.post(
            f"/hall/api/rooms/{room['room_code']}/media",
            json=new_payload,
            headers={"X-Host-Token": room["host_token"]},
        )
        assert ended.status_code == 410


def test_host_name_is_shown_to_members(monkeypatch):
    with _client(monkeypatch) as client:
        room = client.post(
            "/hall/api/rooms",
            json={"name": "test room", "host_name": "  客厅  的  电视  "},
        ).json()
        # The browser remembers the name and hands it in at creation, so a new
        # room opens already labelled -- and tidied up on the way in.
        assert room["room"]["host_name"] == "客厅 的 电视"

        member = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "alice"}
        ).json()
        info = client.get(f"/hall/api/rooms/{room['room_code']}").json()
        assert info["host_name"] == "客厅 的 电视"

        with client.websocket_connect(f"/hall/ws/{room['room_id']}") as ws:
            ws.send_json(
                {"type": "auth", "role": "member", "token": member["member_token"]}
            )
            assert ws.receive_json()["type"] == "auth_ok"

            # A member may not rename the host, and neither may a stray token.
            denied = client.post(
                f"/hall/api/rooms/{room['room_code']}/host_name",
                json={"name": "我说了算"},
                headers={"X-Host-Token": member["member_token"]},
            )
            assert denied.status_code == 403

            renamed = client.post(
                f"/hall/api/rooms/{room['room_code']}/host_name",
                json={"name": "书房"},
                headers={"X-Host-Token": room["host_token"]},
            )
            assert renamed.status_code == 200, renamed.text
            assert renamed.json()["host_name"] == "书房"

            # Members see it without asking: the room state carries the name.
            seen = None
            for _ in range(10):
                message = ws.receive_json()
                if message.get("type") == "room_state" and message.get("host_name") == "书房":
                    seen = message
                    break
            assert seen is not None

            # Empty is a real choice: it means "call me 房主" again.
            cleared = client.post(
                f"/hall/api/rooms/{room['room_code']}/host_name",
                json={"name": "   "},
                headers={"X-Host-Token": room["host_token"]},
            )
            assert cleared.status_code == 200
            assert cleared.json()["host_name"] == ""

        assert client.get(f"/hall/api/rooms/{room['room_code']}").json()["host_name"] == ""


def test_host_name_length_is_capped(monkeypatch):
    with _client(monkeypatch) as client:
        room = _create_room(client)

        too_long = client.post(
            f"/hall/api/rooms/{room['room_code']}/host_name",
            json={"name": "房" * 33},
            headers={"X-Host-Token": room["host_token"]},
        )
        assert too_long.status_code == 400
        assert too_long.json()["code"] == "HOST_NAME_INVALID"

        # The rejected value must not have landed on the room.
        assert client.get(f"/hall/api/rooms/{room['room_code']}").json()["host_name"] == ""

        at_cap = client.post(
            f"/hall/api/rooms/{room['room_code']}/host_name",
            json={"name": "房" * 32},
            headers={"X-Host-Token": room["host_token"]},
        )
        assert at_cap.status_code == 200
        assert at_cap.json()["host_name"] == "房" * 32
