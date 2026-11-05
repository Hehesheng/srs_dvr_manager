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


def _create_empty_room(client, name="playlist room"):
    response = client.post("/hall/api/rooms", json={"name": name})
    assert response.status_code == 200, response.text
    return response.json()


def _item(name, size=1_000_000):
    return {"kind": "host_file", "name": name, "size": size, "mime": "video/mp4"}


def _add(client, room, payload):
    return client.post(
        f"/hall/api/rooms/{room['room_code']}/playlist",
        json=payload,
        headers={"X-Host-Token": room["host_token"]},
    )


def test_room_can_start_without_media(monkeypatch):
    with _client(monkeypatch) as client:
        room = _create_empty_room(client)
        assert room["media"] is None
        assert room["media_id"] is None
        assert room["media_url"] is None
        assert room["room"]["playlist"] == []

        joined = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "alice"}
        )
        assert joined.status_code == 200
        body = joined.json()
        assert body["media"] is None and body["media_url"] is None
        assert body["room"]["playlist"] == []

        listed = client.get("/hall/api/rooms").json()["rooms"]
        listed_room = next(item for item in listed if item["room_code"] == room["room_code"])
        assert listed_room["media_name"] is None
        assert listed_room["playlist_count"] == 0

        # No media means no signed media URL can be served yet.
        assert client.get(f"/hall/media/{room['room_id']}/m_deadbeef?exp=1&sig=x").status_code in (403, 404)


def test_first_item_activates_and_others_wait(monkeypatch):
    with _client(monkeypatch) as client:
        room = _create_empty_room(client)
        first = _add(client, room, _item("first.mp4"))
        assert first.status_code == 200, first.text
        first_body = first.json()
        assert first_body["active"] is True
        assert first_body["media"]["name"] == "first.mp4"
        assert first_body["media_url"].startswith("/hall/media/")
        assert [item["active"] for item in first_body["playlist"]] == [True]

        second = _add(client, room, _item("second.mp4", 2_000_000))
        second_body = second.json()
        assert second_body["active"] is False
        assert second_body["media"]["name"] == "first.mp4"
        assert [item["name"] for item in second_body["playlist"]] == [
            "first.mp4",
            "second.mp4",
        ]
        assert [item["active"] for item in second_body["playlist"]] == [True, False]

        info = client.get(f"/hall/api/rooms/{room['room_code']}").json()
        assert info["media"]["name"] == "first.mp4"
        assert len(info["playlist"]) == 2

        client.delete(
            f"/hall/api/rooms/{room['room_code']}",
            headers={"X-Host-Token": room["host_token"]},
        )


def test_activate_and_remove_playlist_items(monkeypatch):
    with _client(monkeypatch) as client:
        room = _create_empty_room(client)
        first = _add(client, room, _item("first.mp4")).json()
        second = _add(client, room, _item("second.mp4")).json()
        first_url = first["media_url"]
        second_id = second["playlist"][1]["media_id"]

        activated = client.post(
            f"/hall/api/rooms/{room['room_code']}/playlist/{second_id}/activate",
            headers={"X-Host-Token": room["host_token"]},
        )
        assert activated.status_code == 200, activated.text
        body = activated.json()
        assert body["media"]["name"] == "second.mp4"
        assert [item["active"] for item in body["playlist"]] == [False, True]
        assert client.get(first_url, headers={"Range": "bytes=0-1"}).status_code == 404

        # Removing a non-active item leaves playback alone.
        first_id = first["playlist"][0]["media_id"]
        removed = client.delete(
            f"/hall/api/rooms/{room['room_code']}/playlist/{first_id}",
            headers={"X-Host-Token": room["host_token"]},
        )
        assert removed.status_code == 200, removed.text
        assert removed.json()["media"]["name"] == "second.mp4"
        assert [item["name"] for item in removed.json()["playlist"]] == ["second.mp4"]

        # Removing the active item falls back to the previous entry.
        extra = _add(client, room, _item("third.mp4")).json()
        third_id = extra["playlist"][1]["media_id"]
        active_removed = client.delete(
            f"/hall/api/rooms/{room['room_code']}/playlist/{third_id}",
            headers={"X-Host-Token": room["host_token"]},
        )
        assert active_removed.status_code == 200
        assert active_removed.json()["media"]["name"] == "second.mp4"

        # Removing the last item leaves the room empty but alive.
        last_id = active_removed.json()["playlist"][0]["media_id"]
        emptied = client.delete(
            f"/hall/api/rooms/{room['room_code']}/playlist/{last_id}",
            headers={"X-Host-Token": room["host_token"]},
        )
        assert emptied.status_code == 200
        assert emptied.json()["media"] is None
        assert emptied.json()["media_url"] is None
        assert emptied.json()["playlist"] == []
        assert client.get(f"/hall/api/rooms/{room['room_code']}").status_code == 200

        client.delete(
            f"/hall/api/rooms/{room['room_code']}",
            headers={"X-Host-Token": room["host_token"]},
        )


def test_members_cannot_edit_the_playlist(monkeypatch):
    with _client(monkeypatch) as client:
        room = _create_empty_room(client)
        member = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "bob"}
        ).json()

        denied = client.post(
            f"/hall/api/rooms/{room['room_code']}/playlist",
            json=_item("evil.mp4"),
            headers={"X-Host-Token": member["member_token"]},
        )
        assert denied.status_code == 403

        client.delete(
            f"/hall/api/rooms/{room['room_code']}",
            headers={"X-Host-Token": room["host_token"]},
        )


def test_change_media_appends_to_the_playlist(monkeypatch):
    with _client(monkeypatch) as client:
        room = _create_empty_room(client)
        first = _add(client, room, _item("first.mp4")).json()

        changed = client.post(
            f"/hall/api/rooms/{room['room_code']}/media",
            json=_item("replacement.mp4"),
            headers={"X-Host-Token": room["host_token"]},
        )
        assert changed.status_code == 200, changed.text
        body = changed.json()
        assert body["media"]["name"] == "replacement.mp4"
        assert [item["name"] for item in body["playlist"]] == [
            "first.mp4",
            "replacement.mp4",
        ]
        assert [item["active"] for item in body["playlist"]] == [False, True]
        assert first["media_url"] != body["media_url"]

        client.delete(
            f"/hall/api/rooms/{room['room_code']}",
            headers={"X-Host-Token": room["host_token"]},
        )
