import time

import pytest
from fastapi.testclient import TestClient

import api
from hall.subtitles import SubtitleFormatError, convert_subtitle, srt_to_vtt

SRT = """1
00:00:01,000 --> 00:00:04,000
你好，世界

2
00:01:02,500 --> 00:01:05,250
<i>第二行</i>
"""

VTT = """WEBVTT

00:00:01.000 --> 00:00:04.000
你好，世界
"""


def test_srt_conversion_drops_cue_numbers():
    result = srt_to_vtt(SRT)
    assert result.startswith("WEBVTT\n\n")
    assert "1\n00:00" not in result
    assert "00:00:01.000 --> 00:00:04.000" in result
    assert "00:01:02.500 --> 00:01:05.250" in result
    assert "你好，世界" in result
    assert "<i>第二行</i>" in result


def test_srt_without_hours_is_converted():
    result = srt_to_vtt("1\n00:01,000 --> 00:02,500\nshort\n")
    assert "00:01.000 --> 00:02.500" in result


def test_vtt_is_kept_verbatim():
    data = convert_subtitle("zh.vtt", VTT.encode("utf-8"))
    assert data == VTT.encode("utf-8")


def test_vtt_without_header_gets_one():
    data = convert_subtitle("zh.vtt", b"00:00:01.000 --> 00:00:02.000\nhi\n").decode()
    assert data.startswith("WEBVTT\n\n")


def test_gb18030_is_decoded_to_utf8():
    srt = "1\n00:00:00,000 --> 00:00:01,000\n中文字幕\n"
    data = convert_subtitle("zh.srt", srt.encode("gb18030"))
    assert "中文字幕" in data.decode("utf-8")


def test_ass_is_rejected():
    with pytest.raises(SubtitleFormatError):
        convert_subtitle("zh.ass", b"[Script Info]\n")


def test_unknown_extension_is_rejected():
    with pytest.raises(SubtitleFormatError):
        convert_subtitle("zh.txt", b"hello")


# -- API -----------------------------------------------------------------
class FakeRecordManager:
    async def init(self):
        pass

    async def close(self):
        pass

    async def list_records(self, name):
        return []


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api, "WebDavRecordManager", FakeRecordManager)
    with TestClient(api.app) as test_client:
        yield test_client


def _create_room(client):
    response = client.post(
        "/hall/api/rooms",
        json={
            "name": "subtitle room",
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


def _upload(client, room, name="zh.srt", lang="zh", body=None):
    payload = body if body is not None else SRT.encode("utf-8")
    return client.post(
        f"/hall/api/rooms/{room['room_id']}/subtitles",
        params={"name": name, "lang": lang},
        content=payload,
        headers={
            "X-Host-Token": room["host_token"],
            "Content-Type": "text/plain; charset=utf-8",
        },
    )


def test_upload_and_download_subtitle(client):
    room = _create_room(client)
    response = _upload(client, room)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["name"] == "zh.srt"
    assert data["lang"] == "zh"
    assert data["format"] == "vtt"
    assert data["url"].split("?")[0].endswith(".vtt")

    served = client.get(data["url"])
    assert served.status_code == 200
    assert served.headers["content-type"].startswith("text/vtt")
    assert served.headers["x-content-type-options"] == "nosniff"
    assert served.content.startswith(b"WEBVTT")
    assert "你好，世界".encode("utf-8") in served.content

    info = client.get(f"/hall/api/rooms/{room['room_id']}").json()
    assert info["subtitles"][0]["subtitle_id"] == data["subtitle_id"]
    assert info["active_subtitle_id"] == data["subtitle_id"]


def test_member_cannot_upload(client):
    room = _create_room(client)
    member = client.post(f"/hall/api/rooms/{room['room_id']}/join", json={"name": "alice"}).json()
    response = client.post(
        f"/hall/api/rooms/{room['room_id']}/subtitles",
        params={"name": "zh.srt"},
        content=SRT.encode("utf-8"),
        headers={"X-Host-Token": member["member_token"]},
    )
    assert response.status_code == 403


def test_subtitle_over_limit_is_413(client):
    room = _create_room(client)
    too_big = b"1\n00:00:00,000 --> 00:00:01,000\n" + b"A" * (10 * 1024 * 1024)
    response = _upload(client, room, body=too_big)
    assert response.status_code == 413
    assert response.json()["code"] == "SUBTITLE_TOO_LARGE"


def test_subtitle_near_limit_is_accepted(client):
    room = _create_room(client)
    prefix = b"1\n00:00:00,000 --> 00:00:01,000\n"
    body = prefix + b"A" * (9 * 1024 * 1024 - len(prefix) - 1) + b"\n"
    assert len(body) == 9 * 1024 * 1024
    response = _upload(client, room, body=body)
    assert response.status_code == 200, response.text
    assert response.json()["size"] >= 9 * 1024 * 1024


def test_ass_upload_returns_clear_error(client):
    room = _create_room(client)
    response = _upload(client, room, name="zh.ass", body=b"[Script Info]\n")
    assert response.status_code == 415
    assert response.json()["code"] == "SUBTITLE_FORMAT_UNSUPPORTED"
    assert "ASS" in response.json()["message"]


def test_signed_subtitle_url_expiry_and_tampering(client):
    room = _create_room(client)
    data = _upload(client, room).json()
    subtitle_id = data["subtitle_id"]
    url = data["url"]

    tampered = url.rsplit("sig=", 1)[0] + "sig=" + "0" * 64
    assert client.get(tampered).status_code == 403

    hall = client.app.state.hall
    expired = hall.auth.subtitle_url(room["room_id"], subtitle_id, exp=int(time.time()) - 10)
    assert client.get(expired).status_code == 403


def test_host_selects_subtitle_and_member_follows(client):
    room = _create_room(client)
    first = _upload(client, room, name="en.srt", lang="en").json()
    second = _upload(client, room, name="zh.srt", lang="zh").json()
    member = client.post(f"/hall/api/rooms/{room['room_id']}/join", json={"name": "bob"}).json()

    with client.websocket_connect(f"/hall/ws/{room['room_id']}") as host:
        host.send_json({"type": "auth", "role": "host", "token": room["host_token"]})
        auth = host.receive_json()
        assert auth["type"] == "auth_ok"
        assert {item["subtitle_id"] for item in auth["room"]["subtitles"]} == {
            first["subtitle_id"],
            second["subtitle_id"],
        }
        assert auth["room"]["active_subtitle_id"] == first["subtitle_id"]

        with client.websocket_connect(f"/hall/ws/{room['room_id']}") as member_ws:
            member_ws.send_json(
                {"type": "auth", "role": "member", "token": member["member_token"]}
            )
            assert member_ws.receive_json()["type"] == "auth_ok"

            host.send_json(
                {"type": "subtitle_select", "subtitle_id": second["subtitle_id"], "visible": True}
            )
            followed = None
            for _ in range(10):
                message = member_ws.receive_json()
                if (
                    message.get("type") == "room_state"
                    and message.get("active_subtitle_id") == second["subtitle_id"]
                ):
                    followed = message
                    break
            assert followed is not None
            assert followed["active_subtitle_visible"] is True

            member_ws.send_json(
                {"type": "subtitle_select", "subtitle_id": first["subtitle_id"], "visible": True}
            )
            error = None
            for _ in range(10):
                message = member_ws.receive_json()
                if message.get("type") == "error":
                    error = message
                    break
            assert error is not None
            assert error["code"] == "MEMBER_READONLY"


def test_delete_subtitle_clears_active(client):
    room = _create_room(client)
    data = _upload(client, room).json()
    response = client.delete(
        f"/hall/api/rooms/{room['room_id']}/subtitles/{data['subtitle_id']}",
        headers={"X-Host-Token": room["host_token"]},
    )
    assert response.status_code == 200
    info = client.get(f"/hall/api/rooms/{room['room_id']}").json()
    assert info["subtitles"] == []
    assert info["active_subtitle_id"] is None
    assert client.get(data["url"]).status_code == 404


def test_ended_room_rejects_subtitles(client):
    room = _create_room(client)
    data = _upload(client, room).json()
    client.delete(f"/hall/api/rooms/{room['room_id']}", headers={"X-Host-Token": room["host_token"]})
    assert client.get(data["url"]).status_code == 410
    assert _upload(client, room).status_code == 410
