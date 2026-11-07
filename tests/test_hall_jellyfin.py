import asyncio
import time

import pytest
from fastapi.testclient import TestClient

import api
from hall.jellyfin import (
    HlsSession,
    JellyfinClient,
    JellyfinError,
    JellyfinSession,
    clean_vtt,
    playback_mode,
    subtitle_streams,
)
from hall.manager import HallConfig, HallManager, JellyfinConfig


class FakeRecordManager:
    async def init(self):
        pass

    async def close(self):
        pass

    async def list_records(self, name):
        return []


MASTER_TEXT = """#EXTM3U
#EXT-X-STREAM-INF:BANDWIDTH=6192000,CODECS="avc1.424029,mp4a.40.2",RESOLUTION=1280x720
live.m3u8?DeviceId=d1&MediaSourceId=ms-1&VideoCodec=h264&AudioCodec=aac
"""

LIVE_TEXT = """#EXTM3U
#EXT-X-VERSION:3
#EXT-X-TARGETDURATION:3
#EXT-X-MEDIA-SEQUENCE:0
#EXT-X-PLAYLIST-TYPE:EVENT
#EXTINF:3.003,
hls/abc/abc0.ts
#EXTINF:3.003,
hls/abc/abc1.ts
"""


class FakeJellyfinClient(JellyfinClient):
    """The real parsing logic, with every HTTP hop answered locally."""

    def __init__(self):
        super().__init__(None, "http://jelly.invalid", 8_000_000)
        self.playlist_calls = 0
        self.stopped = []
        self.opened = []
        self.profiles = []
        self.subtitle_calls = []
        self.item_calls = []

    async def _json(self, method, path, session=None, *, params=None, body=None, timeout=20.0):
        if method == "POST" and path.endswith("/PlaybackInfo"):
            self.opened.append((path, (body or {}).get("StartTimeTicks")))
            self.profiles.append((body or {}).get("DeviceProfile") or {})
            index = len(self.opened)
            return {
                "MediaSources": [
                    {
                        "Id": "ms-1",
                        "SupportsDirectPlay": False,
                        "TranscodingUrl": f"/videos/x/master.m3u8?DeviceId=d1&PlaySessionId=ps-{index}",
                        "PlaySessionId": f"ps-{index}",
                    }
                ],
                "PlaySessionId": f"ps-{index}",
            }
        if method == "DELETE":
            self.stopped.append(path)
            return {}
        if "master.m3u8" in path:
            return MASTER_TEXT
        if "live.m3u8" in path:
            self.playlist_calls += 1
            return LIVE_TEXT
        return {}

    async def authenticate(self, username, password, device_id):
        if password != "secret":
            raise JellyfinError("Jellyfin 用户名或密码不正确", "JELLYFIN_AUTH_FAILED", 401)
        return JellyfinSession(
            token="TOKEN-SECRET-VALUE",
            user_id="user-1",
            user_name=username,
            device_id=device_id,
        )

    async def verify(self, token, device_id, user_id=None):
        if token != "TOKEN-SECRET-VALUE":
            raise JellyfinError("Jellyfin 认证已失效，请重新登录", "JELLYFIN_AUTH_FAILED", 401)
        if user_id and user_id != "user-1":
            raise JellyfinError("Jellyfin 认证已失效，请重新登录", "JELLYFIN_AUTH_FAILED", 401)
        return JellyfinSession(
            token=token, user_id="user-1", user_name="hehesheng", device_id=device_id
        )

    async def libraries(self, session):
        return [{"id": "lib-1", "name": "电影", "collection_type": "movies"}]

    async def items(self, session, parent_id=None, query="", start=0, limit=60, mode="children"):
        self.item_calls.append(
            {
                "parent_id": parent_id,
                "query": query,
                "start": start,
                "limit": limit,
                "mode": mode,
            }
        )
        return {
            "items": [
                {
                    "id": "direct-1",
                    "name": "直接播放 (2024)",
                    "type": "Movie",
                    "kind": "movie",
                    "is_folder": False,
                    "mode": "direct",
                    "duration": 120.0,
                    "bitrate": 900_000,
                    "container": "mp4",
                },
                {
                    "id": "hls-1",
                    "name": "需要转码 (2021)",
                    "type": "Movie",
                    "kind": "movie",
                    "is_folder": False,
                    "mode": "hls",
                    "duration": 3600.0,
                    "bitrate": 15_000_000,
                    "container": "mkv",
                },
            ],
            "total": 2,
        }

    async def item(self, session, item_id):
        if item_id == "direct-1":
            return {
                "Id": item_id,
                "Name": "直接播放",
                "RunTimeTicks": 120 * 10_000_000,
                "MediaSources": [
                    {
                        "Id": "ms-direct",
                        "Container": "mp4",
                        "MediaStreams": [
                            {"Type": "Video", "Codec": "h264"},
                            {"Type": "Audio", "Codec": "aac"},
                            {
                                "Type": "Subtitle",
                                "Index": 1,
                                "Codec": "subrip",
                                "Language": "eng",
                                "DisplayTitle": "英语 - Eng - ASS",
                            },
                        ],
                    }
                ],
            }
        if item_id == "hls-1":
            return {
                "Id": item_id,
                "Name": "需要转码",
                "RunTimeTicks": 3600 * 10_000_000,
                "MediaSources": [
                    {
                        "Id": "ms-1",
                        "Container": "mkv",
                        "MediaStreams": [
                            {"Type": "Video", "Codec": "hevc"},
                            {"Type": "Audio", "Codec": "flac"},
                            {
                                "Type": "Subtitle",
                                "Index": 2,
                                "Codec": "ass",
                                "Language": "chi",
                                "DisplayTitle": "简体中文 - Chi - 默认 - ASS",
                                "IsDefault": True,
                            },
                            {
                                "Type": "Subtitle",
                                "Index": 3,
                                "Codec": "ass",
                                "Language": "zh-TW",
                                "DisplayTitle": "繁體中文 - Chi - ASS",
                            },
                            {
                                "Type": "Subtitle",
                                "Index": 4,
                                "Codec": "pgssub",
                                "Language": "chi",
                                "DisplayTitle": "图形字幕 - PGS",
                            },
                        ],
                    }
                ],
            }
        raise JellyfinError("Jellyfin 里找不到这个视频", "MEDIA_NOT_FOUND", 404)

    async def probe_size(self, session, item_id):
        return 8_890_000

    async def read_range(self, session, item_id, offset, length):
        return bytes([offset % 251]) * length

    async def read_segment(self, session, hls, index):
        return b"SEG" + bytes([index]) * 16

    async def subtitle_vtt(self, session, item_id, media_source_id, index):
        self.subtitle_calls.append((item_id, index))
        return "WEBVTT\n\n00:00:01.000 --> 00:00:02.000\n你好\n"


def _client(monkeypatch):
    monkeypatch.setattr(api, "WebDavRecordManager", FakeRecordManager)

    def factory(cls, config_path="config.yaml"):
        return cls(
            HallConfig(
                jellyfin=JellyfinConfig(
                    enabled=True,
                    url="http://jelly.invalid",
                    max_bitrate=8_000_000,
                    seek_debounce_ms=0,
                )
            )
        )

    monkeypatch.setattr(api.HallManager, "from_config", classmethod(factory))
    return TestClient(api.app)


def _hall(client):
    return client.app.state.hall


def _fake(client):
    fake = FakeJellyfinClient()
    _hall(client).jellyfin = fake
    return fake


def _create_room(client):
    response = client.post("/hall/api/rooms", json={"name": "jellyfin room"})
    assert response.status_code == 200, response.text
    return response.json()


def _login(client, room, password="secret"):
    return client.post(
        f"/hall/api/rooms/{room['room_code']}/jellyfin/session",
        json={"username": "hehesheng", "password": password},
        headers={"X-Host-Token": room["host_token"]},
    )


def _add(client, room, payload):
    return client.post(
        f"/hall/api/rooms/{room['room_code']}/playlist",
        json=payload,
        headers={"X-Host-Token": room["host_token"]},
    )


def _item(client, room, item_id):
    return _add(client, room, {"kind": "jellyfin", "item_id": item_id})


# -- pure helpers ------------------------------------------------------


def test_playback_mode_splits_between_direct_and_transcode():
    direct = {
        "Container": "mp4",
        "MediaStreams": [
            {"Type": "Video", "Codec": "h264"},
            {"Type": "Audio", "Codec": "aac"},
        ],
    }
    assert playback_mode(direct) == "direct"
    # AV1/AAC in MP4 is decodable by every browser we support.
    av1 = {
        "Container": "mp4",
        "MediaStreams": [
            {"Type": "Video", "Codec": "av1"},
            {"Type": "Audio", "Codec": "aac"},
        ],
    }
    assert playback_mode(av1) == "direct"
    hevc = {
        "Container": "mp4",
        "MediaStreams": [
            {"Type": "Video", "Codec": "hevc"},
            {"Type": "Audio", "Codec": "aac"},
        ],
    }
    assert playback_mode(hevc) == "hls"
    mkv = {
        "Container": "mkv",
        "MediaStreams": [
            {"Type": "Video", "Codec": "h264"},
            {"Type": "Audio", "Codec": "aac"},
        ],
    }
    assert playback_mode(mkv) == "hls"
    flac = {
        "Container": "mp4",
        "MediaStreams": [
            {"Type": "Video", "Codec": "h264"},
            {"Type": "Audio", "Codec": "flac"},
        ],
    }
    assert playback_mode(flac) == "hls"


def test_subtitle_streams_lists_image_tracks_and_cleans_their_labels():
    """Burned-in subtitles can come from PGS too, so no codec filter here."""
    source = {
        "MediaStreams": [
            {"Type": "Video", "Codec": "hevc"},
            {
                "Type": "Subtitle",
                "Index": 2,
                "Codec": "ass",
                "Language": "chi",
                "DisplayTitle": "简体中文 - Chi - 默认 - ASS",
                "IsDefault": True,
            },
            {
                "Type": "Subtitle",
                "Index": 4,
                "Codec": "pgssub",
                "Language": "chi",
                "DisplayTitle": "图形字幕 - PGS",
            },
        ]
    }
    tracks = subtitle_streams(source)
    assert [track["index"] for track in tracks] == [2, 4]
    assert tracks[0]["label"] == "简体中文（默认）"
    assert tracks[0]["is_default"] is True
    # The codec tail is dropped, and a PGS track is still offered: it can be
    # burned in even though it can never become a WebVTT track.
    assert tracks[1]["label"] == "图形字幕"
    assert tracks[1]["codec"] == "pgssub"


def test_burning_a_subtitle_rewrites_the_transcode_url_and_its_session():
    url = (
        "/videos/x/master.m3u8?DeviceId=d1&MediaSourceId=ms-1"
        "&SubtitleStreamIndex=2&SubtitleMethod=Encode&PlaySessionId=old"
    )
    rewritten, session_id = JellyfinClient._rewrite_transcode_subtitles(url, 3, False)
    assert "SubtitleStreamIndex=3" in rewritten
    assert "SubtitleMethod=Encode" in rewritten
    assert f"PlaySessionId={session_id}" in rewritten
    assert session_id and session_id != "old"
    assert "PlaySessionId=old" not in rewritten
    # MediaSourceId and DeviceId must survive: Jellyfin needs both.
    assert "MediaSourceId=ms-1" in rewritten
    assert "DeviceId=d1" in rewritten


def test_turning_subtitles_off_drops_the_burn_in_parameters():
    url = "/videos/x/master.m3u8?DeviceId=d1&SubtitleStreamIndex=2&SubtitleMethod=Encode&PlaySessionId=old"
    rewritten, session_id = JellyfinClient._rewrite_transcode_subtitles(url, None, True)
    assert "SubtitleMethod" not in rewritten
    assert "SubtitleStreamIndex" not in rewritten
    assert f"PlaySessionId={session_id}" in rewritten
    # Nothing to do when the URL never asked for a subtitle.
    plain = "/videos/x/master.m3u8?DeviceId=d1&PlaySessionId=old"
    assert JellyfinClient._rewrite_transcode_subtitles(plain, None, True) == (plain, None)


def test_the_profile_that_turns_subtitles_off_asks_for_none_to_be_burned():
    client = JellyfinClient(None, "http://jelly.invalid", 8_000_000)
    # The default profile says "this client cannot show any subtitle", which
    # is what makes Jellyfin draw the account's default track into the picture.
    assert client._device_profile()["SubtitleProfiles"] == []
    external = client._device_profile(subtitles_off=True)["SubtitleProfiles"]
    assert external, "turning subtitles off must declare the text formats"
    assert {profile["Method"] for profile in external} == {"External"}


def test_playlist_lists_the_whole_movie_so_a_member_can_be_seeked_into_it():
    """Jellyfin hands out a complete VOD playlist; we must not window it.

    Its segments are produced on demand, so the live edge is meaningless —
    publishing only the tail would drop a member at the end of the film.
    """
    many = "#EXTM3U\n#EXT-X-TARGETDURATION:3\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n"
    many += "".join(f"#EXTINF:3.003,\nhls/abc/abc{i}.ts\n" for i in range(150))
    many += "#EXT-X-ENDLIST\n"

    class LongMovie(FakeJellyfinClient):
        async def _json(self, method, path, session=None, **kwargs):
            return many

    client = LongMovie()
    hls = HlsSession(
        item_id="x",
        media_source_id="ms-1",
        play_session_id="ps-1",
        live_url="http://jelly.invalid/videos/x/live.m3u8",
        start_seconds=0.0,
    )
    import asyncio

    session = JellyfinSession(token="t", user_id="u", user_name="n", device_id="d")
    text = asyncio.run(client.render_playlist(session, hls, lambda index: f"SEG{index}"))
    published = [line for line in text.splitlines() if line.startswith("SEG")]
    assert len(published) == 150
    assert published[0] == "SEG0"
    assert published[-1] == "SEG149"
    assert "#EXT-X-MEDIA-SEQUENCE:0" in text
    # A finished playlist is handed over as VOD, so hls.js fetches it once and
    # lets the player seek anywhere inside it instead of chasing a live edge.
    assert "#EXT-X-PLAYLIST-TYPE:VOD" in text
    assert text.rstrip().splitlines()[-1] == "#EXT-X-ENDLIST"
    # Real per-segment durations, not the target duration repeated.
    assert hls.durations[7] == 3.003
    assert "#EXTINF:3.003," in text
    # Every segment stays addressable for the life of the transcode.
    assert len(hls.segments) == 150


def test_a_listed_segment_that_is_not_written_yet_is_waited_for():
    """Jellyfin lists the whole film before it has produced all of it."""

    class SlowEncoder(JellyfinClient):
        def __init__(self):
            super().__init__(None, "http://jelly.invalid", 8_000_000)
            self.calls = 0

        async def _json(self, method, path, session=None, **kwargs):
            return LIVE_TEXT

    class Response:
        def __init__(self, status, body=b""):
            self.status = status
            self._body = body

        async def read(self):
            return self._body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

    class SlowSession:
        def __init__(self):
            self.calls = 0

        def get(self, url, **kwargs):
            self.calls += 1
            if self.calls < 3:
                return Response(404)
            return Response(200, b"TS-DATA")

    import asyncio

    client = SlowEncoder()
    client.session = SlowSession()
    hls = HlsSession(
        item_id="x",
        media_source_id="ms-1",
        play_session_id="ps-1",
        live_url="http://jelly.invalid/videos/x/live.m3u8",
        start_seconds=0.0,
    )
    session = JellyfinSession(token="t", user_id="u", user_name="n", device_id="d")
    hls.segments = {0: "http://jelly.invalid/hls/abc/abc0.ts"}
    assert asyncio.run(client.read_segment(session, hls, 0)) == b"TS-DATA"
    assert client.session.calls == 3

    with pytest.raises(JellyfinError) as excinfo:
        asyncio.run(client.read_segment(session, hls, 9))
    assert excinfo.value.status_code == 404


def test_clean_vtt_drops_ass_overrides_and_region_header():
    raw = (
        "﻿WEBVTT\n\n"
        "Region: id:subtitle width:80% lines:3\n\n"
        "00:05:02.590 --> 00:05:07.590 region:subtitle line:90%\n"
        "{\\fad(200,200)}{\\an8}翻译：某人\n\n"
        "00:05:08.000 --> 00:05:09.000\n"
        "<font color=\"#ffffff\">第二句</font>\n"
    )
    cleaned = clean_vtt(raw)
    assert cleaned.startswith("WEBVTT")
    assert "Region:" not in cleaned
    assert "\\fad" not in cleaned and "\\an8" not in cleaned
    assert "line:90%" not in cleaned
    assert "翻译：某人" in cleaned
    assert "第二句" in cleaned
    assert "00:05:02.590 --> 00:05:07.590" in cleaned


# -- authentication ----------------------------------------------------


def test_jellyfin_needs_login_before_items_can_be_added(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        denied = _item(client, room, "direct-1")
        assert denied.status_code == 401
        assert denied.json()["code"] == "JELLYFIN_AUTH_REQUIRED"

        status_body = client.get(f"/hall/api/rooms/{room['room_code']}/jellyfin/status").json()
        assert status_body["enabled"] is True
        assert status_body["authenticated"] is False


def test_jellyfin_login_rejects_a_wrong_password(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        bad = _login(client, room, password="nope")
        assert bad.status_code == 401
        assert bad.json()["code"] == "JELLYFIN_AUTH_FAILED"


def test_jellyfin_login_returns_libraries_and_never_the_token(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        ok = _login(client, room)
        assert ok.status_code == 200, ok.text
        body = ok.json()
        assert body["jellyfin"]["user_name"] == "hehesheng"
        assert [lib["name"] for lib in body["libraries"]] == ["电影"]
        # The host gets its own token back so the browser can remember it.
        assert body["token"] == "TOKEN-SECRET-VALUE"

        # ...but nothing else in the hall may repeat it.
        info = client.get(f"/hall/api/rooms/{room['room_code']}")
        member = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "bob"}
        )
        listed = client.get("/hall/api/rooms")
        for response in (info, member, listed):
            assert "TOKEN-SECRET-VALUE" not in response.text

        status_body = client.get(f"/hall/api/rooms/{room['room_code']}/jellyfin/status").json()
        assert status_body["authenticated"] is True
        assert "TOKEN-SECRET-VALUE" not in str(status_body)


def test_jellyfin_browse_requires_a_session(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        headers = {"X-Host-Token": room["host_token"]}
        listing = client.get(
            f"/hall/api/rooms/{room['room_code']}/jellyfin/items", headers=headers
        )
        assert listing.status_code == 401

        _login(client, room)
        listing = client.get(
            f"/hall/api/rooms/{room['room_code']}/jellyfin/items", headers=headers
        )
        assert listing.status_code == 200
        body = listing.json()
        assert [item["mode"] for item in body["items"]] == ["direct", "hls"]


def _project(payload):
    client = JellyfinClient(None, "http://jelly.invalid", 1)
    return client._project_items(payload)


def test_projecting_a_folder_offers_navigation_not_playback():
    body = _project(
        {
            "Items": [
                {
                    "Id": "f1",
                    "Name": "埃罗芒阿老师1080p片源",
                    "Type": "Folder",
                    "IsFolder": True,
                    "ChildCount": 12,
                    "RecursiveItemCount": 12,
                }
            ],
            "TotalRecordCount": 1,
        }
    )
    item = body["items"][0]
    assert item["is_folder"] is True
    assert item["kind"] == "folder"
    assert item["child_count"] == 12
    # A folder has no file of its own, so it must not look playable.
    assert item["mode"] is None


def test_projecting_a_series_counts_episodes_not_seasons():
    body = _project(
        {
            "Items": [
                {
                    "Id": "s1",
                    "Name": "86-不存在的战区-",
                    "Type": "Series",
                    "IsFolder": True,
                    "ChildCount": 1,
                    "RecursiveItemCount": 23,
                }
            ],
            "TotalRecordCount": 1,
        }
    )
    item = body["items"][0]
    assert item["kind"] == "series"
    assert item["child_count"] == 23
    assert item["mode"] is None


def test_projecting_an_episode_carries_its_series_and_numbers():
    body = _project(
        {
            "Items": [
                {
                    "Id": "e1",
                    "Name": "张超强闯地铁站",
                    "Type": "Episode",
                    "IsFolder": False,
                    "SeriesId": "s1",
                    "SeriesName": "沉默的真相",
                    "ParentIndexNumber": 2,
                    "IndexNumber": 5,
                    "RunTimeTicks": 600 * 10_000_000,
                    "MediaSources": [
                        {
                            "Container": "mkv",
                            "MediaStreams": [
                                {"Type": "Video", "Codec": "hevc"},
                                {"Type": "Audio", "Codec": "aac"},
                            ],
                        }
                    ],
                }
            ],
            "TotalRecordCount": 1,
        }
    )
    item = body["items"][0]
    assert item["kind"] == "episode"
    assert item["is_folder"] is False
    # A scraper-guessed year on every episode only makes the list noisy.
    assert item["name"] == "张超强闯地铁站"
    assert item["series_id"] == "s1"
    assert item["series_name"] == "沉默的真相"
    assert (item["season"], item["episode"]) == (2, 5)
    assert item["mode"] == "hls"


def test_projecting_a_plain_movie_stays_playable():
    body = _project(
        {
            "Items": [
                {
                    "Id": "m1",
                    "Name": "楚门的世界",
                    "Type": "Movie",
                    "IsFolder": False,
                    "MediaSources": [
                        {
                            "Container": "mp4",
                            "MediaStreams": [
                                {"Type": "Video", "Codec": "h264"},
                                {"Type": "Audio", "Codec": "aac"},
                            ],
                        }
                    ],
                }
            ],
            "TotalRecordCount": 1,
        }
    )
    item = body["items"][0]
    assert item["kind"] == "movie"
    assert item["mode"] == "direct"
    assert item["child_count"] == 0


def test_a_short_file_does_not_advertise_a_guessed_year():
    payload = {
        "Items": [
            {
                "Id": "m2",
                "Name": "埃罗芒阿老师 03",
                "Type": "Movie",
                "ProductionYear": 2013,
                "RunTimeTicks": 23 * 60 * 10_000_000,
                "MediaSources": [
                    {
                        "Container": "mp4",
                        "MediaStreams": [
                            {"Type": "Video", "Codec": "h264"},
                            {"Type": "Audio", "Codec": "aac"},
                        ],
                    }
                ],
            },
            {
                "Id": "m3",
                "Name": "楚门的世界",
                "Type": "Movie",
                "ProductionYear": 1998,
                "RunTimeTicks": 103 * 60 * 10_000_000,
                "MediaSources": [],
            },
        ],
        "TotalRecordCount": 2,
    }
    short, film = _project(payload)["items"]
    assert short["name"] == "埃罗芒阿老师 03"
    assert film["name"] == "楚门的世界 (1998)"


def _recording_client(payload=None):
    """A client whose HTTP layer is captured instead of sent."""
    client = JellyfinClient(None, "http://jelly.invalid", 1)
    calls = []

    async def fake_json(method, path, session=None, *, params=None, body=None, timeout=20.0):
        calls.append(params or {})
        return payload if payload is not None else {"Items": [], "TotalRecordCount": 0}

    client._json = fake_json
    session = JellyfinSession(token="t", user_id="u", user_name="n", device_id="d")
    return client, session, calls


def test_children_mode_lists_one_folder_without_a_type_filter():
    client, session, calls = _recording_client()
    asyncio.run(client.items(session, "lib-1"))
    params = calls[-1]
    assert params["Recursive"] == "false"
    assert params["ParentId"] == "lib-1"
    assert params["SortBy"] == "IsFolder,SortName"
    # A user root answers with UserView/CollectionFolder, so a type filter
    # here would empty the list.
    assert "IncludeItemTypes" not in params
    assert "ChildCount" in params["Fields"]


def test_episodes_mode_flattens_a_series_and_keeps_loose_movies():
    client, session, calls = _recording_client()
    asyncio.run(client.items(session, "series-1", mode="episodes"))
    params = calls[-1]
    assert params["Recursive"] == "true"
    assert params["SortBy"].startswith("ParentIndexNumber")
    types = params["IncludeItemTypes"].split(",")
    assert "Episode" in types and "Movie" in types


def test_search_is_recursive_offers_folders_and_puts_them_first():
    payload = {
        "Items": [
            {"Id": "m1", "Name": "埃罗芒阿老师 01", "Type": "Movie", "IsFolder": False},
            {"Id": "f1", "Name": "埃罗芒阿老师1080p片源", "Type": "Folder", "IsFolder": True},
        ],
        "TotalRecordCount": 2,
    }
    client, session, calls = _recording_client(payload)
    body = asyncio.run(client.items(session, "lib-1", query="埃罗芒阿"))
    params = calls[-1]
    assert params["Recursive"] == "true"
    assert params["SearchTerm"] == "埃罗芒阿"
    assert "Folder" in params["IncludeItemTypes"].split(",")
    assert [item["id"] for item in body["items"]] == ["f1", "m1"]


def test_browse_mode_reaches_the_client_intact(monkeypatch):
    with _client(monkeypatch) as client:
        fake = _fake(client)
        room = _create_room(client)
        _login(client, room)
        url = f"/hall/api/rooms/{room['room_code']}/jellyfin/items"
        headers = {"X-Host-Token": room["host_token"]}

        assert client.get(url, headers=headers).status_code == 200
        assert fake.item_calls[-1]["mode"] == "children"

        assert client.get(f"{url}?mode=episodes&library=s1", headers=headers).status_code == 200
        assert fake.item_calls[-1]["mode"] == "episodes"
        assert fake.item_calls[-1]["parent_id"] == "s1"

        # Anything else falls back to walking the tree.
        assert client.get(f"{url}?mode=nonsense", headers=headers).status_code == 200
        assert fake.item_calls[-1]["mode"] == "children"


def test_adding_a_whole_series_is_rejected_clearly(monkeypatch):
    with _client(monkeypatch) as client:
        fake = _fake(client)
        room = _create_room(client)
        _login(client, room)

        async def container_item(session, item_id):
            return {"Id": item_id, "Name": "沉默的真相", "Type": "Series", "IsFolder": True}

        monkeypatch.setattr(fake, "item", container_item)
        response = _item(client, room, "series-1")
        assert response.status_code == 400
        assert "剧集" in response.json()["message"]


def test_jellyfin_library_is_visible_only_to_the_host(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        _login(client, room)
        member = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "bob"}
        ).json()
        denied = client.get(
            f"/hall/api/rooms/{room['room_code']}/jellyfin/items",
            headers={"X-Host-Token": member["member_token"]},
        )
        assert denied.status_code == 403
        anonymous = client.get(f"/hall/api/rooms/{room['room_code']}/jellyfin/items")
        assert anonymous.status_code == 403


def test_jellyfin_remembers_a_token_instead_of_the_password(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        headers = {"X-Host-Token": room["host_token"]}
        adopted = client.post(
            f"/hall/api/rooms/{room['room_code']}/jellyfin/session",
            json={"token": "TOKEN-SECRET-VALUE", "user_id": "user-1"},
            headers=headers,
        )
        assert adopted.status_code == 200, adopted.text
        assert adopted.json()["jellyfin"]["user_id"] == "user-1"

        stale = client.post(
            f"/hall/api/rooms/{room['room_code']}/jellyfin/session",
            json={"token": "EXPIRED-TOKEN", "user_id": "user-1"},
            headers=headers,
        )
        assert stale.status_code == 401
        assert stale.json()["code"] == "JELLYFIN_AUTH_FAILED"


def test_jellyfin_login_is_host_only(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        member = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "bob"}
        ).json()
        denied = client.post(
            f"/hall/api/rooms/{room['room_code']}/jellyfin/session",
            json={"username": "hehesheng", "password": "secret"},
            headers={"X-Host-Token": member["member_token"]},
        )
        assert denied.status_code == 403


# -- direct playback ---------------------------------------------------


def test_jellyfin_direct_item_streams_through_the_hall_relay(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        _login(client, room)

        added = _item(client, room, "direct-1")
        assert added.status_code == 200, added.text
        body = added.json()
        assert body["media"]["kind"] == "jellyfin"
        assert body["media"]["playback_mode"] == "direct"
        assert body["media"]["name"] == "直接播放"
        assert body["media"]["size"] == 8_890_000
        assert body["media_url"].startswith("/hall/media/")

        response = client.get(body["media_url"], headers={"Range": "bytes=0-1023"})
        assert response.status_code == 206
        assert response.headers["content-range"] == "bytes 0-1023/8890000"
        assert len(response.content) == 1024


# -- transcoded playback -----------------------------------------------


def test_jellyfin_transcoded_item_serves_a_rewritten_playlist(monkeypatch):
    with _client(monkeypatch) as client:
        fake = _fake(client)
        room = _create_room(client)
        _login(client, room)

        added = _item(client, room, "hls-1")
        assert added.status_code == 200, added.text
        body = added.json()
        assert body["media"]["playback_mode"] == "hls"
        assert body["media"]["duration"] == 3600.0
        assert body["media_url"].endswith("live.m3u8?epoch=1&exp=" + body["media_url"].split("exp=")[1])
        assert body["media_url"].startswith("/hall/media/")
        assert room["host_token"] not in body["media_url"]

        playlist = client.get(body["media_url"])
        assert playlist.status_code == 200, playlist.text
        text = playlist.text
        # Upstream URIs never reach the browser: every segment points back at us.
        assert "jelly.invalid" not in text
        assert "api_key" not in text
        assert "hls/abc/abc0.ts" not in text
        seg_lines = [line for line in text.splitlines() if line and not line.startswith("#")]
        assert len(seg_lines) == 2
        assert seg_lines[0].startswith(f"/hall/media/{room['room_id']}/")
        assert "/seg/0.ts?" in seg_lines[0]

        segment = client.get(seg_lines[0])
        assert segment.status_code == 200, segment.text
        assert segment.content == b"SEG" + bytes([0]) * 16
        assert segment.headers["content-type"] == "video/mp2t"

        # The transcoding session was opened once, from the very beginning.
        assert fake.opened == [("/Items/hls-1/PlaybackInfo", 0)]


def test_hls_urls_cannot_be_forged(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        _login(client, room)
        body = _item(client, room, "hls-1").json()
        url = body["media_url"]

        # A playlist signed for another epoch belongs to a stream that is gone.
        assert client.get(url.replace("epoch=1", "epoch=2")).status_code == 403
        # An empty or truncated signature is refused rather than accepted.
        assert client.get(url.split("&sig=")[0] + "&sig=").status_code == 403
        assert client.get(url[:-1]).status_code == 403
        # A media id the room is not playing is refused outright.
        assert client.get(url.replace("m_", "m_0")).status_code == 404
        # The untouched URL still works.
        assert client.get(url).status_code == 200


def test_jellyfin_seek_restarts_the_transcode(monkeypatch):
    with _client(monkeypatch) as client:
        fake = _fake(client)
        room = _create_room(client)
        _login(client, room)
        body = _item(client, room, "hls-1").json()
        old_url = body["media_url"]
        assert "epoch=1" in old_url

        seek = client.post(
            f"/hall/api/rooms/{room['room_code']}/hls/seek",
            json={"position": 1800.0},
            headers={"X-Host-Token": room["host_token"]},
        )
        assert seek.status_code == 200, seek.text

        deadline = time.time() + 5
        epoch = 0
        while time.time() < deadline:
            info = client.get(f"/hall/api/rooms/{room['room_code']}").json()
            epoch = info["media_epoch"]
            if epoch == 2:
                break
            time.sleep(0.05)
        assert epoch == 2, "the transcode was not restarted"
        new_url = info["media_url"]
        assert new_url != old_url
        assert "epoch=2" in new_url
        # The player needs to know where in the item the new stream starts.
        assert info["media_start"] == 1800.0

        # The restart began halfway through, and the previous encoder stopped.
        assert fake.opened[-1] == ("/Items/hls-1/PlaybackInfo", 1800 * 10_000_000)
        assert fake.stopped, "the previous transcoding session was left running"

        # A playlist from before the seek is refused rather than reused.
        stale = client.get(old_url)
        assert stale.status_code == 409
        assert client.get(new_url).status_code == 200


def test_jellyfin_seek_is_host_only(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        _login(client, room)
        _item(client, room, "hls-1")
        member = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "bob"}
        ).json()
        denied = client.post(
            f"/hall/api/rooms/{room['room_code']}/hls/seek",
            json={"position": 10.0},
            headers={"X-Host-Token": member["member_token"]},
        )
        assert denied.status_code == 403


def _wait_epoch(client, room, wanted, timeout=5.0):
    deadline = time.time() + timeout
    info = None
    while time.time() < deadline:
        info = client.get(f"/hall/api/rooms/{room['room_code']}").json()
        if info["media_epoch"] == wanted:
            return info
        time.sleep(0.05)
    return info


def test_host_picks_the_burned_in_subtitle_and_everyone_follows(monkeypatch):
    with _client(monkeypatch) as client:
        fake = _fake(client)
        room = _create_room(client)
        _login(client, room)
        _item(client, room, "hls-1")

        state = client.get(f"/hall/api/rooms/{room['room_code']}").json()
        # The tracks are published so the host's CC menu can list them, even
        # though a transcoded item never becomes a real WebVTT track.
        assert [track["index"] for track in state["jellyfin_subtitles"]] == [2, 3, 4]
        assert state["jellyfin_subtitle"] is None

        pick = client.post(
            f"/hall/api/rooms/{room['room_code']}/hls/subtitle",
            json={"subtitle": 3, "position": 600.0},
            headers={"X-Host-Token": room["host_token"]},
        )
        assert pick.status_code == 200, pick.text
        assert pick.json()["subtitle"] == 3

        info = _wait_epoch(client, room, 2)
        assert info["media_epoch"] == 2, "picking a subtitle must restart the transcode"
        assert info["jellyfin_subtitle"] == 3
        assert info["jellyfin_subtitles_off"] is False
        assert info["media_start"] == 600.0
        assert fake.opened[-1] == ("/Items/hls-1/PlaybackInfo", 600 * 10_000_000)
        assert fake.stopped, "the previous transcoding session was left running"

        session = _hall(client).rooms[room["room_id"]].hls
        assert session.subtitle_index == 3
        assert session.play_session_id not in ("", "ps-1")
        # The chosen track has to survive a later seek.
        seek = client.post(
            f"/hall/api/rooms/{room['room_code']}/hls/seek",
            json={"position": 900.0},
            headers={"X-Host-Token": room["host_token"]},
        )
        assert seek.status_code == 200
        info = _wait_epoch(client, room, 3)
        assert info["media_epoch"] == 3
        assert _hall(client).rooms[room["room_id"]].hls.subtitle_index == 3


def test_subtitles_can_be_turned_off_for_the_transcode(monkeypatch):
    with _client(monkeypatch) as client:
        fake = _fake(client)
        room = _create_room(client)
        _login(client, room)
        _item(client, room, "hls-1")

        off = client.post(
            f"/hall/api/rooms/{room['room_code']}/hls/subtitle",
            json={"subtitle": "off", "position": 30.0},
            headers={"X-Host-Token": room["host_token"]},
        )
        assert off.status_code == 200, off.text
        info = _wait_epoch(client, room, 2)
        assert info["media_epoch"] == 2
        assert info["jellyfin_subtitles_off"] is True
        assert info["jellyfin_subtitle"] is None
        # Off means the profile declares the text formats as external, so
        # there is nothing for ffmpeg to burn in.
        profile = fake.profiles[-1]
        assert {p["Method"] for p in profile["SubtitleProfiles"]} == {"External"}
        assert _hall(client).rooms[room["room_id"]].hls.subtitles_off is True


def test_subtitle_choice_is_host_only(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        _login(client, room)
        _item(client, room, "hls-1")
        member = client.post(
            f"/hall/api/rooms/{room['room_code']}/join", json={"name": "bob"}
        ).json()
        denied = client.post(
            f"/hall/api/rooms/{room['room_code']}/hls/subtitle",
            json={"subtitle": 3, "position": 10.0},
            headers={"X-Host-Token": member["member_token"]},
        )
        assert denied.status_code == 403


def test_an_unknown_subtitle_track_is_refused(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        _login(client, room)
        _item(client, room, "hls-1")
        bad = client.post(
            f"/hall/api/rooms/{room['room_code']}/hls/subtitle",
            json={"subtitle": 99, "position": 10.0},
            headers={"X-Host-Token": room["host_token"]},
        )
        assert bad.status_code == 400


def test_a_direct_item_loads_vtt_tracks_but_a_transcoded_one_does_not(monkeypatch):
    with _client(monkeypatch) as client:
        fake = _fake(client)
        room = _create_room(client)
        _login(client, room)
        _item(client, room, "direct-1")

        deadline = time.time() + 5
        state = None
        while time.time() < deadline:
            state = client.get(f"/hall/api/rooms/{room['room_code']}").json()
            if state["subtitles"]:
                break
            time.sleep(0.05)
        assert state["subtitles"], "direct play must load the item's text tracks"
        assert fake.subtitle_calls == [("direct-1", 1)]
        assert state["jellyfin_subtitles"] == []

        # A transcoded item has its subtitle burned into the picture, so no
        # WebVTT track is fetched (that would draw a second copy on top).
        fake.subtitle_calls.clear()
        _item(client, room, "hls-1")
        time.sleep(0.3)
        assert fake.subtitle_calls == []


def test_switching_a_playlist_back_to_a_transcoded_item_keeps_its_subtitles(monkeypatch):
    """Switching items must re-publish the new item's subtitle state.

    The tracks used to be captured when the item was *added*, so returning to
    a transcoded item left the CC menu empty: the direct item had just written
    an empty track list over it.
    """
    with _client(monkeypatch) as client:
        fake = _fake(client)
        room = _create_room(client)
        _login(client, room)
        code = room["room_code"]
        headers = {"X-Host-Token": room["host_token"]}

        def activate(media_id):
            response = client.post(f"/hall/api/rooms/{code}/playlist/{media_id}/activate", headers=headers)
            assert response.status_code == 200, response.text
            return response.json()

        first = _item(client, room, "hls-1").json()
        assert [track["index"] for track in first["room"]["jellyfin_subtitles"]] == [2, 3, 4]

        # A direct item has no burn-in, so its WebVTT track replaces the list.
        direct = _item(client, room, "direct-1").json()
        body = activate(direct["media_id"])
        assert body["media"]["playback_mode"] == "direct"
        assert body["room"]["jellyfin_subtitles"] == []

        # Back to the transcoded item: the tracks must come back, and the host
        # must be able to pick one again.
        body = activate(first["media_id"])
        assert body["media"]["playback_mode"] == "hls"
        assert [track["index"] for track in body["room"]["jellyfin_subtitles"]] == [2, 3, 4]
        assert body["room"]["jellyfin_subtitle"] is None
        assert body["room"]["jellyfin_subtitles_off"] is False

        epoch = body["room"]["media_epoch"]
        pick = client.post(
            f"/hall/api/rooms/{code}/hls/subtitle",
            json={"subtitle": 3, "position": 30.0},
            headers=headers,
        )
        assert pick.status_code == 200, pick.text
        assert pick.json()["subtitle"] == 3
        info = _wait_epoch(client, room, epoch + 1)
        assert info["jellyfin_subtitle"] == 3
        assert _hall(client).rooms[room["room_id"]].hls.subtitle_index == 3
        assert fake.opened[-1][0] == "/Items/hls-1/PlaybackInfo"


def test_reported_token_never_appears_in_a_room_state(monkeypatch):
    with _client(monkeypatch) as client:
        _fake(client)
        room = _create_room(client)
        _login(client, room)
        _item(client, room, "hls-1")
        state = _hall(client).rooms[room["room_id"]].room_state()
        assert "TOKEN-SECRET-VALUE" not in str(state)
        assert state["jellyfin"] == {"user_name": "hehesheng", "user_id": "user-1"}
