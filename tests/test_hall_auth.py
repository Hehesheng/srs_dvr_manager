import time
from urllib.parse import parse_qs, urlparse

from hall.auth import HallAuth


def _parse(url: str):
    parsed = urlparse(url)
    return parse_qs(parsed.query)


def test_media_signature_roundtrip():
    auth = HallAuth("secret", media_url_ttl=60)
    url = auth.media_url("r_1", "m_1")
    query = _parse(url)
    exp = int(query["exp"][0])
    sig = query["sig"][0]
    assert auth.verify_media("r_1", "m_1", exp, sig)
    assert not auth.verify_media("r_1", "m_2", exp, sig)
    assert not auth.verify_media("r_2", "m_1", exp, sig)
    tampered = sig[:-1] + ("1" if sig[-1] == "0" else "0")
    assert not auth.verify_media("r_1", "m_1", exp, tampered)


def test_expired_url_is_rejected():
    auth = HallAuth("secret", media_url_ttl=60)
    exp = int(time.time()) - 1
    sig = auth.media_signature("r_1", "m_1", exp)
    assert not auth.verify_media("r_1", "m_1", exp, sig)


def test_room_codes_are_not_ambiguous():
    codes = {HallAuth.new_room_code() for _ in range(200)}
    assert len(codes) > 190
    for code in codes:
        assert len(code) == 6
        assert not (set(code) & set("01IO"))
