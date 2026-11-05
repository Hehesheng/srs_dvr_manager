import pytest

from hall.media import ByteRange, RangeError, parse_range


def test_no_range_returns_none():
    assert parse_range(None, 1000) is None
    assert parse_range("", 1000) is None


def test_closed_range():
    assert parse_range("bytes=0-99", 1000) == ByteRange(0, 99)


def test_open_ended_range_is_capped():
    parsed = parse_range("bytes=0-", 100 * 1024 * 1024, max_open_bytes=8 * 1024 * 1024)
    assert parsed == ByteRange(0, 8 * 1024 * 1024 - 1)


def test_open_ended_range_at_eof():
    assert parse_range("bytes=100-", 1000) == ByteRange(100, 999)


def test_suffix_range():
    assert parse_range("bytes=-100", 1000) == ByteRange(900, 999)


def test_closed_range_is_clamped_to_eof():
    assert parse_range("bytes=0-999999", 1000) == ByteRange(0, 999)


@pytest.mark.parametrize(
    "header",
    [
        "bytes=1000-",
        "bytes=5-3",
        "bytes=0-1,3-4",
        "bytes=abc-",
        "bytes=-0",
        "items=0-1",
    ],
)
def test_invalid_ranges(header):
    with pytest.raises(RangeError):
        parse_range(header, 1000)


def test_empty_media_is_unsatisfiable():
    with pytest.raises(RangeError):
        parse_range("bytes=0-1", 0)
