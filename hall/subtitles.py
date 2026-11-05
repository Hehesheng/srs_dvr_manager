"""Subtitle decoding and SRT -> WebVTT conversion for the media hall."""

import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

SUPPORTED_FORMATS = ("vtt", "srt")
UNSUPPORTED_FORMATS = ("ass", "ssa")
MAX_NAME_LENGTH = 128

_TIMESTAMP_HMS = re.compile(r"(\d{1,3}:\d{2}:\d{2}),(\d{3})")
_TIMESTAMP_MS = re.compile(r"(?<![\d:])(\d{1,3}:\d{2}),(\d{3})")
_LANG_RE = re.compile(r"^[A-Za-z0-9_-]{1,16}$")


class SubtitleFormatError(ValueError):
    """The uploaded file is not a supported subtitle format."""


@dataclass
class Subtitle:
    subtitle_id: str
    name: str
    lang: str
    data: bytes
    size: int
    created_at: float = field(default_factory=time.time)
    # Subtitle tracks extracted from one playlist item stay scoped to it; a
    # room-level upload leaves this empty and is offered for every item.
    media_id: Optional[str] = None

    @property
    def format(self) -> str:
        return "vtt"

    def public(self, url: str) -> Dict[str, Any]:
        return {
            "subtitle_id": self.subtitle_id,
            "name": self.name,
            "lang": self.lang,
            "format": self.format,
            "size": self.size,
            "url": url,
            "media_id": self.media_id,
        }


def sanitize_name(raw: str) -> str:
    name = os.path.basename(str(raw or "").replace("\\", "/")).strip()
    name = name.replace("\x00", "")[:MAX_NAME_LENGTH]
    return name or "subtitles.srt"


def sanitize_lang(raw: str) -> str:
    lang = str(raw or "").strip()
    return lang if _LANG_RE.match(lang) else "und"


def extension_of(name: str) -> str:
    return os.path.splitext(name)[1].lower().lstrip(".")


def decode_subtitle(data: bytes) -> str:
    """Decode a subtitle file, preferring UTF-8 and falling back to GB18030."""
    if data.startswith(b"\xef\xbb\xbf"):
        data = data[3:]
    for encoding in ("utf-8", "gb18030"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise SubtitleFormatError("字幕编码无法识别，请使用 UTF-8 或 GB18030")


def _normalize_newlines(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _vtt_time(match: re.Match) -> str:
    return f"{match.group(1)}.{match.group(2)}"


def srt_to_vtt(text: str) -> str:
    """Convert SubRip cues to WebVTT, dropping cue numbers and commas."""
    lines = _normalize_newlines(text).lstrip("﻿").split("\n")
    out = ["WEBVTT", ""]
    index = 0
    total = len(lines)
    while index < total:
        while index < total and not lines[index].strip():
            index += 1
        if index >= total:
            break

        if lines[index].strip().isdigit():
            lookahead = index + 1
            while lookahead < total and not lines[lookahead].strip():
                lookahead += 1
            if lookahead < total and "-->" in lines[lookahead]:
                index = lookahead

        if "-->" not in lines[index]:
            # Not a cue header; skip the malformed block.
            while index < total and lines[index].strip():
                index += 1
            continue

        timestamp = lines[index].strip()
        timestamp = _TIMESTAMP_HMS.sub(_vtt_time, timestamp)
        timestamp = _TIMESTAMP_MS.sub(_vtt_time, timestamp)
        out.append(timestamp)
        index += 1
        while index < total and lines[index].strip():
            out.append(lines[index])
            index += 1
        out.append("")

    while out and out[-1] == "":
        out.pop()
    return "\n".join(out) + "\n"


def vtt_passthrough(text: str) -> str:
    text = _normalize_newlines(text).lstrip("﻿")
    if not text.lstrip().upper().startswith("WEBVTT"):
        text = "WEBVTT\n\n" + text
    return text if text.endswith("\n") else text + "\n"


def convert_subtitle(name: str, data: bytes) -> bytes:
    """Return a valid WebVTT document for a supported subtitle file."""
    ext = extension_of(name)
    if ext in UNSUPPORTED_FORMATS:
        raise SubtitleFormatError("暂不支持 ASS/SSA 字幕，请先转换为 SRT 或 VTT")
    if ext not in SUPPORTED_FORMATS:
        raise SubtitleFormatError("仅支持 .srt 和 .vtt 字幕文件")
    text = decode_subtitle(data)
    converted = srt_to_vtt(text) if ext == "srt" else vtt_passthrough(text)
    return converted.encode("utf-8")
