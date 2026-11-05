"""Tokens and signed media URLs for the media hall."""

import hashlib
import hmac
import logging
import os
import secrets
import time
from typing import Optional

logger = logging.getLogger(__name__)

ROOM_CODE_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
ROOM_CODE_LENGTH = 6
DEFAULT_MEDIA_URL_TTL = 86400


def _load_or_create_secret(project_dir: str) -> str:
    path = os.path.join(project_dir, "hall", ".signing_secret")
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                value = f.read().strip()
            if value:
                return value
        value = secrets.token_urlsafe(48)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(value + "\n")
        logger.info("Generated media hall signing secret at %s", path)
        return value
    except OSError:
        logger.exception("Failed to persist signing secret; using in-memory secret")
        return secrets.token_urlsafe(48)


class HallAuth:
    def __init__(self, signing_secret: str, media_url_ttl: int = DEFAULT_MEDIA_URL_TTL) -> None:
        self._secret = signing_secret.encode("utf-8")
        self.media_url_ttl = int(media_url_ttl)

    @classmethod
    def from_config(cls, cfg: dict, project_dir: str) -> "HallAuth":
        secret = str(cfg.get("signing_secret") or "").strip()
        if not secret or secret == "change-me":
            secret = _load_or_create_secret(project_dir)
        ttl = int(cfg.get("media_url_ttl", DEFAULT_MEDIA_URL_TTL))
        return cls(secret, ttl)

    @staticmethod
    def new_token() -> str:
        return secrets.token_urlsafe(32)

    @staticmethod
    def new_room_code() -> str:
        return "".join(secrets.choice(ROOM_CODE_ALPHABET) for _ in range(ROOM_CODE_LENGTH))

    def _sign(self, room_id: str, media_id: str, exp: int) -> str:
        message = f"{room_id}:{media_id}:{exp}".encode("utf-8")
        return hmac.new(self._secret, message, hashlib.sha256).hexdigest()

    def media_signature(self, room_id: str, media_id: str, exp: int) -> str:
        return self._sign(room_id, media_id, exp)

    def media_url(self, room_id: str, media_id: str, exp: Optional[int] = None) -> str:
        if exp is None:
            exp = int(time.time()) + self.media_url_ttl
        sig = self._sign(room_id, media_id, exp)
        return f"/hall/media/{room_id}/{media_id}?exp={exp}&sig={sig}"

    def media_url_expiry(self) -> int:
        return int(time.time()) + self.media_url_ttl

    def verify_media(self, room_id: str, media_id: str, exp: int, sig: str) -> bool:
        if exp < int(time.time()):
            return False
        expected = self._sign(room_id, media_id, exp)
        return hmac.compare_digest(expected, sig or "")

    # Subtitles use a separate signing context so a media URL signature can
    # never be replayed against the subtitle endpoint (or vice versa).
    def subtitle_signature(self, room_id: str, subtitle_id: str, exp: int) -> str:
        message = f"subtitle:{room_id}:{subtitle_id}:{exp}".encode("utf-8")
        return hmac.new(self._secret, message, hashlib.sha256).hexdigest()

    def subtitle_url(self, room_id: str, subtitle_id: str, exp: Optional[int] = None) -> str:
        if exp is None:
            exp = int(time.time()) + self.media_url_ttl
        sig = self.subtitle_signature(room_id, subtitle_id, exp)
        return f"/hall/subtitles/{room_id}/{subtitle_id}.vtt?exp={exp}&sig={sig}"

    def verify_subtitle(self, room_id: str, subtitle_id: str, exp: int, sig: str) -> bool:
        if exp < int(time.time()):
            return False
        expected = self.subtitle_signature(room_id, subtitle_id, exp)
        return hmac.compare_digest(expected, sig or "")
