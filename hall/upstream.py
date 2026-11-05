"""Range proxy for publicly hosted media URLs, with SSRF protection."""

import asyncio
import ipaddress
import logging
import socket
from typing import AsyncIterator, Dict, Optional, Tuple
from urllib.parse import urljoin, urlparse

import aiohttp

logger = logging.getLogger(__name__)

MAX_REDIRECTS = 3
PROBE_TIMEOUT = 10.0
READ_TIMEOUT = 20.0


class UpstreamError(Exception):
    status_code = 502
    code = "UPSTREAM_ERROR"


class UpstreamUnsupported(UpstreamError):
    status_code = 400
    code = "RANGE_UNSUPPORTED"


def _is_public_ip(raw: str) -> bool:
    try:
        ip = ipaddress.ip_address(raw)
    except ValueError:
        return False
    if isinstance(ip, ipaddress.IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return not (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
        or getattr(ip, "is_site_local", False)
    )


async def validate_public_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise UpstreamError("only http/https URLs are allowed")
    host = parsed.hostname
    if not host:
        raise UpstreamError("URL has no host")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, port, type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        raise UpstreamError(f"cannot resolve host: {exc}") from exc
    addresses = {info[4][0] for info in infos}
    if not addresses:
        raise UpstreamError("host did not resolve")
    for address in addresses:
        if not _is_public_ip(address):
            raise UpstreamError("refusing to proxy a private or reserved address")


async def _request_following_redirects(
    session: aiohttp.ClientSession, method: str, url: str, headers: Optional[Dict[str, str]] = None
) -> Tuple[aiohttp.ClientResponse, str]:
    current = url
    for _ in range(MAX_REDIRECTS + 1):
        await validate_public_url(current)
        response = await session.request(
            method,
            current,
            headers=headers,
            allow_redirects=False,
            timeout=aiohttp.ClientTimeout(total=PROBE_TIMEOUT),
        )
        if response.status in (301, 302, 303, 307, 308):
            location = response.headers.get("Location")
            response.release()
            if not location:
                raise UpstreamError("redirect without Location header")
            current = urljoin(current, location)
            continue
        return response, current
    raise UpstreamError("too many redirects")


def _parse_content_range(value: str) -> Optional[Tuple[int, int, int]]:
    # bytes 0-0/12345
    try:
        unit, _, rest = value.partition(" ")
        if unit.lower() != "bytes":
            return None
        span, _, total = rest.partition("/")
        start_s, _, end_s = span.partition("-")
        if total == "*":
            return None
        return int(start_s), int(end_s), int(total)
    except (ValueError, TypeError):
        return None


async def probe_url(session: aiohttp.ClientSession, url: str) -> Dict[str, object]:
    response, final_url = await _request_following_redirects(session, "HEAD", url)
    try:
        status = response.status
        headers = response.headers
        if status >= 400:
            response.release()
            return await _probe_with_range(session, final_url)

        length = headers.get("Content-Length")
        content_type = headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
        accept_ranges = headers.get("Accept-Ranges", "").lower() == "bytes"
        if length is not None and int(length) > 0:
            return {
                "url": final_url,
                "size": int(length),
                "mime": content_type or "application/octet-stream",
                "range_supported": 1 if accept_ranges else await _probe_range_support(session, final_url),
            }
        response.release()
        return await _probe_with_range(session, final_url)
    finally:
        if not response.closed:
            response.release()


async def _probe_range_support(session: aiohttp.ClientSession, url: str) -> int:
    response, _ = await _request_following_redirects(session, "GET", url, {"Range": "bytes=0-0"})
    try:
        return 1 if response.status == 206 else 0
    finally:
        response.release()


async def _probe_with_range(session: aiohttp.ClientSession, url: str) -> Dict[str, object]:
    response, final_url = await _request_following_redirects(session, "GET", url, {"Range": "bytes=0-0"})
    try:
        content_type = response.headers.get("Content-Type", "application/octet-stream").split(";")[0].strip()
        if response.status == 206:
            parsed = _parse_content_range(response.headers.get("Content-Range", ""))
            if parsed is None:
                raise UpstreamError("upstream returned an invalid Content-Range")
            size = parsed[2]
            return {
                "url": final_url,
                "size": size,
                "mime": content_type or "application/octet-stream",
                "range_supported": 1,
            }
        if response.status == 200:
            length = response.headers.get("Content-Length")
            if length is None:
                raise UpstreamError("upstream does not expose Content-Length")
            return {
                "url": final_url,
                "size": int(length),
                "mime": content_type or "application/octet-stream",
                "range_supported": 0,
            }
        raise UpstreamError(f"upstream probe failed with HTTP {response.status}")
    finally:
        response.release()


class UpstreamSource:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        url: str,
        size: int,
        mime: str,
        range_supported: bool,
    ) -> None:
        self.session = session
        self.url = url
        self.size = int(size)
        self.mime = mime
        self.range_supported = bool(range_supported)

    async def read(self, offset: int, length: int) -> bytes:
        if not self.range_supported:
            raise UpstreamUnsupported("upstream does not support Range requests")
        if length <= 0:
            return b""
        end = offset + length - 1
        response, _ = await _request_following_redirects(
            self.session,
            "GET",
            self.url,
            {"Range": f"bytes={offset}-{end}"},
        )
        try:
            if response.status == 200:
                # Some servers ignore Range for the first block.
                if offset != 0:
                    raise UpstreamUnsupported("upstream ignored the Range header")
                return await response.content.readexactly(length)
            if response.status != 206:
                raise UpstreamError(f"upstream returned HTTP {response.status}")
            parsed = _parse_content_range(response.headers.get("Content-Range", ""))
            if parsed is not None and parsed[0] != offset:
                raise UpstreamError("upstream returned the wrong byte range")
            return await response.content.readexactly(length)
        finally:
            response.release()

    async def stream_all(self) -> AsyncIterator[bytes]:
        response, _ = await _request_following_redirects(self.session, "GET", self.url)
        try:
            if response.status != 200:
                raise UpstreamError(f"upstream returned HTTP {response.status}")
            async for chunk in response.content.iter_chunked(256 * 1024):
                if chunk:
                    yield chunk
        finally:
            response.release()
