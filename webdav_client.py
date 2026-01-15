import asyncio
import logging
import os
import urllib.parse
import xml.etree.ElementTree as ET
from typing import AsyncIterator, Dict, List, Optional, Tuple

import aiofiles
import aiohttp

logger = logging.getLogger(__file__.split("/")[-1])

CHUNK_SIZE = 1024 * 1024


class WebDavEntry:
    def __init__(self, name: str, is_dir: bool, size: int, last_modified: str):
        self.name = name
        self.is_dir = is_dir
        self.size = size
        self.last_modified = last_modified


class WebDavClient:
    def __init__(self, hostname: str, login: str, password: str, root: str) -> None:
        self.hostname = hostname.rstrip("/")
        self.root = "/" + root.strip("/") + "/"
        self._auth = aiohttp.BasicAuth(login, password)
        self._session: Optional[aiohttp.ClientSession] = None

    async def init(self) -> None:
        if self._session is not None:
            return
        self._session = aiohttp.ClientSession(auth=self._auth, raise_for_status=False)

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _build_url(self, remote_relative_path: str) -> str:
        relative = remote_relative_path.lstrip("/")
        path = f"{self.hostname}{self.root}{relative}"
        return path

    async def _ensure_dir(self, remote_dir: str) -> None:
        if self._session is None:
            raise RuntimeError("WebDavClient not initialized")
        cleaned_dir = remote_dir.strip("/")
        if cleaned_dir == "":
            return
        parts = cleaned_dir.split("/")
        current = ""
        for part in parts:
            current = f"{current}/{part}" if current else part
            url = self._build_url(current) + "/"
            try:
                resp = await self._session.request("MKCOL", url)
                await resp.read()
                if resp.status not in (201, 405):
                    logger.warning("MKCOL %s failed with status %s", url, resp.status)
            except Exception:
                logger.exception("Ensure remote dir %s failed", url)

    async def upload_file(self, local_path: str, remote_relative_path: str) -> None:
        if self._session is None:
            raise RuntimeError("WebDavClient not initialized")
        await self._ensure_dir(os.path.dirname(remote_relative_path))
        url = self._build_url(remote_relative_path)

        async def _stream() -> AsyncIterator[bytes]:
            async with aiofiles.open(local_path, mode="rb") as f:
                while True:
                    chunk = await f.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    yield chunk

        async with self._session.put(url, data=_stream()) as resp:
            await resp.read()
            if resp.status not in (200, 201, 204):
                raise RuntimeError(f"Upload {local_path} to {url} failed, status: {resp.status}")

    async def fetch_bytes(self, remote_relative_path: str) -> Optional[bytes]:
        if self._session is None:
            raise RuntimeError("WebDavClient not initialized")
        url = self._build_url(remote_relative_path)
        async with self._session.get(url) as resp:
            if resp.status != 200:
                await resp.read()
                return None
            return await resp.read()

    async def stream_file(self, remote_relative_path: str, range_header: Optional[str] = None) -> Tuple[int, Dict[str, str], AsyncIterator[bytes]]:
        if self._session is None:
            raise RuntimeError("WebDavClient not initialized")
        url = self._build_url(remote_relative_path)
        headers: Dict[str, str] = {}
        if range_header:
            headers["Range"] = range_header
        resp = await self._session.get(url, headers=headers)

        async def _gen() -> AsyncIterator[bytes]:
            async with resp:
                async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
                    yield chunk

        response_headers = {
            "content-type": resp.headers.get("Content-Type", "application/octet-stream"),
            "accept-ranges": resp.headers.get("Accept-Ranges", "bytes"),
        }
        if resp.headers.get("Content-Length"):
            response_headers["content-length"] = resp.headers["Content-Length"]
        if resp.headers.get("Content-Range"):
            response_headers["content-range"] = resp.headers["Content-Range"]
        return resp.status, response_headers, _gen()

    async def list_directory(self, remote_relative_dir: str = "") -> List[WebDavEntry]:
        if self._session is None:
            raise RuntimeError("WebDavClient not initialized")
        target = remote_relative_dir.strip("/")
        url = self._build_url(target)
        # PROPFIND needs a trailing slash to list directory contents
        if not url.endswith("/"):
            url += "/"
        body = """<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<d:propfind xmlns:d=\"DAV:\">\n  <d:prop>\n    <d:displayname/>\n    <d:getcontentlength/>\n    <d:getlastmodified/>\n    <d:resourcetype/>\n  </d:prop>\n</d:propfind>\n"""
        headers = {"Depth": "1", "Content-Type": "application/xml"}
        async with self._session.request("PROPFIND", url, data=body, headers=headers) as resp:
            text = await resp.text()
            if resp.status not in (207, 200):
                logger.error("PROPFIND %s failed, status=%s, body=%s", url, resp.status, text)
                raise RuntimeError(f"PROPFIND failed: {resp.status}")
        try:
            root = ET.fromstring(text)
        except Exception:
            logger.exception("Parse PROPFIND response failed")
            raise
        ns = {"d": "DAV:"}
        entries: List[WebDavEntry] = []
        for response in root.findall("d:response", ns):
            href = response.findtext("d:href", default="", namespaces=ns)
            name = urllib.parse.unquote(href).rstrip("/").split("/")[-1]
            if name == "":
                continue
            prop = response.find("d:propstat/d:prop", ns)
            if prop is None:
                continue
            res_type = prop.find("d:resourcetype", ns)
            is_dir = res_type is not None and res_type.find("d:collection", ns) is not None
            if name == target.split("/")[-1] and is_dir:
                # skip the directory itself
                continue
            size_text = prop.findtext("d:getcontentlength", default="0", namespaces=ns) or "0"
            last_modified = prop.findtext("d:getlastmodified", default="", namespaces=ns) or ""
            try:
                size = int(size_text)
            except ValueError:
                size = 0
            entries.append(WebDavEntry(name=name, is_dir=is_dir, size=size, last_modified=last_modified))
        return entries

    async def delete_file(self, remote_relative_path: str) -> None:
        if self._session is None:
            raise RuntimeError("WebDavClient not initialized")
        url = self._build_url(remote_relative_path)
        async with self._session.request("DELETE", url) as resp:
            await resp.read()
            if resp.status not in (200, 204, 404):
                logger.error("DELETE %s failed, status=%s", url, resp.status)
                raise RuntimeError(f"DELETE failed: {resp.status}")
