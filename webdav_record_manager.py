import asyncio
import logging
import os
import subprocess
import traceback
from typing import AsyncIterator, Dict, List, Optional, Tuple

import yaml

from RecordFileManager import RecordFileBaseModel
from webdav_client import CHUNK_SIZE, WebDavClient, WebDavEntry

logger = logging.getLogger(__file__.split("/")[-1])

VALID_MEDIA_TYPES = {"flv", "mp4"}
DEFAULT_MAX_STORAGE_BYTES = 53687091200  # 50GB


class WebDavRecordManager:
    def __init__(self, config_path: str = "config.yaml") -> None:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"config file not found: {config_path}")
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f)
        webdav_cfg: Dict[str, str] = cfg.get("webdav", {})
        record_cfg: Dict[str, str] = cfg.get("record", {})
        self._client = WebDavClient(
            hostname=webdav_cfg.get("hostname", ""),
            login=webdav_cfg.get("login", ""),
            password=webdav_cfg.get("password", ""),
            root=webdav_cfg.get("root", "/"),
        )
        self.local_record_dir = record_cfg.get("local_dir", "./live")
        self.local_cover_dir = record_cfg.get("cover_dir", "./live/cover")
        self.remote_cover_dir = record_cfg.get("cover_remote_dir", "cover")
        self.max_storage_bytes: int = int(webdav_cfg.get("max_storage_bytes", DEFAULT_MAX_STORAGE_BYTES))

    async def init(self) -> None:
        await self._client.init()

    async def close(self) -> None:
        await self._client.close()

    def _cover_name(self, file_name: str) -> str:
        base, _ = os.path.splitext(file_name)
        return f"{base}.jpg"

    def _cover_remote_path(self, cover_file_name: str) -> str:
        return f"{self.remote_cover_dir.strip('/')}/{cover_file_name}" if self.remote_cover_dir else cover_file_name

    def _resolve_local_file(self, incoming_path: str, file_name: str) -> str:
        if os.path.isfile(incoming_path):
            return incoming_path
        fallback = os.path.join(self.local_record_dir, file_name)
        return fallback

    async def _safe_remove(self, path: str) -> None:
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            logger.warning("Remove file failed: %s", path)

    async def _generate_cover(self, local_file_path: str, file_name: str) -> Optional[str]:
        os.makedirs(self.local_cover_dir, exist_ok=True)
        cover_output = os.path.join(self.local_cover_dir, self._cover_name(file_name))
        command = [
            "ffmpeg",
            "-y",
            "-ss",
            "00:00:01",
            "-i",
            local_file_path,
            "-frames:v",
            "1",
            cover_output,
        ]
        try:
            process = await asyncio.create_subprocess_exec(
                *command, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
            except asyncio.TimeoutError:
                process.kill()
                raise
            if process.returncode != 0:
                logger.error(
                    "Generate cover failed for %s, code=%s, stdout=%s, stderr=%s",
                    file_name,
                    process.returncode,
                    stdout.decode(errors="ignore"),
                    stderr.decode(errors="ignore"),
                )
                return None
            return cover_output
        except Exception:
            logger.error("%s", traceback.format_exc())
            return None

    async def handle_record_file(self, stream_name: str, file_name: str, incoming_path: str, enable_record: bool) -> None:
        local_file_path = self._resolve_local_file(incoming_path, file_name)
        if not enable_record:
            await self._safe_remove(local_file_path)
            return
        if not os.path.isfile(local_file_path):
            logger.warning("Local record file not found: %s", local_file_path)
            return
        logger.info("Uploading record %s for stream %s", file_name, stream_name)
        await self._client.upload_file(local_file_path, file_name)
        cover_path = await self._generate_cover(local_file_path, file_name)
        if cover_path:
            try:
                await self._client.upload_file(cover_path, self._cover_remote_path(self._cover_name(file_name)))
            finally:
                await self._safe_remove(cover_path)
        await self._safe_remove(local_file_path)
        # Cleanup old files if storage limit exceeded
        await self._limit_storage_size()

    def _extract_timestamp(self, file_name: str) -> int:
        parts = file_name.split(".")
        if len(parts) < 3:
            return 0
        # format: stream.timestamp.xxx.ext
        try:
            return int(parts[-2])
        except ValueError:
            return 0

    async def list_records(self, stream_name: str) -> List[RecordFileBaseModel]:
        entries = await self._client.list_directory("")
        files: List[RecordFileBaseModel] = []
        for entry in entries:
            if entry.is_dir:
                continue
            suffix = entry.name.split(".")[-1].lower()
            if suffix not in VALID_MEDIA_TYPES:
                continue
            if not entry.name.startswith(f"{stream_name}."):
                continue
            timestamp = self._extract_timestamp(entry.name)
            files.append(
                RecordFileBaseModel(
                    file_name=entry.name,
                    timestamp=timestamp,
                    file_size=entry.size,
                    download_url=f"/stream/record/d/{entry.name}",
                    thumb_url=f"/stream/record/cover/{self._cover_name(entry.name)}",
                )
            )
        files.sort(key=lambda x: x.timestamp)
        return files

    async def stream_record(self, file_name: str, range_header: Optional[str]) -> Tuple[int, Dict[str, str], AsyncIterator[bytes]]:
        status, headers, body = await self._client.stream_file(file_name, range_header)
        # ensure headers include sane defaults
        headers.setdefault("content-type", "video/mp4")
        headers.setdefault("accept-ranges", "bytes")
        return status, headers, body

    async def fetch_cover(self, cover_name: str) -> Optional[bytes]:
        target = cover_name
        if cover_name.lower().endswith((".flv", ".mp4")):
            target = self._cover_name(cover_name)
        remote_path = self._cover_remote_path(target)
        return await self._client.fetch_bytes(remote_path)

    async def _limit_storage_size(self) -> None:
        """Limit remote storage size by deleting oldest files when exceeding limit."""
        try:
            entries = await self._client.list_directory("")
            # Separate media files and cover files
            media_files: List[Tuple[WebDavEntry, int]] = []
            cover_files_map: Dict[str, str] = {}  # base_name -> cover_file_name

            for entry in entries:
                if entry.is_dir:
                    continue
                suffix = entry.name.split(".")[-1].lower()
                if suffix in VALID_MEDIA_TYPES:
                    timestamp = self._extract_timestamp(entry.name)
                    media_files.append((entry, timestamp))
                elif entry.name.lower().endswith(".jpg"):
                    # Track cover files
                    base_name = os.path.splitext(entry.name)[0]
                    cover_files_map[base_name] = entry.name

            # Calculate total size
            total_size = sum(entry.size for entry, _ in media_files)
            total_size += sum(e.size for e in entries if not e.is_dir and e.name.lower().endswith(".jpg"))

            if total_size <= self.max_storage_bytes:
                logger.info("Storage size %d bytes is within limit %d bytes", total_size, self.max_storage_bytes)
                return

            # Sort by timestamp (oldest first)
            media_files.sort(key=lambda x: x[1])

            # Delete oldest files until under limit
            deleted_count = 0
            for entry, timestamp in media_files:
                if total_size <= self.max_storage_bytes:
                    break

                logger.info("Deleting old file %s (timestamp=%s, size=%d) to free space", entry.name, timestamp, entry.size)

                # Delete media file
                await self._client.delete_file(entry.name)
                total_size -= entry.size
                deleted_count += 1

                # Delete associated cover file if exists
                base_name = os.path.splitext(entry.name)[0]
                cover_name = cover_files_map.get(base_name)
                if cover_name:
                    cover_remote_path = self._cover_remote_path(cover_name)
                    try:
                        await self._client.delete_file(cover_remote_path)
                        # Find cover size and subtract it
                        for e in entries:
                            if not e.is_dir and cover_remote_path.endswith(e.name):
                                total_size -= e.size
                                break
                        logger.info("Deleted associated cover file %s", cover_remote_path)
                    except Exception as e:
                        logger.warning("Failed to delete cover file %s: %s", cover_remote_path, e)

            logger.info("Storage cleanup completed: deleted %d files, new total size: %d bytes", deleted_count, total_size)
        except Exception:
            logger.error("Failed to limit storage size: %s", traceback.format_exc())
