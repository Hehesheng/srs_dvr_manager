import os
import asyncio
import datetime
from typing import List, Optional

from pydantic import BaseModel

from alist_file_agent import AlistFileSystemAgent

AlistFileInfo = AlistFileSystemAgent.ReqListFilesStruct.DataStruct.ContentStruct

BYTES_OF_GB = 1024 * 1024 * 1024
BYTES_OF_2GB = 2 * BYTES_OF_GB
BYTES_OF_4GB = 4 * BYTES_OF_GB
BYTES_OF_8GB = 8 * BYTES_OF_GB
BYTES_OF_10GB = 10 * BYTES_OF_GB
BYTES_OF_16GB = 16 * BYTES_OF_GB

SINGLE_STREAM_RECORD_MAX_SIZE = BYTES_OF_4GB
ALL_STREAM_RECORD_MAX_SIZE = BYTES_OF_10GB

STREAM_RECORD_FILE_DIR = "./live"
STREAM_THUMB_DIR = "./live/thumb"
STREAM_COVER_DIR = "./live/cover"


class AlistRecordManager(object):
    def __init__(
        self,
        record_dir: str = STREAM_RECORD_FILE_DIR,
        thumb_dir: str = STREAM_THUMB_DIR,
        cover_dir: str = STREAM_COVER_DIR,
    ) -> None:
        self.record_file = record_dir
        self.thumb_file = thumb_dir
        self.cover_file = cover_dir
        self._file_agent = AlistFileSystemAgent()
        self._cover_generate_time_dict = {}

    async def init(self) -> None:
        await self._file_agent.init()

    async def close(self) -> None:
        pass

    def _generate_stream_cover(self, stream_name) -> int:
        return os.system(
            f"yes | ffmpeg -i rtmp://localhost:1935/live/{stream_name} -ss 0 -f image2 -vframes 1 ./live/cover/img_cover_{stream_name}.jpg"
        )

    async def get_stream_cover(self, stream_name: str, inner: bool = False) -> bytes | None:
        cover_files: list[AlistFileInfo] = await self._file_agent.list_files("/local/cover")
        for cover_info in cover_files:
            if cover_info.name == f"img_cover_{stream_name}.jpg":
                create_time = datetime.datetime.strptime(cover_info.created, "%Y-%m-%dT%H:%M:%S.%f%z")
                if (datetime.datetime.now() - create_time).seconds > 10:
                    self._generate_stream_cover(stream_name)
                with open(f"./live/cover/img_cover_{stream_name}.jpg") as f:
                    return f.read()
        if inner:
            return None
        self._generate_stream_cover(stream_name)
        return await self.get_stream_cover(stream_name, inner=True)

    async def _filter_get_files(self, path: str, stream_name: str) -> list[AlistFileInfo]:
        file_infos: list[AlistFileInfo] = await self._file_agent.list_files(path)
        valid_media_types = ["flv", "mp4"]
        return [
            info
            for info in file_infos
            if not info.is_dir and info.name.split(".")[-1] in valid_media_types and info.name.split(".")[0] == stream_name
        ]

    async def get_local_files(self, stream_name) -> list[AlistFileInfo]:
        return await self._filter_get_files("/local", stream_name)

    async def get_cloud_files(self, stream_name) -> list[AlistFileInfo]:
        return await self._filter_get_files("/cloud", stream_name)

    async def get_all_files(self, stream_name) -> list[AlistFileInfo]:
        local_files = await self.get_local_files(stream_name)
        cloud_files = await self.get_cloud_files(stream_name)
        all_files = local_files
        return all_files
