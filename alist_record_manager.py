import os
import time
import asyncio
import datetime
import logging

from pydantic import BaseModel

from alist_file_agent import AlistFileSystemAgent

logger = logging.getLogger(__file__.split("/")[-1])

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

    async def _generate_stream_cover(self, stream_name) -> None:

        def _inner_generate_cover() -> int:
            return os.system(
                f"yes | ffmpeg -i rtmp://localhost/live/{stream_name} -ss 0 -f image2 -vframes 1 ./live/cover/img_cover_{stream_name}.jpg"
            )

        try:
            sys_code = await asyncio.wait_for(asyncio.to_thread(_inner_generate_cover), timeout=10)
            if sys_code != 0:
                logger.error(f"generate stream cover failed, stream name is {stream_name}")
        except TimeoutError as e:
            logger.error(f"{e}")

    async def get_stream_cover(self, stream_name: str, inner: bool = False) -> bytes | None:
        cover_files: list[AlistFileInfo] = await self._file_agent.list_files("/local/cover")
        for cover_info in cover_files or []:
            if cover_info.name == f"img_cover_{stream_name}.jpg":
                created_fix_slice = cover_info.created.split(".")
                created_fix_str = created_fix_slice[0] + cover_info.created[-6:]
                create_time = datetime.datetime.strptime(created_fix_str, "%Y-%m-%dT%H:%M:%S%z")
                if time.time() - create_time.timestamp() > 10:
                    await self._generate_stream_cover(stream_name)
                with open(f"./live/cover/img_cover_{stream_name}.jpg", mode="rb") as f:
                    return f.read()
        if inner:
            return None
        await self._generate_stream_cover(stream_name)
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
