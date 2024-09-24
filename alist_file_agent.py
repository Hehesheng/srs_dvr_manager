import json
import asyncio
import logging.config
import yaml
import logging

import aiohttp
from pydantic import BaseModel

from file_agent_wrapper import FileSystemAgentWrapperInterface

logger = logging.getLogger(__file__.split("/")[-1])


# class AlistFileSystemAgent(FileSystemAgentWrapperInterface):
class AlistFileSystemAgent(object):

    def __init__(self):
        with open("config.yaml", "r") as f:
            config: dict = yaml.load(f, Loader=yaml.CSafeLoader)
        self.alist_config = config["alist"]
        self.recoder_watcher_auth = {
            "username": self.alist_config["username"],
            "password": self.alist_config["password"],
        }
        self._base_url = self.alist_config["url"]

    async def init(self) -> None:
        self._session: aiohttp.ClientSession = aiohttp.ClientSession(base_url=self._base_url)
        # repeat try init
        for i in range(60):
            try:
                self._token = await self._get_token()
                break
            except Exception as e:
                logger.info(f"init fail: {e}, has try {i} times")
            await asyncio.sleep(10)

        asyncio.get_event_loop().call_later(24 * 60 * 60, self.loop_update_token)

    class ReqTokenStruct(BaseModel):
        code: int
        message: str

        class DataStruct(BaseModel):
            token: str

        data: DataStruct

    async def _get_token(self) -> str:
        response = await self._session.post(
            "/api/auth/login",
            headers={
                "Content-Type": "application/json",
            },
            data=json.dumps(self.recoder_watcher_auth),
        )
        if response.status != 200:
            raise RuntimeError(f"url: {self._base_url} login fail: {response.status}")
        response_data = AlistFileSystemAgent.ReqTokenStruct.model_validate_json(await response.read())
        logger.info(f"get token: {response_data=}")
        if response_data.code != 200 or response_data.message != "success":
            raise RuntimeError(f"url: {self._base_url} login fail: {response_data}")
        token = response_data.data.token
        if token is None:
            raise RuntimeError("token is None")
        return token

    async def loop_update_token(self) -> None:
        if self._session is None or self._session.closed:
            return
        self._token = await self._get_token()
        asyncio.get_event_loop().call_later(24 * 60 * 60, self._get_token)

    class ReqListFilesStruct(BaseModel):
        code: int
        message: str

        class DataStruct(BaseModel):
            class ContentStruct(BaseModel):
                name: str
                size: int
                is_dir: bool
                modified: str
                created: str
                sign: str
                thumb: str
                type: int
                hashinfo: str

            content: list[ContentStruct] | None

        data: DataStruct

    async def list_files(self, path: str) -> "AlistFileSystemAgent.ReqListFilesStruct.DataStruct.ContentStruct":
        payload = json.dumps(
            {
                "path": f"{path}",
                "password": "",
                "page": 1,
                "per_page": 0,
                "refresh": False,
            }
        )
        headers = {
            "Authorization": f"{self._token}",
            "Content-Type": "application/json",
        }
        response = await self._session.post("/api/fs/list", headers=headers, data=payload)
        if response.status != 200:
            raise RuntimeError(f"url: {self._base_url} list files fail: {response.status}")
        response_data = AlistFileSystemAgent.ReqListFilesStruct.model_validate_json(await response.read())
        logger.info(f"get file list: {response_data=}")
        if response_data.code != 200:
            raise RuntimeError(f"url response error")
        return response_data.data.content

    class ReqDefaultFileStruct(BaseModel):
        code: int
        message: str

    async def copy_file(self, src: str, dest: str, file_name: str) -> str:
        payload = json.dumps(
            {
                "src_dir": f"{src}",
                "dst_dir": f"{dest}",
                "names": [f"{file_name}"],
            }
        )
        headers = {
            "Authorization": f"{self._token}",
            "Content-Type": "application/json",
        }
        response = await self._session.post("/api/fs/copy", headers=headers, data=payload)
        if response.status != 200:
            raise RuntimeError(f"url: {self._base_url} copy file fail: {response.status}")
        response_data = AlistFileSystemAgent.ReqDefaultFileStruct.model_validate_json(await response.read())
        logger.info(f"copy file: {response_data=}")
        if response_data.code != 200:
            raise RuntimeError(f"url response error")
        return response_data.message

    async def delete_file(self, path: str) -> str:
        payload = json.dumps(
            {
                "names": [f"{path}"],
                "dir": "/",
            }
        )
        headers = {
            "Authorization": f"{self._token}",
            "Content-Type": "application/json",
        }
        response = await self._session.post("/api/fs/remove", headers=headers, data=payload)
        if response.status != 200:
            raise RuntimeError(f"url: {self._base_url} delete file fail: {response.status}")
        response_data = AlistFileSystemAgent.ReqDefaultFileStruct.model_validate_json(await response.read())
        logger.info(f"delete file {path}: {response_data}")
        if response_data.code != 200:
            raise RuntimeError(f"url response error")
        return response_data.message

    async def close(self) -> None:
        await self._session.close()


async def main() -> None:
    alist_file_system_agent = AlistFileSystemAgent()
    await alist_file_system_agent.init()
    await alist_file_system_agent.list_files("/local")
    await alist_file_system_agent.copy_file("/local/temp", "/local/temp/test", "img_thump_kylin.jpg")
    await alist_file_system_agent.delete_file("/local/temp/test/img_thump_kylin.jpg")
    await alist_file_system_agent.delete_file("/local/temp/img_thump_hehe.jpg")
    await alist_file_system_agent.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
    logger.info("done")
