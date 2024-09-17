import json
import asyncio
import yaml

import aiohttp

from file_agent_wrapper import FileSystemAgentWrapperInterface


# class AlistFileSystemAgent(FileSystemAgentWrapperInterface):
class AlistFileSystemAgent(object):
    recoder_watcher_auth = {
        "username": "recorder",
        "password": "recorderpassword",
    }

    def __init__(self, base_url: str):
        with open("config.yaml", "r") as f:
            config = yaml.load(f, Loader=yaml.CSafeLoader)
        self.recoder_watcher_auth = config["alist"]
        self._base_url = base_url

    async def init(self) -> None:
        self._session: aiohttp.ClientSession = aiohttp.ClientSession(base_url=self._base_url)
        self._token = await self._get_token()
        asyncio.get_event_loop().call_later(24 * 60 * 60, self.loop_update_token)

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
        response_dict = json.loads(await response.read())
        if response_dict["code"] != 200 or response_dict["message"] != "success":
            raise RuntimeError(f"url: {self._base_url} login fail: {response_dict}")
        token = response_dict["data"]["token"]
        if token is None:
            raise RuntimeError("token is None")
        return token

    async def loop_update_token(self) -> None:
        if self._session is None or self._session.closed:
            return
        self._token = await self._get_token()
        asyncio.get_event_loop().call_later(24 * 60 * 60, self._get_token)

    async def list_files(self, path: str) -> list[str]:
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
        response_dict = json.loads(await response.read())
        print(response_dict)
        return []

    async def close(self) -> None:
        await self._session.close()


async def main() -> None:
    alist_file_system_agent = AlistFileSystemAgent(base_url="https://alist.3geeks.top")
    await alist_file_system_agent.init()
    await alist_file_system_agent.list_files("/")
    await alist_file_system_agent.close()


if __name__ == "__main__":
    asyncio.run(main())
    print("done")
