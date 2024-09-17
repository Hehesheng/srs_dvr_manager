import io
import os

from file_agent_wrapper import FileSystemAgentWrapperInterface, FileAgentWrapperInterface, FileAgentInfoWrapper, FileTypeEnum


class LocalFileAgent(FileSystemAgentWrapperInterface):
    def __init__(self, work_dir: str = "./"):
        pass

    def open_file(self, path: str) -> FileAgentWrapperInterface:
        pass

    def list_files(self, path: str) -> list[str]:
        return os.listdir(path)

    def get_file_info(self, path: str) -> FileAgentInfoWrapper:
        is_dir = os.path.isdir(path)
        res = FileAgentInfoWrapper()
        res.file_type = FileTypeEnum.DIR if is_dir else FileTypeEnum.FILE
        res.file_name = path
        res.file_size = os.stat(path).st_size
        return res

    def delete_file(self, path) -> None:
        os.remove(path)
