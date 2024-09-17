import io
from enum import Enum
from abc import ABC, abstractmethod


class FileTypeEnum(Enum):
    DIR = "dir"
    FILE = "file"


class FileAgentInfoWrapper(object):
    file_type: FileTypeEnum
    file_name: str
    file_size: int


class FileAgentWrapperInterface(ABC):
    @abstractmethod
    def close() -> None:
        pass


class FileSystemAgentWrapperInterface(ABC):
    @abstractmethod
    def open_file(self, path: str) -> io.TextIOWrapper:
        pass

    @abstractmethod
    def list_files(self, path: str) -> list[str]:
        pass

    @abstractmethod
    def get_file_info(self, path: str) -> FileAgentInfoWrapper:
        pass

    @abstractmethod
    def delete_file(self, path: str) -> None:
        pass
