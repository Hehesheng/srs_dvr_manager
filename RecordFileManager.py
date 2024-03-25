import os
import asyncio
from typing import List, Optional

from pydantic import BaseModel

BYTES_OF_GB = 1024*1024*1024
BYTES_OF_2GB = 2 * BYTES_OF_GB
BYTES_OF_16GB = 16 * BYTES_OF_GB

# SINGLE_STREAM_RECORD_MAX_SIZE = 200 * 1024 * 1024
SINGLE_STREAM_RECORD_MAX_SIZE = BYTES_OF_2GB
ALL_STREAM_RECORD_MAX_SIZE = BYTES_OF_16GB

RECORD_FILE_PATH = '../live'


class RecordFileBaseModel(BaseModel):
    file_name: str
    timestamp: int
    file_size: int


class RecordFile(object):
    file_name: str
    stream_name: str
    timestamp: int
    file_size: int

    def __init__(self, file_name: str, path: str = None):
        file_name_split_slice = file_name.split('.')
        if file_name_split_slice[-1] == 'tmp':
            self.valid = False
            return
        self.valid = True
        self.file_name = file_name
        self.stream_name = file_name_split_slice[0]
        self.timestamp = int(file_name_split_slice[-2])
        self.file_path = os.path.join(path, file_name)
        self.file_size = os.stat(self.file_path).st_size

    def __repr__(self) -> str:
        return self.file_name

    def is_valid(self) -> bool:
        return self.valid

    def get_file_name(self) -> str:
        return self.file_name

    def get_file_path(self) -> str:
        return self.file_path

    def get_timestamp(self) -> int:
        return self.timestamp

    def get_stream_name(self) -> str:
        return self.stream_name

    def get_file_size(self) -> int:
        return self.file_size

    def cover_to_basemodel(self) -> RecordFileBaseModel:
        return RecordFileBaseModel(
            file_name=self.file_name, timestamp=self.timestamp, file_size=self.file_size)


def get_record_file_list(path: str, stream_name: Optional[str]) -> List[RecordFile]:
    ret_list = list()
    for file_name in os.listdir(path):
        record_file = RecordFile(file_name, path=path)
        if not record_file.is_valid():
            continue
        if stream_name != None and record_file.get_stream_name() != stream_name:
            continue
        ret_list.append(record_file)
    return ret_list


def limit_record_file_size(record_file_list: List[RecordFile], limit_size: int = SINGLE_STREAM_RECORD_MAX_SIZE) -> None:
    record_total_size = 0
    record_file_list.sort(key=lambda x: x.get_timestamp())
    del_index = 0
    for record_file in record_file_list:
        record_total_size += record_file.get_file_size()
        while record_total_size > limit_size and record_total_size > record_file.get_file_size():
            del_record_file(record_file_list[del_index].get_file_path())
            record_total_size -= record_file_list[del_index].get_file_size()
            del_index += 1
    record_file_list = record_file_list[del_index:]


def del_record_file(path: str) -> None:
    os.remove(path)


def record_file_update() -> None:
    record_file_list = get_record_file_list(RECORD_FILE_PATH)
    limit_record_file_size(
        record_file_list, limit_size=ALL_STREAM_RECORD_MAX_SIZE)


def get_base_model_record_file_list(stream_name: str) -> List[RecordFileBaseModel]:
    record_file_list = get_record_file_list(
        RECORD_FILE_PATH, stream_name=stream_name)
    limit_record_file_size(record_file_list)
    basemodle_list = list()
    for f in record_file_list:
        basemodle_list.append(f.cover_to_basemodel())
    return basemodle_list


def find_record_file_by_name(file_name: str) -> str:
    file_path = os.path.join(RECORD_FILE_PATH, file_name)
    if os.path.exists(file_path):
        return file_path
    return ''


if __name__ == "__main__":
    record_file_update()
