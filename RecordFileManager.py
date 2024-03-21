import os
import asyncio
from typing import Dict, List, Tuple

from pydantic import BaseModel


BYTE_OF_2GB_SIZE = 2 * 1024 * 1024 * 1024

# BYTE_OF_LIMIT_SIZE = 200 * 1024 * 1024
BYTE_OF_LIMIT_SIZE = BYTE_OF_2GB_SIZE

RECORD_FILE_PATH = './live'


class RecordFileBaseModel(BaseModel):
    file_name: str
    stream_name: str
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
            file_name=self.file_name, stream_name=self.stream_name, timestamp=self.timestamp, file_size=self.file_size)


def get_all_stream_name_to_record_file_map(path: str) -> Dict[str, List[RecordFile]]:
    ret_map = dict()
    for file_name in os.listdir(path):
        record_file = RecordFile(file_name, path=path)
        if not record_file.is_valid():
            continue
        if record_file.get_stream_name() not in ret_map:
            ret_map[record_file.get_stream_name()] = [record_file]
        else:
            ret_map[record_file.get_stream_name()].append(record_file)
    return ret_map


def limit_record_file_size(stream_name_to_file_map: Dict[str, List[RecordFile]]) -> None:
    for stream_name, record_file_list in stream_name_to_file_map.items():
        record_total_size = 0
        record_file_list.sort(key=lambda x: x.get_timestamp())
        del_index = 0
        for record_file in record_file_list:
            record_total_size += record_file.get_file_size()
            while record_total_size > BYTE_OF_LIMIT_SIZE and record_total_size > record_file.get_file_size():
                del_record_file(record_file_list[del_index].get_file_path())
                record_total_size -= record_file_list[del_index].get_file_size()
                del_index += 1
        stream_name_to_file_map[stream_name] = record_file_list[del_index:]


def del_record_file(path: str) -> None:
    os.remove(path)


def record_file_update() -> None:
    stream_name_to_file_map = get_all_stream_name_to_record_file_map(
        RECORD_FILE_PATH)
    limit_record_file_size(stream_name_to_file_map)


def get_record_file_list(stream_name: str) -> List[RecordFileBaseModel]:
    stream_name_to_file_map = get_all_stream_name_to_record_file_map(
        RECORD_FILE_PATH)
    limit_record_file_size(stream_name_to_file_map)
    file_list = stream_name_to_file_map[stream_name]
    basemodle_list = list()
    for f in file_list:
        basemodle_list.append(f.cover_to_basemodel())
    return basemodle_list


def find_record_file_by_name(file_name: str) -> str:
    if file_name in os.listdir(RECORD_FILE_PATH):
        return os.path.join(RECORD_FILE_PATH, file_name)
    return ''


if __name__ == "__main__":
    record_file_update()
