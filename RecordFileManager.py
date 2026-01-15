import os
import asyncio
from typing import List, Optional

from pydantic import BaseModel

BYTES_OF_GB = 1024*1024*1024
BYTES_OF_1GB = 1 * BYTES_OF_GB
BYTES_OF_2GB = 2 * BYTES_OF_GB
BYTES_OF_3GB = 3 * BYTES_OF_GB
BYTES_OF_4GB = 4 * BYTES_OF_GB
BYTES_OF_8GB = 8 * BYTES_OF_GB
BYTES_OF_10GB = 10 * BYTES_OF_GB
BYTES_OF_16GB = 16 * BYTES_OF_GB

SINGLE_STREAM_RECORD_MAX_SIZE = BYTES_OF_2GB
ALL_STREAM_RECORD_MAX_SIZE = BYTES_OF_2GB

RECORD_FILE_PATH = "./live"


class RecordFileBaseModel(BaseModel):
    file_name: str
    timestamp: int
    file_size: int
    download_url: str | None = None
    player_url: str | None = None
    thumb_url: str | None = None


class RecordFile(object):

    def __init__(self, file_name: str, path: str = ""):
        valid_media_types = ["flv", "mp4"]
        file_name_split_slice = file_name.split('.')
        self.__is_tmp_file = False
        self.__is_media_file = False
        if file_name_split_slice[-1] in valid_media_types:
            self.__is_media_file = True
        elif (file_name_split_slice[-1] == 'tmp' and
              file_name_split_slice[-2] in valid_media_types):
            self.__is_tmp_file = True
            self.__is_media_file = True
        else:
            return
        self.__file_name = file_name
        self.__stream_name = file_name_split_slice[0]
        self.__timestamp = int(file_name_split_slice[-2]
                               if not self.__is_tmp_file
                               else file_name_split_slice[-3])
        self.__file_path = os.path.join(path, file_name)
        self.__file_size = os.stat(self.file_path).st_size

    def __repr__(self) -> str:
        return self.__file_name

    @property
    def is_tmp(self) -> bool:
        return self.__is_media_file and self.__is_tmp_file

    @property
    def is_valid(self) -> bool:
        return self.__is_media_file and not self.__is_tmp_file

    @property
    def file_name(self) -> str:
        return self.__file_name

    @property
    def file_path(self) -> str:
        return self.__file_path

    @property
    def timestamp(self) -> int:
        return self.__timestamp

    @property
    def stream_name(self) -> str:
        return self.__stream_name

    @property
    def file_size(self) -> int:
        return self.__file_size

    def cover_to_basemodel(self) -> RecordFileBaseModel:
        return RecordFileBaseModel(
            file_name=self.__file_name,
            timestamp=self.__timestamp,
            file_size=self.__file_size
        )


def get_record_file_list(path: str, stream_name: Optional[str] = None) -> List[RecordFile]:
    ret_list = list()
    for file_name in os.listdir(path):
        record_file = RecordFile(file_name, path=path)
        if not record_file.is_valid:
            continue
        if stream_name != None and record_file.stream_name != stream_name:
            continue
        ret_list.append(record_file)
    return ret_list


def limit_record_file_size(
    record_file_list: List[RecordFile],
    limit_size: int = SINGLE_STREAM_RECORD_MAX_SIZE
) -> List[RecordFile]:
    record_total_size = 0
    record_file_list.sort(key=lambda x: x.timestamp)
    del_index = 0
    for record_file in record_file_list:
        record_total_size += record_file.file_size
        while (record_total_size > limit_size and
               record_total_size > record_file.file_size):
            del_record_file(record_file_list[del_index].file_path)
            record_total_size -= record_file_list[del_index].file_size
            del_index += 1
    return record_file_list[del_index:]


def get_specified_record_file(path: str, file_name: str) -> RecordFile | None:
    real_file_path = os.path.join(path, file_name)
    if os.path.isfile(real_file_path):
        return RecordFile(file_name, path=path)
    return None


def del_record_file(path: str) -> None:
    os.remove(path)


def clear_crash_uncomplete_tmp_files(path: str) -> None:
    if path == "":
        return
    last_record_file_map = dict()
    for file_name in os.listdir(path):
        record_file = RecordFile(file_name, path=path)
        if not record_file.is_tmp:
            continue
        if record_file.stream_name in last_record_file_map:
            if last_record_file_map[record_file.stream_name].timestamp < record_file.timestamp:
                del_record_file(
                    last_record_file_map[record_file.stream_name].file_path)
                last_record_file_map[record_file.stream_name] = record_file
            else:
                del_record_file(record_file.file_path)
        else:
            last_record_file_map[record_file.stream_name] = record_file


def record_file_update(stream_name: Optional[str] = None, file_name: str = "", enable_record: bool = True) -> None:
    if not enable_record:
        record_file = get_specified_record_file(RECORD_FILE_PATH, file_name)
        if record_file is not None:
            del_record_file(record_file.file_path)
    if stream_name is not None:
        get_and_limit_stream_record_file_list(stream_name)
    get_and_limit_all_stream_record_file_list()
    # clear crash record files
    clear_crash_uncomplete_tmp_files(RECORD_FILE_PATH)


def get_and_limit_stream_record_file_list(stream_name: str) -> List[RecordFile]:
    return limit_record_file_size(get_record_file_list(RECORD_FILE_PATH, stream_name=stream_name))


def get_and_limit_all_stream_record_file_list() -> List[RecordFile]:
    return limit_record_file_size(get_record_file_list(RECORD_FILE_PATH), limit_size=ALL_STREAM_RECORD_MAX_SIZE)


def get_base_model_record_file_list(stream_name: str) -> List[RecordFileBaseModel]:
    record_file_list = get_and_limit_stream_record_file_list(stream_name)
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
