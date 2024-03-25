from typing import Dict, Any
import os

import uvicorn
from fastapi import FastAPI, Request, status, HTTPException
from fastapi.responses import Response, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import aiofiles

import RecordFileManager


CHUNK_SIZE = 1024 * 1024

app = FastAPI()

origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.post("/stream/on_dvr/")
async def dvr_done_callback(callback_context: Dict[Any, Any]):
    # print(callback_context)
    # {'server_id': 'vid-390ik4q', 'service_id': '900vr5u1', 'action': 'on_dvr', 'client_id': '1v62389x', 'ip': '172.19.0.1', 'vhost': '__defaultVhost__', 'app': 'live', 'tcUrl': 'rtmp://[2406:da14:2b7:8500:38d8:bcae:8ff9:fe7]:11935/live', 'stream': 'hehe', 'param': '', 'cwd': '/usr/local/srs', 'file': './objs/nginx/html/record/live/hehe.2024-03-22.05:41:56.1711086116534.flv', 'stream_url': '/live/hehe', 'stream_id': 'vid-7tm5r5z'}
    RecordFileManager.record_file_update()
    return 0


@app.get("/stream/query_record/{stream_name}")
async def read_stream_name_record_file_list(stream_name: str):
    record_file_list = RecordFileManager.get_base_model_record_file_list(
        stream_name)
    return {"stream_name": stream_name, "files": record_file_list}


async def send_bytes_range_requests(file_path: str, start: int, end: int):
    """Send a file in chunks using Range Requests specification RFC7233

    `start` and `end` parameters are inclusive due to specification
    """
    async with aiofiles.open(file_path, mode="rb") as f:
        await f.seek(start)
        while (pos := await f.tell()) <= end:
            read_size = min(CHUNK_SIZE, end + 1 - pos)
            yield await f.read(read_size)


def _get_range_header(range_header: str, file_size: int) -> tuple[int, int]:
    def _invalid_range():
        return HTTPException(
            status.HTTP_416_REQUESTED_RANGE_NOT_SATISFIABLE,
            detail=f"Invalid request range (Range:{range_header!r})",
        )

    try:
        h = range_header.replace("bytes=", "").split("-")
        start = int(h[0]) if h[0] != "" else 0
        end = int(h[1]) if h[1] != "" else file_size - 1
    except ValueError:
        raise _invalid_range()

    if start > end or start < 0 or end > file_size - 1:
        raise _invalid_range()
    return start, end


@app.get("/stream/record/p/{file_name}")
async def streaming_response_stream_record(file_name: str, request: Request):
    file_path = RecordFileManager.find_record_file_by_name(file_name)
    if file_path == '':
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    file_size = os.stat(file_path).st_size
    range_header = request.headers.get("range")
    headers = {
        "content-type": "video/mp4",
        "accept-ranges": "bytes",
        "content-encoding": "identity",
        "content-length": str(file_size),
        "access-control-expose-headers": (
            "content-type, accept-ranges, content-length, "
            "content-range, content-encoding"
        ),
    }
    start = 0
    end = file_size - 1
    status_code = status.HTTP_200_OK

    if range_header is not None:
        start, end = _get_range_header(range_header, file_size)
        size = end - start + 1
        headers["content-length"] = str(size)
        headers["content-range"] = f"bytes {start}-{end}/{file_size}"
        status_code = status.HTTP_206_PARTIAL_CONTENT
    return StreamingResponse(
        send_bytes_range_requests(file_path, start, end),
        headers=headers,
        status_code=status_code,
    )


@app.get("/stream/record/d/{file_name}")
async def download_record_file(file_name: str, request: Request):
    return await streaming_response_stream_record(file_name, request)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=11985)
