from typing import Dict, Any
import os

import uvicorn
from fastapi import FastAPI, Body, Header
from fastapi.responses import FileResponse, Response, StreamingResponse
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
    record_file_list = RecordFileManager.get_record_file_list(stream_name)
    return {"stream_name": stream_name, "files": record_file_list}


@app.get("/stream/preview_record/{file_name}")
async def preview_stream_video(file_name: str, range: str = Header(None)):
    # print(f"preview {file_name} record: {range}")
    file_path = RecordFileManager.find_record_file_by_name(file_name)
    if file_path == '':
        return Response(status_code=404)
    filesize = os.stat(file_path).st_size
    start, end = range.replace("bytes=", "").split("-")
    start = int(start)
    end = int(end) if end else start + 4 * CHUNK_SIZE
    end = end if end < filesize else filesize
    async with aiofiles.open(file_path, "rb") as video:
        await video.seek(start)
        data = await video.read(end - start)
        headers = {
            'Content-Range': f'bytes {start}-{end - 1}/{filesize}',
            'Accept-Ranges': 'bytes'
        }
        # print(f"preview response: {headers}")
        return Response(content=data, status_code=206, headers=headers, media_type="video/mp4")


@app.get("/stream/download_record/{file_name}")
def download_file(file_name: str):
    file_path = RecordFileManager.find_record_file_by_name(file_name)
    if file_path == '':
        return Response(status_code=404)
    def iter_file():
        with open(file_path, mode='rb') as f:
            while chunk := f.read(CHUNK_SIZE):
                yield chunk
    return StreamingResponse(iter_file(), media_type='video/mp4')


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=11985)
