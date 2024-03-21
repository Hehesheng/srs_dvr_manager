from typing import Union

import uvicorn
from fastapi import FastAPI
from fastapi.responses import FileResponse, Response

import RecordFileManager

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/stream/{stream_name}")
async def read_stream_name_record_file_list(stream_name: str):
    record_file_list = RecordFileManager.get_record_file_list(stream_name)
    return {"stream_name": stream_name, "files": record_file_list}


@app.get("/download_record/{file_name}")
async def download_file(file_name: str):
    file_path = RecordFileManager.find_record_file_by_name(file_name)
    if file_path == '':
        return Response(status_code=404)
    return FileResponse(path=file_path, filename=file_name, media_type='video/*')


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=11985)
