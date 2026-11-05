import logging
import traceback
import urllib.parse
from typing import Any, Dict

from fastapi import APIRouter, Request, status
from fastapi.responses import Response, StreamingResponse

from .record_manager import WebDavRecordManager

logger = logging.getLogger(__name__)

router = APIRouter(tags=["dvr"])


def _get_record_mgr(request: Request) -> WebDavRecordManager | None:
    return getattr(request.app.state, "record_mgr", None)


@router.post("/stream/on_dvr/")
async def dvr_done_callback(request: Request, callback_context: Dict[Any, Any]):
    print(callback_context)
    # {'server_id': 'vid-390ik4q', 'service_id': '900vr5u1', 'action': 'on_dvr', 'client_id': '1v62389x', 'ip': '172.19.0.1', 'vhost': '__defaultVhost__', 'app': 'live', 'tcUrl': 'rtmp://[2406:da14:2b7:8500:38d8:bcae:8ff9:fe7]:11935/live', 'stream': 'hehe', 'param': '', 'cwd': '/usr/local/srs', 'file': './objs/nginx/html/record/live/hehe.2024-03-22.05:41:56.1711086116534.flv', 'stream_url': '/live/hehe', 'stream_id': 'vid-7tm5r5z'}
    url_param = callback_context.get("param")
    enable_record = True
    if url_param is not None:
        try:
            url_param = url_param.lstrip("?")
            url_param_dict = urllib.parse.parse_qs(url_param)
            url_param_dict = {k: v[0] if len(v) == 1 else v for k, v in url_param_dict.items()}
            record_value = url_param_dict.get("record", "true")
            if isinstance(record_value, str):
                enable_record = record_value.lower() == "true"
        except Exception as e:
            print(e, traceback.format_exc())
    record_file_name = callback_context.get("file", "").split("/")[-1]
    stream_name = callback_context.get("stream", "")
    incoming_path = callback_context.get("file", "")
    record_mgr = _get_record_mgr(request)
    if record_mgr is not None:
        await record_mgr.handle_record_file(
            stream_name=stream_name,
            file_name=record_file_name,
            incoming_path=incoming_path,
            enable_record=enable_record,
        )
    return {"code": 0}


@router.get("/stream/record/cover/{cover_name}")
async def get_record_cover(cover_name: str, req: Request):
    record_mgr = _get_record_mgr(req)
    if record_mgr is None:
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    stream = await record_mgr.fetch_cover(cover_name)
    if stream is None:
        return Response(status_code=404)
    return Response(stream, media_type="image/jpeg")


@router.get("/stream/cover/{stream_name}")
async def get_stream_cover(stream_name: str, req: Request):
    """Get live stream cover with 5-minute local cache."""
    record_mgr = _get_record_mgr(req)
    if record_mgr is None:
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    cover_bytes = await record_mgr.get_stream_cover(stream_name)
    if cover_bytes is None:
        return Response(status_code=404)
    return Response(cover_bytes, media_type="image/jpeg")


@router.get("/stream/cover_compat/{cover_name}")
async def get_stream_cover_compat(cover_name: str, req: Request):
    """Deprecated: Use /stream/record/cover/{cover_name} instead."""
    return await get_record_cover(cover_name, req)


@router.get("/stream/query_record/{stream_name}")
async def read_stream_name_record_file_list(stream_name: str, req: Request):
    record_mgr = _get_record_mgr(req)
    if record_mgr is None:
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    record_file_list = await record_mgr.list_records(stream_name)
    return {"stream_name": stream_name, "files": record_file_list}


@router.get("/stream/record/p/{file_name}")
async def streaming_response_stream_record(file_name: str, request: Request):
    record_mgr = _get_record_mgr(request)
    if record_mgr is None:
        return Response(status_code=status.HTTP_503_SERVICE_UNAVAILABLE)
    range_header = request.headers.get("range")
    status_code, headers, body = await record_mgr.stream_record(file_name, range_header)
    if status_code == status.HTTP_404_NOT_FOUND:
        return Response(status_code=status.HTTP_404_NOT_FOUND)
    if status_code not in (status.HTTP_200_OK, status.HTTP_206_PARTIAL_CONTENT):
        return Response(status_code=status.HTTP_502_BAD_GATEWAY)
    return StreamingResponse(body, headers=headers, status_code=status_code)


@router.get("/stream/record/d/{file_name}")
async def download_record_file(file_name: str, request: Request):
    return await streaming_response_stream_record(file_name, request)
