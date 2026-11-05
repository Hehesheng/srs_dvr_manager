import logging.config
import logging.handlers
import os
from contextlib import asynccontextmanager

import uvicorn
import yaml
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from dvr.record_manager import WebDavRecordManager
from dvr.router import router as dvr_router

if not os.path.exists(os.path.dirname(__file__) + "/logs"):
    os.mkdir(os.path.dirname(__file__) + "/logs")
with open("logging_config.yaml", "r") as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger(__name__)

for handle in logger.handlers:
    if isinstance(handle, logging.handlers.TimedRotatingFileHandler):
        handle.suffix = "%Y-%m-%d.log"


@asynccontextmanager
async def lifespan(app: FastAPI):
    record_mgr = WebDavRecordManager()
    await record_mgr.init()
    app.state.record_mgr = record_mgr

    # Media hall manager will be initialized here once hall/ is implemented.
    # app.state.hall = HallManager()
    # await app.state.hall.start()

    yield

    # if app.state.hall is not None:
    #     await app.state.hall.stop()
    await record_mgr.close()


app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(dvr_router)


@app.get("/")
def read_root():
    return {"Hello": "World"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=11985)
