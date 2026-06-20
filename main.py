from enum import Enum

from fastapi import FastAPI
from pydantic import BaseModel


class TargetFormat(str, Enum):
    MP4 = "mp4"
    WEBM = "webm"
    MP3 = "mp3"


class TargetResolution(str, Enum):
    RES_1080P = "1080p"
    RES_720P = "720p"
    RES_480P = "480p"


class TranscodeRequest(BaseModel):
    file_name: str
    target_format: TargetFormat
    target_resolution: TargetResolution


class JobStatus(BaseModel):
    job_id: str
    status: str
    progress: int
    current_step: str


app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok"}
