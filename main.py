import asyncio
import random
import threading
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel


class TargetFormat(str, Enum):
    MP4 = "mp4"
    WEBM = "webm"
    MP3 = "mp3"


class TargetResolution(str, Enum):
    RES_1080P = "1080p"
    RES_720P = "720p"
    RES_480P = "480p"


class JobState(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class TranscodeRequest(BaseModel):
    file_name: str
    target_format: TargetFormat
    target_resolution: TargetResolution


class JobStatus(BaseModel):
    job_id: str
    status: JobState
    progress: int
    current_step: str


class JobData(BaseModel):
    job_id: str
    file_name: str
    target_format: TargetFormat
    target_resolution: TargetResolution
    status: JobState
    progress: int
    current_step: str
    created_at: datetime
    updated_at: datetime


class JobStateManager:
    def __init__(self) -> None:
        self._jobs: dict[str, JobData] = {}
        self._lock = threading.Lock()

    def create_job(
        self,
        file_name: str,
        target_format: TargetFormat,
        target_resolution: TargetResolution,
    ) -> JobData:
        job_id = str(uuid.uuid4())
        now = datetime.utcnow()
        job = JobData(
            job_id=job_id,
            file_name=file_name,
            target_format=target_format,
            target_resolution=target_resolution,
            status=JobState.PENDING,
            progress=0,
            current_step="Queued",
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._jobs[job_id] = job
        return job

    def get_job(self, job_id: str) -> Optional[JobData]:
        with self._lock:
            return self._jobs.get(job_id)

    def update_progress(self, job_id: str, progress: int) -> Optional[JobData]:
        progress = max(0, min(100, progress))
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            updated = job.model_copy(
                update={"progress": progress, "updated_at": datetime.utcnow()}
            )
            self._jobs[job_id] = updated
            return updated

    def set_current_step(self, job_id: str, step: str) -> Optional[JobData]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            updated = job.model_copy(
                update={"current_step": step, "updated_at": datetime.utcnow()}
            )
            self._jobs[job_id] = updated
            return updated

    def set_status(self, job_id: str, status: JobState) -> Optional[JobData]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            updated = job.model_copy(
                update={"status": status, "updated_at": datetime.utcnow()}
            )
            self._jobs[job_id] = updated
            return updated

    def update_job(
        self,
        job_id: str,
        status: Optional[JobState] = None,
        progress: Optional[int] = None,
        current_step: Optional[str] = None,
    ) -> Optional[JobData]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return None
            updates: dict = {"updated_at": datetime.utcnow()}
            if status is not None:
                updates["status"] = status
            if progress is not None:
                updates["progress"] = max(0, min(100, progress))
            if current_step is not None:
                updates["current_step"] = current_step
            updated = job.model_copy(update=updates)
            self._jobs[job_id] = updated
            return updated

    def list_jobs(self) -> list[JobData]:
        with self._lock:
            return list(self._jobs.values())


job_manager = JobStateManager()


PROCESSING_STEPS = [
    ("Analyzing metadata", 0, 15),
    ("Extracting audio", 15, 40),
    ("Compressing blocks", 40, 85),
    ("Finalizing container", 85, 100),
]


async def run_transcoding_pipeline(job_id: str) -> None:
    job = job_manager.get_job(job_id)
    if job is None:
        return

    job_manager.update_job(job_id, status=JobState.PROCESSING, progress=0)

    try:
        for step_name, start_progress, end_progress in PROCESSING_STEPS:
            job_manager.update_job(
                job_id, current_step=step_name, progress=start_progress
            )

            progress_range = end_progress - start_progress
            increments = random.randint(3, 6)

            for i in range(increments):
                await asyncio.sleep(random.uniform(0.3, 0.8))
                current_progress = start_progress + int(
                    progress_range * (i + 1) / increments
                )
                job_manager.update_job(job_id, progress=current_progress)

        job_manager.update_job(
            job_id,
            status=JobState.SUCCESS,
            progress=100,
            current_step="Complete",
        )
    except Exception:
        job_manager.update_job(
            job_id,
            status=JobState.FAILED,
            current_step="Error during processing",
        )


app = FastAPI()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/transcode", status_code=status.HTTP_202_ACCEPTED)
async def create_transcode_job(
    request: TranscodeRequest, background_tasks: BackgroundTasks
) -> JSONResponse:
    job = job_manager.create_job(
        file_name=request.file_name,
        target_format=request.target_format,
        target_resolution=request.target_resolution,
    )
    background_tasks.add_task(run_transcoding_pipeline, job.job_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"job_id": job.job_id},
    )
