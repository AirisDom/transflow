import asyncio
import random
import threading
import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, WebSocket, WebSocketDisconnect, status
from fastapi.responses import HTMLResponse, JSONResponse
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


class ConnectionManager:
    def __init__(self) -> None:
        self._connections: dict[str, list[WebSocket]] = {}
        self._lock = asyncio.Lock()

    async def connect(self, job_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            if job_id not in self._connections:
                self._connections[job_id] = []
            self._connections[job_id].append(websocket)

    async def disconnect(self, job_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            if job_id in self._connections:
                if websocket in self._connections[job_id]:
                    self._connections[job_id].remove(websocket)
                if not self._connections[job_id]:
                    del self._connections[job_id]

    async def broadcast_to_job(self, job_id: str, message: dict) -> None:
        async with self._lock:
            connections = self._connections.get(job_id, []).copy()

        disconnected: list[WebSocket] = []
        for websocket in connections:
            try:
                await websocket.send_json(message)
            except Exception:
                disconnected.append(websocket)

        for websocket in disconnected:
            await self.disconnect(job_id, websocket)

    async def get_connection_count(self, job_id: str) -> int:
        async with self._lock:
            return len(self._connections.get(job_id, []))

    async def has_connections(self, job_id: str) -> bool:
        async with self._lock:
            return job_id in self._connections and len(self._connections[job_id]) > 0


connection_manager = ConnectionManager()


PROCESSING_STEPS = [
    ("Analyzing metadata", 0, 15),
    ("Extracting audio", 15, 40),
    ("Compressing blocks", 40, 85),
    ("Finalizing container", 85, 100),
]


async def broadcast_job_update(job_id: str) -> None:
    job = job_manager.get_job(job_id)
    if job is None:
        return
    message = {
        "status": job.status.value,
        "progress": job.progress,
        "current_step": job.current_step,
    }
    await connection_manager.broadcast_to_job(job_id, message)


async def run_transcoding_pipeline(job_id: str) -> None:
    job = job_manager.get_job(job_id)
    if job is None:
        return

    job_manager.update_job(job_id, status=JobState.PROCESSING, progress=0)
    await broadcast_job_update(job_id)

    try:
        for step_name, start_progress, end_progress in PROCESSING_STEPS:
            job_manager.update_job(
                job_id, current_step=step_name, progress=start_progress
            )
            await broadcast_job_update(job_id)

            progress_range = end_progress - start_progress
            increments = random.randint(3, 6)

            for i in range(increments):
                await asyncio.sleep(random.uniform(0.3, 0.8))
                current_progress = start_progress + int(
                    progress_range * (i + 1) / increments
                )
                job_manager.update_job(job_id, progress=current_progress)
                await broadcast_job_update(job_id)

        job_manager.update_job(
            job_id,
            status=JobState.SUCCESS,
            progress=100,
            current_step="Complete",
        )
        await broadcast_job_update(job_id)
    except Exception:
        job_manager.update_job(
            job_id,
            status=JobState.FAILED,
            current_step="Error during processing",
        )
        await broadcast_job_update(job_id)


app = FastAPI()


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en" class="h-full">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TransFlow - Media Transcoding Engine</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="h-full bg-gray-900 text-gray-100">
    <div class="min-h-full">
        <header class="bg-gray-800 border-b border-gray-700">
            <div class="mx-auto max-w-7xl px-4 py-4 sm:px-6 lg:px-8">
                <h1 class="text-2xl font-bold tracking-tight text-white">TransFlow</h1>
                <p class="text-sm text-gray-400">Asynchronous Media Transcoding Engine</p>
            </div>
        </header>

        <main class="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
            <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <!-- Configuration Desk (Left Column) -->
                <section class="bg-gray-800 rounded-lg border border-gray-700 p-6">
                    <h2 class="text-lg font-semibold text-white mb-4">Configuration Desk</h2>
                    <form id="transcode-form" class="space-y-4">
                        <div>
                            <label for="file_name" class="block text-sm font-medium text-gray-300 mb-1">File Name</label>
                            <input type="text" id="file_name" name="file_name"
                                   class="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-gray-100 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent"
                                   placeholder="video.mp4" required>
                        </div>
                        <div>
                            <label for="target_format" class="block text-sm font-medium text-gray-300 mb-1">Target Format</label>
                            <select id="target_format" name="target_format"
                                    class="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-gray-100 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent">
                                <option value="mp4">MP4</option>
                                <option value="webm">WebM</option>
                                <option value="mp3">MP3</option>
                            </select>
                        </div>
                        <div>
                            <label for="target_resolution" class="block text-sm font-medium text-gray-300 mb-1">Target Resolution</label>
                            <select id="target_resolution" name="target_resolution"
                                    class="w-full px-3 py-2 bg-gray-700 border border-gray-600 rounded-md text-gray-100 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-transparent">
                                <option value="1080p">1080p</option>
                                <option value="720p">720p</option>
                                <option value="480p">480p</option>
                            </select>
                        </div>
                        <button type="submit"
                                class="w-full py-3 px-4 bg-indigo-600 hover:bg-indigo-500 active:bg-indigo-700 text-white font-semibold text-lg rounded-lg shadow-lg shadow-indigo-500/30 transition-all duration-200 focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 focus:ring-offset-gray-800 transform hover:scale-[1.02]">
                            Submit Transcode Job
                        </button>
                    </form>
                </section>

                <!-- Live Progress Matrix (Right Column) -->
                <section class="bg-gray-800 rounded-lg border border-gray-700 p-6">
                    <h2 class="text-lg font-semibold text-white mb-4">Live Progress Matrix</h2>
                    <div id="jobs-container" class="space-y-4">
                        <p class="text-gray-400 text-sm" id="no-jobs-message">No active jobs. Submit a transcode request to begin.</p>
                    </div>
                </section>
            </div>
        </main>
    </div>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    return HTMLResponse(content=HTML_TEMPLATE, media_type="text/html")


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


@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str) -> JSONResponse:
    job = job_manager.get_job(job_id)
    if job is None:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"error": "Job not found"},
        )
    return JSONResponse(
        content=JobStatus(
            job_id=job.job_id,
            status=job.status,
            progress=job.progress,
            current_step=job.current_step,
        ).model_dump()
    )


@app.websocket("/ws/progress/{job_id}")
async def websocket_progress(websocket: WebSocket, job_id: str):
    job = job_manager.get_job(job_id)
    if job is None:
        await websocket.close(code=4004)
        return

    await connection_manager.connect(job_id, websocket)

    try:
        initial_message = {
            "status": job.status.value,
            "progress": job.progress,
            "current_step": job.current_step,
        }
        await websocket.send_json(initial_message)

        if job.status in (JobState.SUCCESS, JobState.FAILED):
            return

        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        await connection_manager.disconnect(job_id, websocket)
