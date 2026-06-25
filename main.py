import asyncio
import logging
import random
import sys
import threading
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from enum import Enum
from typing import Optional

from fastapi import BackgroundTasks, FastAPI, Request, WebSocket, WebSocketDisconnect, status
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, field_validator
from fastapi.exceptions import RequestValidationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger("transflow")


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


MAX_FILENAME_LENGTH = 255
ALLOWED_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.wmv', '.flv', '.webm', '.mp3', '.wav', '.aac', '.ogg', '.m4a'}


class TranscodeRequest(BaseModel):
    file_name: str
    target_format: TargetFormat
    target_resolution: TargetResolution
    simulate_failure: bool = False

    @field_validator('file_name')
    @classmethod
    def validate_file_name(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError('File name cannot be empty')
        if len(v) > MAX_FILENAME_LENGTH:
            raise ValueError(f'File name cannot exceed {MAX_FILENAME_LENGTH} characters')
        if '/' in v or '\\' in v:
            raise ValueError('File name cannot contain path separators')
        forbidden_chars = '<>:"|?*'
        for char in forbidden_chars:
            if char in v:
                raise ValueError(f'File name contains invalid character: {char}')
        if v.startswith('.'):
            raise ValueError('File name cannot start with a dot')
        return v


FAILURE_PROBABILITY = 0.15

JOB_TTL_SECONDS = 3600


class JobStatus(BaseModel):
    job_id: str
    status: JobState
    progress: int
    current_step: str
    error_message: Optional[str] = None


class JobData(BaseModel):
    job_id: str
    file_name: str
    target_format: TargetFormat
    target_resolution: TargetResolution
    status: JobState
    progress: int
    current_step: str
    error_message: Optional[str] = None
    simulate_failure: bool = False
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
        simulate_failure: bool = False,
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
            error_message=None,
            simulate_failure=simulate_failure,
            created_at=now,
            updated_at=now,
        )
        with self._lock:
            self._jobs[job_id] = job
        logger.info(
            "Job created: job_id=%s file=%s format=%s resolution=%s",
            job_id, file_name, target_format.value, target_resolution.value
        )
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
        error_message: Optional[str] = None,
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
            if error_message is not None:
                updates["error_message"] = error_message
            updated = job.model_copy(update=updates)
            self._jobs[job_id] = updated
            return updated

    def list_jobs(self) -> list[JobData]:
        with self._lock:
            return list(self._jobs.values())

    def cleanup_expired_jobs(self, ttl_seconds: int = JOB_TTL_SECONDS) -> int:
        now = datetime.utcnow()
        cutoff = now - timedelta(seconds=ttl_seconds)
        expired_ids: list[str] = []
        with self._lock:
            for job_id, job in self._jobs.items():
                if job.status in (JobState.SUCCESS, JobState.FAILED):
                    if job.updated_at < cutoff:
                        expired_ids.append(job_id)
            for job_id in expired_ids:
                del self._jobs[job_id]
        if expired_ids:
            logger.info("Cleanup: removed %d expired job(s)", len(expired_ids))
        return len(expired_ids)

    def get_stats(self) -> dict:
        with self._lock:
            total = len(self._jobs)
            by_status = {}
            for job in self._jobs.values():
                status_key = job.status.value
                by_status[status_key] = by_status.get(status_key, 0) + 1
            return {"total_jobs": total, "by_status": by_status}


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
            count = len(self._connections[job_id])
        logger.info("WebSocket connected: job_id=%s clients=%d", job_id, count)

    async def disconnect(self, job_id: str, websocket: WebSocket) -> None:
        async with self._lock:
            if job_id in self._connections:
                if websocket in self._connections[job_id]:
                    self._connections[job_id].remove(websocket)
                if not self._connections[job_id]:
                    del self._connections[job_id]
                    remaining = 0
                else:
                    remaining = len(self._connections[job_id])
            else:
                remaining = 0
        logger.info("WebSocket disconnected: job_id=%s remaining=%d", job_id, remaining)

    async def broadcast_to_job(self, job_id: str, message: dict) -> None:
        async with self._lock:
            connections = self._connections.get(job_id, []).copy()

        disconnected: list[WebSocket] = []
        for websocket in connections:
            try:
                await websocket.send_json(message)
            except Exception as e:
                logger.warning("WebSocket send failed: job_id=%s error=%s", job_id, str(e))
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
        "error_message": job.error_message,
    }
    await connection_manager.broadcast_to_job(job_id, message)


SIMULATED_ERRORS = [
    "Codec initialization failed: unsupported format combination",
    "Memory allocation error during frame processing",
    "Input stream corrupted at byte offset 0x{:08x}".format(random.randint(0, 0xFFFFFF)),
    "Hardware encoder unavailable: fallback failed",
    "Audio sync lost: timestamp discontinuity detected",
    "Container format mismatch: cannot mux streams",
]


async def run_transcoding_pipeline(job_id: str) -> None:
    job = job_manager.get_job(job_id)
    if job is None:
        logger.warning("Pipeline start failed: job_id=%s not found", job_id)
        return

    logger.info("Pipeline starting: job_id=%s file=%s", job_id, job.file_name)
    should_fail = job.simulate_failure or random.random() < FAILURE_PROBABILITY
    fail_at_step = random.randint(0, len(PROCESSING_STEPS) - 1) if should_fail else -1

    job_manager.update_job(job_id, status=JobState.PROCESSING, progress=0)
    await broadcast_job_update(job_id)

    try:
        for step_index, (step_name, start_progress, end_progress) in enumerate(PROCESSING_STEPS):
            logger.debug("Processing step: job_id=%s step=%s", job_id, step_name)
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

                if should_fail and step_index == fail_at_step and i >= increments // 2:
                    error_msg = random.choice(SIMULATED_ERRORS)
                    logger.error(
                        "Job failed: job_id=%s step=%s error=%s", job_id, step_name, error_msg
                    )
                    job_manager.update_job(
                        job_id,
                        status=JobState.FAILED,
                        current_step=f"Failed: {step_name}",
                        error_message=error_msg,
                    )
                    await broadcast_job_update(job_id)
                    return

        job_manager.update_job(
            job_id,
            status=JobState.SUCCESS,
            progress=100,
            current_step="Complete",
        )
        logger.info("Job completed: job_id=%s", job_id)
        await broadcast_job_update(job_id)
    except Exception as e:
        logger.exception("Job exception: job_id=%s", job_id)
        job_manager.update_job(
            job_id,
            status=JobState.FAILED,
            current_step="Error during processing",
            error_message=str(e) or "An unexpected error occurred",
        )
        await broadcast_job_update(job_id)


CLEANUP_INTERVAL_SECONDS = 300

_cleanup_task: Optional[asyncio.Task] = None


async def periodic_cleanup_task() -> None:
    logger.info("Cleanup task started: interval=%ds ttl=%ds", CLEANUP_INTERVAL_SECONDS, JOB_TTL_SECONDS)
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
        job_manager.cleanup_expired_jobs()


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _cleanup_task
    logger.info("Application starting")
    _cleanup_task = asyncio.create_task(periodic_cleanup_task())
    yield
    logger.info("Application shutting down")
    _cleanup_task.cancel()
    try:
        await _cleanup_task
    except asyncio.CancelledError:
        pass


app = FastAPI(lifespan=lifespan)


class ValidationErrorDetail(BaseModel):
    field: str
    message: str


class ValidationErrorResponse(BaseModel):
    detail: str
    errors: list[ValidationErrorDetail]


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    errors = []
    for error in exc.errors():
        loc = error.get('loc', [])
        field = '.'.join(str(l) for l in loc if l != 'body')
        msg = error.get('msg', 'Invalid value')
        if msg.startswith('Value error, '):
            msg = msg[13:]
        errors.append({"field": field, "message": msg})

    first_error = errors[0]['message'] if errors else 'Validation failed'
    logger.warning("Validation error: %s errors=%s", first_error, errors)
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "detail": first_error,
            "errors": errors
        }
    )


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en" class="h-full">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TransFlow - Media Transcoding Engine</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        @keyframes progress-glow {
            0%, 100% { box-shadow: 0 0 8px rgba(99, 102, 241, 0.6), 0 0 16px rgba(99, 102, 241, 0.3); }
            50% { box-shadow: 0 0 12px rgba(99, 102, 241, 0.8), 0 0 24px rgba(99, 102, 241, 0.5); }
        }
        @keyframes gradient-shift {
            0% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
            100% { background-position: 0% 50%; }
        }
        .progress-processing {
            background: linear-gradient(90deg, #6366f1, #818cf8, #a5b4fc, #818cf8, #6366f1);
            background-size: 200% 100%;
            animation: gradient-shift 2s ease-in-out infinite, progress-glow 1.5s ease-in-out infinite;
        }
        .progress-bar-smooth {
            transition: width 400ms cubic-bezier(0.4, 0, 0.2, 1);
        }
        .progress-success {
            background: linear-gradient(90deg, #10b981, #34d399);
            box-shadow: 0 0 8px rgba(16, 185, 129, 0.4);
        }
        .progress-failed {
            background: linear-gradient(90deg, #e11d48, #f43f5e);
            box-shadow: 0 0 8px rgba(225, 29, 72, 0.4);
        }
    </style>
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
                    <div class="flex items-center justify-between mb-4">
                        <h2 class="text-lg font-semibold text-white">Live Progress Matrix</h2>
                        <span id="jobs-counter" class="hidden px-2 py-1 text-xs font-medium bg-indigo-500/20 text-indigo-400 rounded-full"></span>
                    </div>
                    <div id="jobs-container" class="space-y-3 max-h-[500px] overflow-y-auto">
                        <div id="no-jobs-message" class="text-center py-8">
                            <svg class="mx-auto h-12 w-12 text-gray-600 mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M7 4V2m10 2V2M5 8h14M5 8a2 2 0 00-2 2v10a2 2 0 002 2h14a2 2 0 002-2V10a2 2 0 00-2-2M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4m-4 4h4" />
                            </svg>
                            <p class="text-gray-400 text-sm">No active jobs</p>
                            <p class="text-gray-500 text-xs mt-1">Submit a transcode request to begin</p>
                        </div>
                    </div>
                </section>
            </div>
        </main>
    </div>

    <script>
        const form = document.getElementById('transcode-form');
        const jobsContainer = document.getElementById('jobs-container');
        const noJobsMessage = document.getElementById('no-jobs-message');
        const jobsCounter = document.getElementById('jobs-counter');
        const submitButton = form.querySelector('button[type="submit"]');

        const jobHistory = new Map();

        function updateJobsCounter() {
            const total = jobHistory.size;
            const active = Array.from(jobHistory.values()).filter(j => j.status === 'PENDING' || j.status === 'PROCESSING').length;
            if (total > 0) {
                if (active > 0) {
                    jobsCounter.textContent = `${active} active / ${total} total`;
                } else {
                    jobsCounter.textContent = `${total} job${total > 1 ? 's' : ''}`;
                }
                jobsCounter.classList.remove('hidden');
            } else {
                jobsCounter.classList.add('hidden');
            }
        }

        function clearFieldErrors() {
            document.querySelectorAll('.field-error').forEach(el => el.remove());
            document.querySelectorAll('.field-error-border').forEach(el => {
                el.classList.remove('field-error-border', 'border-red-500', 'ring-1', 'ring-red-500');
            });
        }

        function showFieldError(field, message) {
            const input = document.getElementById(field);
            if (!input) return;
            input.classList.add('field-error-border', 'border-red-500', 'ring-1', 'ring-red-500');
            const errorSpan = document.createElement('span');
            errorSpan.className = 'field-error text-red-400 text-xs mt-1 block';
            errorSpan.textContent = message;
            input.parentNode.appendChild(errorSpan);
        }

        function showError(message, errors = null) {
            clearFieldErrors();
            const existingAlerts = form.querySelectorAll('.error-alert');
            existingAlerts.forEach(el => el.remove());

            const errorDiv = document.createElement('div');
            errorDiv.className = 'error-alert bg-red-900/50 border border-red-700 text-red-200 px-4 py-3 rounded-md mb-4';

            let html = `<div class="flex items-start gap-2">
                <svg class="w-5 h-5 text-red-400 flex-shrink-0 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <div>
                    <strong class="font-semibold">Validation Error</strong>
                    <p class="text-sm mt-1">${message}</p>`;

            if (errors && errors.length > 1) {
                html += '<ul class="list-disc list-inside text-sm mt-2 space-y-1">';
                errors.forEach(err => {
                    html += `<li><span class="text-red-300">${err.field}:</span> ${err.message}</li>`;
                    showFieldError(err.field, err.message);
                });
                html += '</ul>';
            } else if (errors && errors.length === 1) {
                showFieldError(errors[0].field, errors[0].message);
            }

            html += '</div></div>';
            errorDiv.innerHTML = html;
            form.insertBefore(errorDiv, form.firstChild);
            setTimeout(() => errorDiv.remove(), 8000);
        }

        function createJobCard(jobId, fileName, targetFormat, targetResolution, submittedAt) {
            const card = document.createElement('div');
            card.id = `job-${jobId}`;
            card.className = 'bg-gray-700/50 border border-gray-600 rounded-lg p-4 transition-all duration-200';
            card.innerHTML = `
                <div class="job-header border-b border-gray-600 pb-3 mb-3">
                    <div class="flex items-center justify-between mb-1">
                        <h3 class="text-sm font-semibold text-white truncate" title="${fileName}">${fileName}</h3>
                        <div class="job-status-badge">
                            <span class="job-status px-2.5 py-1 text-xs font-semibold rounded-full bg-gray-500/20 text-gray-400">PENDING</span>
                        </div>
                    </div>
                    <div class="flex items-center gap-2 text-xs text-gray-400">
                        <span class="font-mono bg-gray-800 px-2 py-0.5 rounded">${jobId.substring(0, 8)}</span>
                        <span>&middot;</span>
                        <span class="uppercase font-medium">${targetFormat}</span>
                        <span>&middot;</span>
                        <span>${targetResolution}</span>
                    </div>
                </div>
                <div class="job-step-container mb-3">
                    <div class="flex items-center justify-between text-sm mb-1">
                        <span class="job-step text-gray-300 font-medium">Queued</span>
                        <span class="job-progress-text text-gray-400">0%</span>
                    </div>
                    <div class="job-progress-container w-full bg-gray-600/80 rounded-full h-3 overflow-hidden shadow-inner">
                        <div class="job-progress h-3 rounded-full progress-bar-smooth bg-indigo-500" style="width: 0%"></div>
                    </div>
                </div>
                <div class="job-error-container hidden mt-3 p-3 bg-rose-900/20 border border-rose-800/50 rounded-md">
                    <div class="flex items-start gap-2">
                        <svg class="w-4 h-4 text-rose-400 mt-0.5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        <span class="job-error-message text-sm text-rose-300"></span>
                    </div>
                </div>
                <div class="job-details flex items-center justify-between text-xs text-gray-500 border-t border-gray-600 pt-2 mt-2">
                    <span class="job-submitted">Submitted: ${submittedAt}</span>
                    <span class="job-id-full font-mono" title="${jobId}">${jobId}</span>
                </div>
            `;
            return card;
        }

        function updateJobCard(jobId, data) {
            const card = document.getElementById(`job-${jobId}`);
            if (!card) return;

            const statusBadge = card.querySelector('.job-status');
            const stepText = card.querySelector('.job-step');
            const progressBar = card.querySelector('.job-progress');
            const progressText = card.querySelector('.job-progress-text');
            const errorContainer = card.querySelector('.job-error-container');
            const errorMessage = card.querySelector('.job-error-message');

            statusBadge.textContent = data.status;
            stepText.textContent = data.current_step;
            progressBar.style.width = `${data.progress}%`;
            progressText.textContent = `${data.progress}%`;

            statusBadge.classList.remove('bg-gray-500/20', 'text-gray-400',
                                         'bg-yellow-500/20', 'text-yellow-400', 'animate-pulse',
                                         'bg-emerald-500/20', 'text-emerald-400',
                                         'bg-rose-900/30', 'text-rose-400');

            progressBar.classList.remove('bg-indigo-500', 'progress-processing', 'progress-success', 'progress-failed');

            if (data.status === 'PENDING') {
                statusBadge.classList.add('bg-gray-500/20', 'text-gray-400');
                progressBar.classList.add('bg-indigo-500');
                errorContainer.classList.add('hidden');
            } else if (data.status === 'PROCESSING') {
                statusBadge.classList.add('bg-yellow-500/20', 'text-yellow-400', 'animate-pulse');
                progressBar.classList.add('progress-processing');
                errorContainer.classList.add('hidden');
            } else if (data.status === 'SUCCESS') {
                statusBadge.classList.add('bg-emerald-500/20', 'text-emerald-400');
                progressBar.classList.add('progress-success');
                errorContainer.classList.add('hidden');
            } else if (data.status === 'FAILED') {
                statusBadge.classList.add('bg-rose-900/30', 'text-rose-400');
                progressBar.classList.add('progress-failed');
                card.classList.add('border-rose-800/50');
                if (data.error_message) {
                    errorMessage.textContent = data.error_message;
                    errorContainer.classList.remove('hidden');
                }
            }

            const jobData = jobHistory.get(jobId);
            if (jobData) {
                jobData.status = data.status;
                jobData.progress = data.progress;
                jobData.currentStep = data.current_step;
                jobData.errorMessage = data.error_message;
            }
            updateJobsCounter();
        }

        class ValidationError extends Error {
            constructor(message, errors) {
                super(message);
                this.name = 'ValidationError';
                this.errors = errors;
            }
        }

        async function submitJob(formData) {
            const payload = {
                file_name: formData.get('file_name'),
                target_format: formData.get('target_format'),
                target_resolution: formData.get('target_resolution')
            };

            const response = await fetch('/api/transcode', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify(payload)
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({}));
                if (response.status === 422 && errorData.errors) {
                    throw new ValidationError(errorData.detail, errorData.errors);
                }
                throw new Error(errorData.detail || `Request failed with status ${response.status}`);
            }

            return response.json();
        }

        function formatTimestamp(date) {
            return date.toLocaleTimeString('en-US', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        }

        function connectWebSocket(jobId) {
            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
            const wsUrl = `${protocol}//${window.location.host}/ws/progress/${jobId}`;
            const ws = new WebSocket(wsUrl);

            const jobData = jobHistory.get(jobId);
            if (jobData) {
                jobData.ws = ws;
            }

            ws.onopen = () => {
                console.log(`WebSocket connected for job ${jobId}`);
            };

            ws.onmessage = (event) => {
                try {
                    const data = JSON.parse(event.data);
                    updateJobCard(jobId, data);

                    if (data.status === 'SUCCESS' || data.status === 'FAILED') {
                        ws.close();
                    }
                } catch (err) {
                    console.error('Failed to parse WebSocket message:', err);
                }
            };

            ws.onclose = (event) => {
                console.log(`WebSocket closed for job ${jobId}`, event.code);
                const jobData = jobHistory.get(jobId);
                if (jobData) {
                    jobData.ws = null;
                }
            };

            ws.onerror = (error) => {
                console.error(`WebSocket error for job ${jobId}:`, error);
            };
        }

        async function loadExistingJobs() {
            try {
                const response = await fetch('/api/jobs');
                if (!response.ok) {
                    console.error('Failed to load existing jobs:', response.status);
                    return;
                }

                const jobs = await response.json();
                if (jobs.length === 0) {
                    return;
                }

                if (noJobsMessage && noJobsMessage.parentNode) {
                    noJobsMessage.remove();
                }

                for (const job of jobs) {
                    const createdAt = new Date(job.created_at);
                    const card = createJobCard(
                        job.job_id,
                        job.file_name,
                        job.target_format,
                        job.target_resolution,
                        formatTimestamp(createdAt)
                    );
                    jobsContainer.appendChild(card);

                    jobHistory.set(job.job_id, {
                        jobId: job.job_id,
                        fileName: job.file_name,
                        targetFormat: job.target_format,
                        targetResolution: job.target_resolution,
                        submittedAt: createdAt,
                        status: job.status,
                        progress: job.progress,
                        currentStep: job.current_step,
                        errorMessage: job.error_message,
                        ws: null
                    });

                    updateJobCard(job.job_id, {
                        status: job.status,
                        progress: job.progress,
                        current_step: job.current_step,
                        error_message: job.error_message
                    });

                    if (job.status === 'PENDING' || job.status === 'PROCESSING') {
                        connectWebSocket(job.job_id);
                    }
                }

                updateJobsCounter();
            } catch (error) {
                console.error('Error loading existing jobs:', error);
            }
        }

        document.addEventListener('DOMContentLoaded', loadExistingJobs);

        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            clearFieldErrors();

            const formData = new FormData(form);
            const fileName = formData.get('file_name').trim();

            if (!fileName) {
                showError('File name cannot be empty', [{field: 'file_name', message: 'File name cannot be empty'}]);
                return;
            }

            submitButton.disabled = true;
            submitButton.innerHTML = `
                <svg class="animate-spin -ml-1 mr-3 h-5 w-5 text-white inline" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
                    <path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                </svg>
                Submitting...
            `;

            try {
                const result = await submitJob(formData);
                const jobId = result.job_id;
                const targetFormat = formData.get('target_format');
                const targetResolution = formData.get('target_resolution');
                const submittedAt = new Date();

                if (noJobsMessage && noJobsMessage.parentNode) {
                    noJobsMessage.remove();
                }

                const card = createJobCard(
                    jobId,
                    fileName,
                    targetFormat,
                    targetResolution,
                    formatTimestamp(submittedAt)
                );
                jobsContainer.insertBefore(card, jobsContainer.firstChild);

                jobHistory.set(jobId, {
                    jobId,
                    fileName,
                    targetFormat,
                    targetResolution,
                    submittedAt,
                    status: 'PENDING',
                    progress: 0,
                    currentStep: 'Queued',
                    errorMessage: null,
                    ws: null
                });
                updateJobsCounter();

                connectWebSocket(jobId);

                form.reset();

            } catch (error) {
                if (error instanceof ValidationError) {
                    showError(error.message, error.errors);
                } else {
                    showError(error.message || 'Failed to submit job. Please try again.');
                }
            } finally {
                submitButton.disabled = false;
                submitButton.textContent = 'Submit Transcode Job';
            }
        });
    </script>
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
        simulate_failure=request.simulate_failure,
    )
    background_tasks.add_task(run_transcoding_pipeline, job.job_id)
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"job_id": job.job_id},
    )


@app.get("/api/jobs")
async def list_jobs(status: Optional[JobState] = None) -> JSONResponse:
    job_manager.cleanup_expired_jobs()
    jobs = job_manager.list_jobs()
    if status is not None:
        jobs = [job for job in jobs if job.status == status]
    jobs_sorted = sorted(jobs, key=lambda j: j.created_at, reverse=True)
    return JSONResponse(
        content=[
            {
                "job_id": job.job_id,
                "file_name": job.file_name,
                "target_format": job.target_format.value,
                "target_resolution": job.target_resolution.value,
                "status": job.status.value,
                "progress": job.progress,
                "current_step": job.current_step,
                "error_message": job.error_message,
                "created_at": job.created_at.isoformat(),
                "updated_at": job.updated_at.isoformat(),
            }
            for job in jobs_sorted
        ]
    )


@app.get("/api/jobs/{job_id}")
async def get_job(job_id: str) -> JSONResponse:
    job = job_manager.get_job(job_id)
    if job is None:
        logger.debug("Job not found: job_id=%s", job_id)
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={"detail": "Job not found"},
        )
    return JSONResponse(
        content={
            "job_id": job.job_id,
            "file_name": job.file_name,
            "target_format": job.target_format.value,
            "target_resolution": job.target_resolution.value,
            "status": job.status.value,
            "progress": job.progress,
            "current_step": job.current_step,
            "error_message": job.error_message,
            "created_at": job.created_at.isoformat(),
            "updated_at": job.updated_at.isoformat(),
        }
    )


@app.websocket("/ws/progress/{job_id}")
async def websocket_progress(websocket: WebSocket, job_id: str):
    job = job_manager.get_job(job_id)
    if job is None:
        logger.warning("WebSocket rejected: job_id=%s not found", job_id)
        await websocket.close(code=4004)
        return

    await connection_manager.connect(job_id, websocket)

    try:
        initial_message = {
            "status": job.status.value,
            "progress": job.progress,
            "current_step": job.current_step,
            "error_message": job.error_message,
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
