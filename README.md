# TransFlow

An asynchronous media transcoding engine built with FastAPI. TransFlow demonstrates real-time WebSocket communication, background task processing, and modern async Python patterns through a simulated media processing pipeline.

## Features

- **Real-Time Progress Updates**: WebSocket connections stream live job progress to connected clients
- **Async Background Processing**: Jobs run asynchronously via FastAPI's BackgroundTasks without blocking the server
- **In-Memory Job Management**: Thread-safe state manager tracks job lifecycles (PENDING → PROCESSING → SUCCESS/FAILED)
- **Automatic Job Cleanup**: Expired completed jobs are purged periodically to prevent memory growth
- **Multi-Client Support**: Multiple WebSocket clients can monitor the same job simultaneously
- **Input Validation**: Pydantic models enforce file name rules and format constraints
- **Responsive Dashboard**: Dark-themed Tailwind CSS UI with smooth progress bar animations
- **Simulated Pipeline**: Four-stage processing simulation (metadata analysis, audio extraction, compression, finalization)

## Quick Start

### Prerequisites

- Python 3.10+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

### Installation with uv

```bash
# Clone the repository
git clone https://github.com/your-username/transflow.git
cd transflow

# Create virtual environment and install dependencies
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install -r requirements.txt
```

### Installation with pip

```bash
# Clone the repository
git clone https://github.com/your-username/transflow.git
cd transflow

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Running the Server

```bash
uvicorn main:app --reload
```

The server starts at `http://localhost:8000`. Open this URL in your browser to access the dashboard.

For production:

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
```

## API Reference

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Dashboard HTML page |
| `GET` | `/health` | Health check endpoint |
| `POST` | `/api/transcode` | Create a new transcode job |
| `GET` | `/api/jobs` | List all jobs (optional `status` filter) |
| `GET` | `/api/jobs/{job_id}` | Get single job details |
| `WS` | `/ws/progress/{job_id}` | WebSocket for real-time progress |

### POST /api/transcode

Creates a new transcoding job and returns immediately with a job ID.

**Request Body:**

```json
{
  "file_name": "video.mp4",
  "target_format": "webm",
  "target_resolution": "720p",
  "simulate_failure": false
}
```

**Parameters:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `file_name` | string | Yes | Name of the file (1-255 chars, no path separators) |
| `target_format` | string | Yes | Output format: `mp4`, `webm`, or `mp3` |
| `target_resolution` | string | Yes | Output resolution: `1080p`, `720p`, or `480p` |
| `simulate_failure` | boolean | No | Force job failure for testing (default: false) |

**Response (202 Accepted):**

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

### GET /api/jobs

Lists all jobs, sorted by creation time (newest first).

**Query Parameters:**

| Parameter | Type | Description |
|-----------|------|-------------|
| `status` | string | Filter by status: `PENDING`, `PROCESSING`, `SUCCESS`, `FAILED` |

**Response:**

```json
[
  {
    "job_id": "550e8400-e29b-41d4-a716-446655440000",
    "file_name": "video.mp4",
    "target_format": "webm",
    "target_resolution": "720p",
    "status": "PROCESSING",
    "progress": 45,
    "current_step": "Compressing blocks",
    "error_message": null,
    "created_at": "2024-01-15T10:30:00.000000",
    "updated_at": "2024-01-15T10:30:15.000000"
  }
]
```

### GET /api/jobs/{job_id}

Returns details for a specific job.

**Response (200 OK):**

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "file_name": "video.mp4",
  "target_format": "webm",
  "target_resolution": "720p",
  "status": "SUCCESS",
  "progress": 100,
  "current_step": "Complete",
  "error_message": null,
  "created_at": "2024-01-15T10:30:00.000000",
  "updated_at": "2024-01-15T10:30:25.000000"
}
```

**Response (404 Not Found):**

```json
{
  "detail": "Job not found"
}
```

## WebSocket Usage

Connect to `/ws/progress/{job_id}` to receive real-time progress updates.

### JavaScript Example

```javascript
const jobId = "550e8400-e29b-41d4-a716-446655440000";
const ws = new WebSocket(`ws://localhost:8000/ws/progress/${jobId}`);

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log(`Status: ${data.status}`);
  console.log(`Progress: ${data.progress}%`);
  console.log(`Step: ${data.current_step}`);

  if (data.status === "SUCCESS" || data.status === "FAILED") {
    ws.close();
  }
};

ws.onclose = (event) => {
  if (event.code === 4004) {
    console.error("Job not found");
  }
};
```

### Python Example

```python
import asyncio
import websockets
import json

async def monitor_job(job_id: str):
    uri = f"ws://localhost:8000/ws/progress/{job_id}"
    async with websockets.connect(uri) as ws:
        async for message in ws:
            data = json.loads(message)
            print(f"[{data['status']}] {data['progress']}% - {data['current_step']}")
            if data["status"] in ("SUCCESS", "FAILED"):
                break

asyncio.run(monitor_job("550e8400-e29b-41d4-a716-446655440000"))
```

### Message Format

Each WebSocket message contains:

```json
{
  "status": "PROCESSING",
  "progress": 45,
  "current_step": "Compressing blocks",
  "error_message": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `status` | string | Job state: `PENDING`, `PROCESSING`, `SUCCESS`, `FAILED` |
| `progress` | integer | Completion percentage (0-100) |
| `current_step` | string | Current processing stage |
| `error_message` | string/null | Error details if status is `FAILED` |

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         TransFlow                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     ┌──────────────┐     ┌──────────────────┐ │
│  │   Browser    │────▶│  Dashboard   │────▶│  POST /transcode │ │
│  │              │     │  (HTML/JS)   │     │                  │ │
│  └──────┬───────┘     └──────────────┘     └────────┬─────────┘ │
│         │                                           │            │
│         │ WebSocket                                 ▼            │
│         │                                  ┌──────────────────┐  │
│         │                                  │  JobStateManager │  │
│         │                                  │  (In-Memory)     │  │
│         │                                  └────────┬─────────┘  │
│         │                                           │            │
│         │                                           ▼            │
│  ┌──────▼───────┐                          ┌──────────────────┐  │
│  │ Connection   │◀─────────────────────────│ BackgroundTasks  │  │
│  │   Manager    │      broadcast           │ (Pipeline)       │  │
│  └──────────────┘                          └──────────────────┘  │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### Components

| Component | Description |
|-----------|-------------|
| **Dashboard** | Single-page HTML/JS app embedded in the FastAPI response |
| **JobStateManager** | Thread-safe dict storing job metadata and state |
| **ConnectionManager** | Tracks WebSocket connections per job ID |
| **Pipeline** | Async function simulating multi-step transcoding |
| **Cleanup Task** | Background coroutine purging expired jobs every 5 minutes |

### Processing Pipeline

The simulated pipeline progresses through four stages:

1. **Analyzing metadata** (0-15%)
2. **Extracting audio** (15-40%)
3. **Compressing blocks** (40-85%)
4. **Finalizing container** (85-100%)

Each stage includes random delays (0.3-0.8s per increment) to simulate real processing. Jobs have a 15% random failure chance to demonstrate error handling.

### Job Lifecycle

```
PENDING ──▶ PROCESSING ──▶ SUCCESS
                │
                └──────────▶ FAILED
```

- **PENDING**: Job created, waiting to start
- **PROCESSING**: Pipeline running, progress updates streaming
- **SUCCESS**: Processing complete (progress = 100%)
- **FAILED**: Error occurred (error_message populated)

## Configuration

Key constants in `main.py`:

| Constant | Default | Description |
|----------|---------|-------------|
| `JOB_TTL_SECONDS` | 3600 | Time before completed jobs expire |
| `CLEANUP_INTERVAL_SECONDS` | 300 | Cleanup task frequency |
| `FAILURE_PROBABILITY` | 0.15 | Random failure rate |
| `MAX_FILENAME_LENGTH` | 255 | Maximum file name length |

## Demo

1. Open `http://localhost:8000` in your browser
2. Enter a file name (e.g., `interview.mov`)
3. Select target format (MP4, WebM, or MP3) and resolution
4. Click "Submit Transcode Job"
5. Watch the progress bar animate in real-time
6. Submit multiple jobs to see parallel tracking

The dashboard shows:
- Live progress bars with gradient animations during processing
- Status badges (yellow pulsing for active, green for complete, red for failed)
- Job metadata (ID, format, resolution, timestamps)
- Error messages for failed jobs
- Job history persisted across page refreshes

## Development

### Running Tests

```bash
# Install test dependencies
uv pip install pytest pytest-asyncio httpx

# Run tests
pytest
```

### Code Structure

```
transflow/
├── main.py           # Complete application (single file)
├── requirements.txt  # Dependencies (fastapi, uvicorn)
└── README.md         # This file
```

The single-file architecture ensures the application is immediately runnable without complex setup.

## License

MIT

## Acknowledgments

Built with:
- [FastAPI](https://fastapi.tiangolo.com/) - Modern Python web framework
- [Uvicorn](https://www.uvicorn.org/) - ASGI server
- [Pydantic](https://docs.pydantic.dev/) - Data validation
- [Tailwind CSS](https://tailwindcss.com/) - Utility-first CSS (via CDN)
