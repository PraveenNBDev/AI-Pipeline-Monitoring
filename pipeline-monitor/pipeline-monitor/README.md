# ⚡ DataPulse — AI Pipeline Monitor

A production-ready, AI-powered monitoring dashboard for your data pipeline:

```
Source → GCS Raw → GCS Preprocessor → BQ Load → Raw Layer → Hist Layer → ODS Layer
```

---

## 📸 Features

| Tab | What it shows |
|-----|--------------|
| **Overview** | Live pipeline topology, 4 real-time charts, top anomalies |
| **Alerts** | All detected anomalies with filters & charts |
| **Data Quality** | 9 automated checks (null, dup, spike, SLA, schema, recon…) |
| **Layer Health** | Per-layer record trend sparklines, issue counts |
| **Reconciliation** | Cross-layer record loss table + trend chart |
| **AI Agent** | Claude-powered chat for root cause analysis & remediation |

### Automated Checks
- ⬆ **Record Spike Detection** — Z-score ≥ 2.3σ above baseline
- ⬇ **Record Drop Detection** — Z-score ≤ -2.3σ below baseline
- ∅ **Null Value Check** — flags if null% > 5%
- ⊕ **Duplicate Detection** — flags if dup% > 3%
- ⚡ **Schema Drift** — column/type mismatch detection
- ⏱ **SLA / Latency Breach** — flags if latency > 3600ms
- ⚖ **Row Count Reconciliation** — flags > 4% loss between layers
- 📦 **Source → ODS Yield** — end-to-end record yield %
- 📅 **Completeness** — verifies all scheduled runs arrived

---

## 🚀 Quick Start

### Prerequisites
- Python 3.9 or higher
- An Anthropic API key → https://console.anthropic.com

### Step 1 — Clone / Download the project
```bash
# If you have git:
git clone <your-repo-url>
cd pipeline-monitor

# Or just unzip the downloaded folder
cd pipeline-monitor
```

### Step 2 — Set your API key
```bash
cp .env.example .env
# Edit .env and replace  sk-ant-your-key-here  with your real key
```

```env
ANTHROPIC_API_KEY=sk-ant-api03-your-real-key-here
```

### Step 3 — Start the app

**macOS / Linux:**
```bash
chmod +x scripts/start.sh
./scripts/start.sh
```

**Windows:**
```cmd
scripts\start.bat
```

**Manual start (any OS):**
```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Step 4 — Open dashboard
```
http://localhost:8000
```

---

## 📁 Project Structure

```
pipeline-monitor/
│
├── backend/
│   ├── main.py              # FastAPI application + all API routes
│   └── requirements.txt     # Python dependencies
│
├── frontend/
│   └── index.html           # Complete single-file dashboard (HTML/CSS/JS)
│
├── scripts/
│   ├── start.sh             # macOS/Linux launcher
│   └── start.bat            # Windows launcher
│
├── .env.example             # Environment variables template
├── .env                     # Your local config (created from .env.example)
└── README.md                # This file
```

---

## 🔌 REST API Reference

All endpoints are served at `http://localhost:8000`

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/health` | Health check |
| GET | `/api/runs?limit=56` | All pipeline run data |
| GET | `/api/alerts?severity=critical` | Filtered anomalies |
| GET | `/api/metrics/summary` | Dashboard KPIs |
| GET | `/api/metrics/reconciliation` | Cross-layer record loss |
| GET | `/api/metrics/quality_checks` | All 9 quality check results |
| POST | `/api/agent/chat` | AI agent chat endpoint |
| GET | `/docs` | Interactive Swagger API docs |

### Example — Fetch alerts
```bash
curl http://localhost:8000/api/alerts?severity=critical
```

### Example — Chat with AI agent
```bash
curl -X POST http://localhost:8000/api/agent/chat \
  -H "Content-Type: application/json" \
  -d '{"messages": [{"role": "user", "content": "Why did the record spike happen?"}]}'
```

---

## 🔧 Connecting Real Data

The backend (`backend/main.py`) uses simulated data by default. To connect your real pipeline:

### Option A — Replace `generate_pipeline_runs()` with BigQuery
```python
from google.cloud import bigquery

def generate_pipeline_runs(days=14):
    client = bigquery.Client()
    query = """
        SELECT
          run_ts,
          source_count,
          gcs_raw_count,
          gcs_prep_count,
          bq_load_count,
          raw_count,
          hist_count,
          ods_count,
          null_pct,
          dup_pct,
          latency_ms
        FROM `your_project.monitoring.pipeline_runs`
        WHERE DATE(run_ts) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
        ORDER BY run_ts DESC
        LIMIT 56
    """
    rows = client.query(query).result()
    return [dict(row) for row in rows]
```

### Option B — GCS Metadata
```python
from google.cloud import storage

def get_gcs_counts(bucket_name, prefix):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blobs  = list(bucket.list_blobs(prefix=prefix))
    return len(blobs)
```

### Option C — Add to .env for real GCP config
```env
ANTHROPIC_API_KEY=sk-ant-...
GOOGLE_CLOUD_PROJECT=your-project-id
BQ_DATASET=monitoring
BQ_TABLE=pipeline_runs
GCS_BUCKET=your-pipeline-bucket
```

---

## ⚙️ Configuration

Edit `.env` to configure:

```env
ANTHROPIC_API_KEY=sk-ant-api03-...   # Required for AI Agent
PORT=8000                             # Backend port (default: 8000)
ENV=development                       # development | production
```

---

## 🔒 Production Deployment

For production, serve the frontend via a CDN or Nginx and run the backend with:

```bash
# Production start (no reload, multiple workers)
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4

# Or with gunicorn
pip install gunicorn
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

**Nginx config example:**
```nginx
server {
    listen 80;
    server_name your-domain.com;

    location /api/ {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
    }
    location / {
        root /path/to/pipeline-monitor/frontend;
        try_files $uri /index.html;
    }
}
```

---

## 🐛 Troubleshooting

| Problem | Solution |
|---------|----------|
| `Port 8000 already in use` | Change `PORT=8001` in `.env` |
| `API OFFLINE` in dashboard | Make sure backend is running on port 8000 |
| AI Agent returns error | Check `ANTHROPIC_API_KEY` in `.env` |
| CORS errors in browser | Backend CORS is set to `*` — should work on localhost |
| Charts not loading | Check browser console; ensure Chart.js CDN is reachable |

---

## 📄 License

MIT License — free for personal and commercial use.
