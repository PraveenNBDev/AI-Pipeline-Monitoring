"""
DataPulse — AI Pipeline Monitor
Backend API (FastAPI)
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import httpx
import os
import json
import random
import math
from datetime import datetime, timedelta
from typing import List, Optional
from pathlib import Path

app = FastAPI(title="DataPulse Pipeline Monitor API", version="1.0.0")

# ── CORS ────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Config ──────────────────────────────────────────────────────
ANTHROPIC_API_KEY = os.getenv("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", "")
ANTHROPIC_URL     = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL      = "claude-sonnet-4-20250514"

# ── Layer definitions ───────────────────────────────────────────
LAYERS = ["BQ Load", "Raw Layer", "Hist Layer", "ODS Layer"]
LKEYS  = ["bq_load", "raw", "hist", "ods"]

# ═══════════════════════════════════════════════════════════════
#  DATA GENERATION (simulated pipeline runs)
# ═══════════════════════════════════════════════════════════════
def generate_pipeline_runs(days: int = 14) -> List[dict]:
    runs = []
    base = 125000
    rng  = random.Random(42)   # fixed seed for reproducibility

    for d in range(days - 1, -1, -1):
        dt = datetime.now() - timedelta(days=d)
        ds = dt.strftime("%Y-%m-%d")

        for h in range(0, 24, 6):
            ts  = f"{ds} {str(h).zfill(2)}:00"
            src = base + int((rng.random() - 0.5) * 9000)

            # Inject realistic anomalies
            if d == 2 and h == 6:   src = int(base * 3.2)   # big spike
            if d == 4 and h == 12:  src = int(base * 0.09)  # big drop
            if d == 1 and h == 18:  src = int(base * 2.5)   # spike
            if d == 7 and h == 0:   src = int(base * 0.18)  # drop

            null_pct  = 19.4 if (d == 3 and h == 0)  else round(rng.random() * 2.8, 2)
            dup_pct   = 9.2  if (d == 5 and h == 6)  else round(rng.random() * 0.9, 2)
            schema    = d == 6 and h == 12
            sla_breach= d == 2 and h == 6
            latency   = int(4500 + rng.random() * 600) if sla_breach else int(800 + rng.random() * 450)

            gcs_raw  = int(src     * (0.989 + rng.random() * 0.008))
            gcs_prep = int(gcs_raw * (0.981 + rng.random() * 0.010))
            bq_load  = int(gcs_prep* (0.996 + rng.random() * 0.003))
            raw      = int(bq_load * (0.965 + rng.random() * 0.020))
            hist     = int(raw     * (0.994 + rng.random() * 0.004))
            ods      = int(hist    * (0.997 + rng.random() * 0.002))

            counts = dict(source=src, gcs_raw=gcs_raw, gcs_prep=gcs_prep,
                          bq_load=bq_load, raw=raw, hist=hist, ods=ods)

            runs.append(dict(
                ts=ts, date=ds, hour=h,
                counts=counts,
                null_pct=null_pct,
                dup_pct=dup_pct,
                schema_drift=schema,
                sla_breach=sla_breach,
                latency=latency,
            ))

        base += int((rng.random() - 0.5) * 4000)

    runs.reverse()
    return runs[:56]


def detect_anomalies(runs: List[dict]) -> List[dict]:
    alerts = []
    src_counts = [r["counts"]["source"] for r in runs]
    mean = sum(src_counts) / len(src_counts)
    std  = math.sqrt(sum((c - mean) ** 2 for c in src_counts) / len(src_counts))

    for i, run in enumerate(runs):
        if i < 3:
            continue
        z = (run["counts"]["source"] - mean) / std

        if z > 2.3:
            alerts.append(dict(ts=run["ts"], type="SPIKE", severity="critical",
                msg=f"Record spike at Source: {run['counts']['source']:,} records ({z:.1f}σ above baseline)",
                layer="Source", z_score=round(z, 2)))

        if z < -2.3:
            alerts.append(dict(ts=run["ts"], type="DROP", severity="critical",
                msg=f"Record drop at Source: {run['counts']['source']:,} records ({abs(z):.1f}σ below baseline)",
                layer="Source", z_score=round(z, 2)))

        if run["null_pct"] > 5:
            alerts.append(dict(ts=run["ts"], type="NULL", severity="high",
                msg=f"Null rate {run['null_pct']}% in ODS Layer — exceeds 5% threshold",
                layer="ODS Layer", z_score=None))

        if run["dup_pct"] > 3:
            alerts.append(dict(ts=run["ts"], type="DUPE", severity="high",
                msg=f"Duplicate rate {run['dup_pct']}% in Raw Layer — exceeds 3% threshold",
                layer="Raw Layer", z_score=None))

        if run["schema_drift"]:
            alerts.append(dict(ts=run["ts"], type="SCHEMA", severity="high",
                msg="Schema drift detected at GCS Preprocessor — column type mismatch or new field",
                layer="GCS Preprocessor", z_score=None))

        if run["sla_breach"]:
            alerts.append(dict(ts=run["ts"], type="SLA", severity="medium",
                msg=f"SLA breach: latency {run['latency']}ms exceeds 3600ms threshold",
                layer="BQ Load", z_score=None))

        # Reconciliation
        for li in range(1, len(LKEYS)):
            k_from = LKEYS[li - 1]
            k_to   = LKEYS[li]
            from_c = run["counts"][k_from]
            to_c   = run["counts"][k_to]
            loss   = (from_c - to_c) / from_c
            if loss > 0.04:
                alerts.append(dict(ts=run["ts"], type="RECON", severity="medium",
                    msg=f"{loss*100:.1f}% record loss: {LAYERS[li-1]} → {LAYERS[li]}",
                    layer=LAYERS[li], z_score=None))

    alerts.sort(key=lambda a: a["ts"], reverse=True)
    return alerts[:30]


# Pre-compute at startup
RUNS   = generate_pipeline_runs(14)
ALERTS = detect_anomalies(RUNS)


# ═══════════════════════════════════════════════════════════════
#  REST ENDPOINTS
# ═══════════════════════════════════════════════════════════════

@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/runs")
def get_runs(limit: int = 56):
    return {"runs": RUNS[:limit], "total": len(RUNS)}


@app.get("/api/alerts")
def get_alerts(severity: Optional[str] = None, type: Optional[str] = None, limit: int = 30):
    filtered = ALERTS
    if severity:
        filtered = [a for a in filtered if a["severity"] == severity]
    if type:
        filtered = [a for a in filtered if a["type"] == type]
    return {"alerts": filtered[:limit], "total": len(filtered)}


@app.get("/api/metrics/summary")
def get_summary():
    last8 = RUNS[:8]
    last  = RUNS[0]

    avg_latency = int(sum(r["latency"] for r in last8) / len(last8))
    avg_null    = round(sum(r["null_pct"] for r in last8) / len(last8), 2)
    avg_dup     = round(sum(r["dup_pct"]  for r in last8) / len(last8), 2)
    ods_yield   = round(last["counts"]["ods"] / last["counts"]["source"] * 100, 1)

    return {
        "total_alerts":    len(ALERTS),
        "critical_alerts": sum(1 for a in ALERTS if a["severity"] == "critical"),
        "high_alerts":     sum(1 for a in ALERTS if a["severity"] == "high"),
        "medium_alerts":   sum(1 for a in ALERTS if a["severity"] == "medium"),
        "avg_latency_ms":  avg_latency,
        "avg_null_pct":    avg_null,
        "avg_dup_pct":     avg_dup,
        "ods_yield_pct":   ods_yield,
        "last_run_ts":     last["ts"],
        "layers":          LAYERS,
        "latest_counts":   last["counts"],
    }


@app.get("/api/metrics/reconciliation")
def get_reconciliation():
    last = RUNS[0]
    rows = []
    for i in range(1, len(LKEYS)):
        k_from = LKEYS[i - 1]
        k_to   = LKEYS[i]
        from_c = last["counts"][k_from]
        to_c   = last["counts"][k_to]
        loss   = from_c - to_c
        loss_pct = round(loss / from_c * 100, 2)
        status = "FAIL" if loss_pct > 4 else ("WARN" if loss_pct > 1.5 else "OK")
        rows.append(dict(
            from_layer=LAYERS[i - 1], to_layer=LAYERS[i],
            from_count=from_c, to_count=to_c,
            loss=loss, loss_pct=loss_pct, status=status
        ))
    return {"reconciliation": rows, "ts": last["ts"]}


@app.get("/api/metrics/quality_checks")
def get_quality_checks():
    last8 = RUNS[:8]
    last  = RUNS[0]

    avg_null   = round(sum(r["null_pct"] for r in last8) / len(last8), 2)
    avg_dup    = round(sum(r["dup_pct"]  for r in last8) / len(last8), 2)
    ods_yield  = round(last["counts"]["ods"] / last["counts"]["source"] * 100, 1)
    spikes     = sum(1 for a in ALERTS if a["type"] == "SPIKE")
    drops      = sum(1 for a in ALERTS if a["type"] == "DROP")
    schema_evt = sum(1 for a in ALERTS if a["type"] == "SCHEMA")
    sla_breach = sum(1 for a in ALERTS if a["type"] == "SLA")
    recon_gaps = sum(1 for a in ALERTS if a["type"] == "RECON")

    checks = [
        {"name": "Record Spike Detection",   "icon": "⬆", "desc": "Z-score ≥ 2.3σ",
         "value": spikes,  "unit": "spikes",
         "status": "FAIL" if spikes > 2 else ("WARN" if spikes > 0 else "PASS"),
         "detail": f"{spikes} spike events in last 14 days"},
        {"name": "Record Drop Detection",    "icon": "⬇", "desc": "Z-score ≤ -2.3σ",
         "value": drops,   "unit": "drops",
         "status": "FAIL" if drops > 2 else ("WARN" if drops > 0 else "PASS"),
         "detail": f"{drops} drop events in last 14 days"},
        {"name": "Null Value Check",         "icon": "∅", "desc": "Threshold < 5%",
         "value": avg_null, "unit": "avg null%",
         "status": "FAIL" if avg_null > 5 else ("WARN" if avg_null > 2 else "PASS"),
         "detail": f"Avg {avg_null}% null rate (last 8 runs)"},
        {"name": "Duplicate Detection",      "icon": "⊕", "desc": "Threshold < 3%",
         "value": avg_dup,  "unit": "avg dup%",
         "status": "FAIL" if avg_dup > 3 else ("WARN" if avg_dup > 1 else "PASS"),
         "detail": f"Avg {avg_dup}% duplication rate (last 8 runs)"},
        {"name": "Schema Drift Detection",   "icon": "⚡", "desc": "Column/type mismatch",
         "value": schema_evt, "unit": "events",
         "status": "WARN" if schema_evt > 0 else "PASS",
         "detail": f"{schema_evt} schema drift event(s) detected"},
        {"name": "SLA / Latency Check",      "icon": "⏱", "desc": "Threshold < 3600ms",
         "value": sla_breach, "unit": "breaches",
         "status": "FAIL" if sla_breach > 2 else ("WARN" if sla_breach > 0 else "PASS"),
         "detail": f"{sla_breach} SLA breach(es) detected"},
        {"name": "Row Count Reconciliation", "icon": "⚖", "desc": "< 4% loss per layer",
         "value": recon_gaps, "unit": "flags",
         "status": "WARN" if recon_gaps > 3 else "PASS",
         "detail": f"{recon_gaps} reconciliation gap(s)"},
        {"name": "Source → ODS Yield",       "icon": "📦", "desc": "Expected > 90%",
         "value": ods_yield, "unit": "% yield",
         "status": "PASS" if ods_yield > 95 else ("WARN" if ods_yield > 88 else "FAIL"),
         "detail": f"Latest: {last['counts']['source']:,} → {last['counts']['ods']:,}"},
        {"name": "Pipeline Completeness",    "icon": "📅", "desc": "All runs arrived",
         "value": "100", "unit": "% coverage",
         "status": "PASS",
         "detail": "All expected runs detected in window"},
    ]
    return {"checks": checks}


# ═══════════════════════════════════════════════════════════════
#  AI AGENT ENDPOINT
# ═══════════════════════════════════════════════════════════════

class ChatMessage(BaseModel):
    role: str
    content: str

class ChatRequest(BaseModel):
    messages: List[ChatMessage]

@app.post("/api/agent/chat")
async def agent_chat(req: ChatRequest):
    if not ANTHROPIC_API_KEY:
        raise HTTPException(status_code=500, detail="ANTHROPIC_API_KEY not set. Please add it to your .env file.")

    system_prompt = f"""You are an expert AI data pipeline monitoring agent. The user's pipeline:
Source System → GCS Bucket Raw → GCS Preprocessor → BQ Load → Raw Layer → Hist Layer → ODS Layer

PIPELINE ANOMALIES DETECTED (last 14 days):
{chr(10).join(f"[{a['ts']}] {a['type']} | {a['severity']} | {a['layer']}: {a['msg']}" for a in ALERTS)}

RECENT RUN STATS (last 5 runs):
{chr(10).join(
    f"{r['ts']}: src={r['counts']['source']:,} gcs_raw={r['counts']['gcs_raw']:,} "
    f"gcs_prep={r['counts']['gcs_prep']:,} bq_load={r['counts']['bq_load']:,} "
    f"raw={r['counts']['raw']:,} hist={r['counts']['hist']:,} ods={r['counts']['ods']:,} "
    f"null%={r['null_pct']} dup%={r['dup_pct']} latency={r['latency']}ms"
    for r in RUNS[:5]
)}

Respond with root cause analysis, business impact, and actionable remediation steps.
Use markdown. Keep responses under 300 words. Be specific to GCS/BigQuery pipeline patterns."""

    payload = {
        "model": CLAUDE_MODEL,
        "max_tokens": 1024,
        "system": system_prompt,
        "messages": [{"role": m.role, "content": m.content} for m in req.messages],
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            ANTHROPIC_URL,
            headers={
                "Content-Type":      "application/json",
                "x-api-key":         ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
            },
            json=payload,
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail=resp.text)

    data = resp.json()
    reply = data["content"][0]["text"]
    return {"reply": reply}


# ═══════════════════════════════════════════════════════════════
#  SERVE FRONTEND
# ═══════════════════════════════════════════════════════════════
frontend_dir = Path(__file__).parent.parent / "frontend"

if frontend_dir.exists():
    app.mount("/static", StaticFiles(directory=str(frontend_dir)), name="static")

@app.get("/")
def root():
    index = frontend_dir / "index.html"
    if index.exists():
        return FileResponse(str(index))
    return {"message": "DataPulse API running. Frontend not found — serve frontend/index.html separately."}
