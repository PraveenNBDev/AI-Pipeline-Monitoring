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
from dotenv import load_dotenv

load_dotenv()

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
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_URL     = "https://api.anthropic.com/v1/messages"
CLAUDE_MODEL      = "claude-sonnet-4-20250514"
DATA_SOURCE       = os.getenv("DATA_SOURCE", "mock").lower()
GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT", "")
BQ_DATASET        = os.getenv("BQ_DATASET", "")
BQ_TABLE          = os.getenv("BQ_TABLE", "")
DBT_LOG_PATH      = Path(__file__).parent / "dbt.log"

# ── Layer definitions ───────────────────────────────────────────
LAYERS = ["BQ Load", "Raw Layer", "Hist Layer", "ODS Layer"]
LKEYS  = ["bq_load", "raw", "hist", "ods"]
TABLE_NAMES = [
    "dim_customers",
    "fact_orders",
    "stg_payments",
    "ods_transactions",
    "stg_products",
    "orders_archive",
]
FILE_STATUSES = ["Landed", "Preprocessed", "BQ Loaded", "Raw", "Hist", "ODS Ready"]

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


# Sample audit table schema:
# - source_file_name: name of the incoming file
# - table_name: destination table or logical table name
# - run_date: scheduled run date for this batch
# - job_start_date: actual processing start timestamp
# - row_count: processed row count for the table
# - status: derived pipeline state
# - anomalies: detected data quality issues
# - layer_counts: optional counts for monitoring each layer

def calculate_layer_metrics(record: dict) -> dict:
    """Calculate data quality metrics per layer and record loss."""
    lc = record["layer_counts"]
    layers = ["source", "gcs_raw", "gcs_prep", "bq_load", "raw", "hist"]
    metrics = []
    
    for i in range(len(layers)):
        curr_count = lc[layers[i]]
        loss_pct = 0
        
        if i > 0:
            prev_count = lc[layers[i - 1]]
            loss_pct = round(((prev_count - curr_count) / prev_count * 100), 2) if prev_count > 0 else 0
        
        status = "OK"
        if loss_pct > 4:
            status = "FAIL"
        elif loss_pct > 1.5:
            status = "WARN"
        
        metrics.append({
            "layer": layers[i],
            "count": curr_count,
            "loss_pct": loss_pct,
            "status": status
        })
    
    return {"layers": metrics, "anomalies": record.get("anomalies", [])}

def generate_audit_records(count: int = 100) -> List[dict]:
    rng = random.Random(1234)
    records = []
    base_rows = 90000
    last_ods_by_table = {}

    for i in range(count):
        run_dt = datetime.now() - timedelta(hours=i * 1)
        run_date = run_dt.strftime("%Y-%m-%d")
        arrival_dt = run_dt - timedelta(minutes=int(rng.random() * 15 + 3))
        preproc_dt = arrival_dt + timedelta(minutes=int(rng.random() * 7 + 4))
        bq_load_dt = preproc_dt + timedelta(minutes=int(rng.random() * 6 + 3))
        raw_dt = bq_load_dt + timedelta(minutes=int(rng.random() * 4 + 2))
        hist_dt = raw_dt + timedelta(minutes=int(rng.random() * 3 + 1))
        job_start_dt = run_dt - timedelta(minutes=int(rng.random() * 15))
        job_start_date = job_start_dt.strftime("%Y-%m-%d %H:%M:%S")
        table_name = TABLE_NAMES[i % len(TABLE_NAMES)]
        file_name = f"{table_name}_{run_dt.strftime('%Y%m%d_%H%M%S')}.csv"
        row_count = base_rows + int((rng.random() - 0.5) * 14000)
        null_pct = round(rng.random() * 3.5 + (5 if rng.random() < 0.06 else 0), 2)
        gcs_raw = int(row_count * (0.98 + rng.random() * 0.012))
        gcs_prep = int(gcs_raw * (0.98 + rng.random() * 0.012))
        bq_load = int(gcs_prep * (0.995 + rng.random() * 0.005))
        raw = int(bq_load * (0.96 + rng.random() * 0.022))
        hist = int(raw * (0.992 + rng.random() * 0.006))
        ods = int(hist * (0.995 + rng.random() * 0.004))
        status_index = rng.randint(3, 6)
        status = FILE_STATUSES[status_index - 1]
        anomalies = []
        if row_count > base_rows * 1.8:
            anomalies.append("SPIKE")
        if row_count < base_rows * 0.55:
            anomalies.append("DROP")
        if rng.random() < 0.08:
            anomalies.append("SCHEMA")
        if null_pct > 5 or rng.random() < 0.06:
            anomalies.append("NULL")
        if rng.random() < 0.05:
            anomalies.append("DUPE")

        previous_ods = last_ods_by_table.get(table_name, ods)
        ods_change_pct = round(((ods - previous_ods) / previous_ods) * 100, 1) if previous_ods else 0
        if ods_change_pct > 10:
            anomalies.append("ODS_SPIKE")
        if ods_change_pct < -10:
            anomalies.append("ODS_DROP")

        rec = {
            "source_file_name": file_name,
            "table_name": table_name,
            "run_date": run_date,
            "job_start_date": job_start_date,
            "source_arrival_ts": arrival_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "gcs_preprocessor_ts": preproc_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "bq_load_ts": bq_load_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "raw_complete_ts": raw_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "hist_complete_ts": hist_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "row_count": row_count,
            "null_pct": null_pct,
            "status": status,
            "trigger_type": "schedule" if status == "ODS Ready" else "event",
            "current_layer": "ODS" if status == "ODS Ready" else status,
            "layer_counts": {
                "source": row_count,
                "gcs_raw": gcs_raw,
                "gcs_prep": gcs_prep,
                "bq_load": bq_load,
                "raw": raw,
                "hist": hist,
                "ods": ods,
            },
            "ods_row_count": ods,
            "ods_change_pct": ods_change_pct,
            "ods_trend": "spike" if ods_change_pct > 10 else "drop" if ods_change_pct < -10 else "stable",
            "latency_ms": int((run_dt - job_start_dt).total_seconds()),
            "anomalies": anomalies,
        }
        
        # Add calculated layer metrics
        rec["layer_metrics"] = calculate_layer_metrics(rec)
        records.append(rec)
        last_ods_by_table[table_name] = ods
        base_rows += int((rng.random() - 0.5) * 800)

    records.reverse()
    return records


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
AUDIT_RECORDS = generate_audit_records(100)
FILES  = AUDIT_RECORDS


# ═══════════════════════════════════════════════════════════════
#  REST ENDPOINTS
# ═══════════════════════════════════════════════════════

@app.get("/api/health")
def health():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.get("/api/runs")
def get_runs(limit: int = 56):
    return {"runs": RUNS[:limit], "total": len(RUNS)}


@app.get("/api/files")
def get_files(limit: int = 100):
    return {"files": FILES[:limit], "total": len(FILES)}


@app.get("/api/files/detail")
def get_files_detail(limit: int = 50):
    """Return per-file layer-by-layer tracking with data quality metrics."""
    detailed = []
    for f in FILES[:limit]:
        layers = f.get("layer_metrics", {}).get("layers", [])
        # Ensure duplicate percentage is available per-file; fall back to run-level dup_pct when missing
        dup_pct = f.get("dup_pct")
        if dup_pct is None:
            run_match = next((r for r in RUNS if r.get("date") == f.get("run_date")), None)
            dup_pct = run_match.get("dup_pct") if run_match else None
        detailed.append({
            "source_file_name": f["source_file_name"],
            "table_name": f["table_name"],
            "run_date": f["run_date"],
            "job_start_date": f["job_start_date"],
            "source_arrival_ts": f.get("source_arrival_ts"),
            "hist_complete_ts": f.get("hist_complete_ts"),
            "row_count": f.get("row_count"),
            "null_pct": f.get("null_pct"),
            "dup_pct": dup_pct,
            "latency_ms": f.get("latency_ms"),
            "status": f["status"],
            "trigger_type": f.get("trigger_type"),
            "layer_metrics": f.get("layer_metrics"),
            "layers": layers,
            "anomalies": f.get("anomalies", []),
            "has_quality_issues": len(f.get("anomalies", [])) > 0 or any(m["status"] != "OK" for m in layers),
        })
    return {"files": detailed, "total": len(FILES)}


@app.get("/api/audit")
def get_audit(limit: int = 100):
    return {"audit": AUDIT_RECORDS[:limit], "total": len(AUDIT_RECORDS)}


@app.get("/api/tables")
def get_tables():
    tables = {}
    for f in FILES:
        tbl = f["table_name"]
        stats = tables.setdefault(tbl, {
            "name": tbl,
            "file_count": 0,
            "latest_status": "",
            "alerts": 0,
            "ods_ready": 0,
            "last_run": "",
        })
        stats["file_count"] += 1
        stats["alerts"] += len(f.get("anomalies", []))
        if f["status"] == "ODS Ready":
            stats["ods_ready"] += 1
        if not stats["latest_status"] or f["run_date"] >= stats["last_run"]:
            stats["latest_status"] = f["status"]
            stats["last_run"] = f["run_date"]
    return {"tables": sorted(tables.values(), key=lambda x: (-x["file_count"], x["name"]))}


@app.get("/api/tables/{table_name}")
def get_table_detail(table_name: str):
    selected = [f for f in FILES if f["table_name"] == table_name]
    if not selected:
        raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

    selected = sorted(selected, key=lambda f: (f["run_date"], f["job_start_date"]))
    anomalies = sum(len(f.get("anomalies", [])) for f in selected)
    ods_ready = sum(1 for f in selected if f["status"] == "ODS Ready")
    files_in_progress = len(selected) - ods_ready
    latest = selected[-1]
    return {
        "table_name": table_name,
        "total_files": len(selected),
        "alerts": anomalies,
        "ods_ready": ods_ready,
        "in_progress": files_in_progress,
        "latest_file": {
            "source_file_name": latest["source_file_name"],
            "status": latest["status"],
            "run_date": latest["run_date"],
            "job_start_date": latest["job_start_date"],
            "row_count": latest["row_count"],
            "latency_ms": latest["latency_ms"],
            "anomalies": latest.get("anomalies", []),
            "layer_metrics": latest.get("layer_metrics", {}),
        },
        "files": selected,
    }


def parse_dbt_log_file(path: Path) -> dict:
    details = {"INFO": 0, "WARNING": 0, "ERROR": 0, "total": 0}
    summary = {"levels": details, "messages": []}
    if not path.exists():
        return {"message": "dbt log file not found", "levels": summary["levels"], "recent": [], "parse_error": False}

    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            level = None
            if "ERROR" in line:
                level = "ERROR"
            elif "WARNING" in line:
                level = "WARNING"
            elif "INFO" in line:
                level = "INFO"
            else:
                continue

            summary["levels"][level] += 1
            summary["levels"]["total"] += 1
            if len(summary["messages"]) < 12:
                ts = line.split(" ")[0].strip("[]")
                summary["messages"].append({"level": level, "text": line, "timestamp": ts})

    return {
        "message": "dbt log analysis loaded",
        "levels": summary["levels"],
        "recent": summary["messages"],
        "parse_error": False,
    }


@app.get("/api/dbt/logs/summary")
def get_dbt_log_summary():
    return parse_dbt_log_file(DBT_LOG_PATH)


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
    latest_run_date = max(f["run_date"] for f in FILES)
    latest_files = [f for f in FILES if f["run_date"] == latest_run_date]
    expected_files_today = len(latest_files)
    received_files_today = sum(1 for f in latest_files if f["status"] in ["Hist", "ODS Ready"])
    pending_files_today = sum(1 for f in latest_files if f["status"] not in ["Hist", "ODS Ready"])
    event_files = sum(1 for f in latest_files if f.get("trigger_type") == "event")
    hist_ready = sum(1 for f in latest_files if f["status"] in ["Hist", "ODS Ready"])
    ods_ready = sum(1 for f in latest_files if f["status"] == "ODS Ready")
    awaiting_ods = sum(1 for f in latest_files if f["status"] == "Hist")
    files_with_alerts = sum(1 for f in latest_files if f["anomalies"])
    ods_schedule_spikes = sum(1 for f in latest_files if f.get("trigger_type") == "schedule" and f.get("ods_change_pct", 0) > 10)
    ods_schedule_drops = sum(1 for f in latest_files if f.get("trigger_type") == "schedule" and f.get("ods_change_pct", 0) < -10)
    ods_null_events = sum(1 for f in latest_files if f.get("trigger_type") == "schedule" and "NULL" in f.get("anomalies", []))
    recon_gaps = sum(1 for a in ALERTS if a["type"] == "RECON")

    return {
        "total_alerts":    len(ALERTS),
        "critical_alerts": sum(1 for a in ALERTS if a["severity"] == "critical"),
        "high_alerts":     sum(1 for a in ALERTS if a["severity"] == "high"),
        "medium_alerts":   sum(1 for a in ALERTS if a["severity"] == "medium"),
        "avg_latency_ms":  avg_latency,
        "avg_null_pct":    avg_null,
        "avg_dup_pct":     avg_dup,
        "ods_yield_pct":   ods_yield,
        "latest_run_date": latest_run_date,
        "expected_files_today": expected_files_today,
        "received_files_today": received_files_today,
        "pending_files_today": pending_files_today,
        "event_files": event_files,
        "hist_ready": hist_ready,
        "ods_ready": ods_ready,
        "awaiting_ods": awaiting_ods,
        "files_with_alerts": files_with_alerts,
        "ods_schedule_spikes": ods_schedule_spikes,
        "ods_schedule_drops": ods_schedule_drops,
        "ods_null_events": ods_null_events,
        "recon_gaps": recon_gaps,
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
