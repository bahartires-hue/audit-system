from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from pathlib import Path
from typing import Any, Dict, Optional

log = logging.getLogger("importer.scrape_jobs")

_lock = threading.Lock()
_jobs: Dict[str, Dict[str, Any]] = {}


def _jobs_dir() -> Optional[Path]:
    root = (os.getenv("AUDITFLOW_DATA_ROOT") or "").strip()
    if not root:
        return None
    d = Path(root) / "scrape_jobs"
    try:
        d.mkdir(parents=True, exist_ok=True)
    except OSError:
        return None
    return d


def _persist_job(job_id: str) -> None:
    d = _jobs_dir()
    if not d:
        return
    with _lock:
        payload = dict(_jobs.get(job_id) or {})
    try:
        (d / f"{job_id}.json").write_text(
            json.dumps(payload, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
    except Exception as e:
        log.warning("job_persist_failed job_id=%s err=%s", job_id, e)


def _load_job_from_disk(job_id: str) -> Optional[Dict[str, Any]]:
    d = _jobs_dir()
    if not d:
        return None
    path = d / f"{job_id}.json"
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def create_job() -> str:
    jid = uuid.uuid4().hex
    with _lock:
        _jobs[jid] = {
            "status": "running",
            "progress": 1,
            "message": "بدء السحب...",
            "result": None,
            "error": None,
        }
    _persist_job(jid)
    return jid


def update_job(job_id: str, progress: int, message: str = "") -> None:
    with _lock:
        if job_id not in _jobs:
            stored = _load_job_from_disk(job_id)
            if stored:
                _jobs[job_id] = stored
            else:
                return
        cur = int(_jobs[job_id].get("progress") or 0)
        nxt = max(cur, min(99, int(progress)))
        _jobs[job_id]["progress"] = nxt
        if message:
            _jobs[job_id]["message"] = message
    _persist_job(job_id)


def complete_job(job_id: str, result: Dict[str, Any]) -> None:
    with _lock:
        if job_id not in _jobs:
            stored = _load_job_from_disk(job_id)
            if stored:
                _jobs[job_id] = stored
            else:
                return
        _jobs[job_id]["status"] = "done"
        _jobs[job_id]["progress"] = 100
        _jobs[job_id]["message"] = "تم"
        _jobs[job_id]["result"] = result
    _persist_job(job_id)


def fail_job(job_id: str, error: str) -> None:
    with _lock:
        if job_id not in _jobs:
            stored = _load_job_from_disk(job_id)
            if stored:
                _jobs[job_id] = stored
            else:
                return
        _jobs[job_id]["status"] = "error"
        _jobs[job_id]["error"] = error
        _jobs[job_id]["message"] = error
    _persist_job(job_id)


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with _lock:
        j = _jobs.get(job_id)
        if not j:
            j = _load_job_from_disk(job_id)
            if j:
                _jobs[job_id] = j
        if not j:
            return None
        return {
            "status": j.get("status"),
            "progress": j.get("progress"),
            "message": j.get("message"),
            "done": j.get("status") in {"done", "error"},
            "error": j.get("error"),
            "result": j.get("result") if j.get("status") == "done" else None,
        }
