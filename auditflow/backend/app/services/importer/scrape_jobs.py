from __future__ import annotations

import threading
import uuid
from typing import Any, Dict, Optional

_lock = threading.Lock()
_jobs: Dict[str, Dict[str, Any]] = {}


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
    return jid


def update_job(job_id: str, progress: int, message: str = "") -> None:
    with _lock:
        if job_id not in _jobs:
            return
        cur = int(_jobs[job_id].get("progress") or 0)
        nxt = max(cur, min(99, int(progress)))
        _jobs[job_id]["progress"] = nxt
        if message:
            _jobs[job_id]["message"] = message


def complete_job(job_id: str, result: Dict[str, Any]) -> None:
    with _lock:
        if job_id not in _jobs:
            return
        _jobs[job_id]["status"] = "done"
        _jobs[job_id]["progress"] = 100
        _jobs[job_id]["message"] = "تم"
        _jobs[job_id]["result"] = result


def fail_job(job_id: str, error: str) -> None:
    with _lock:
        if job_id not in _jobs:
            return
        _jobs[job_id]["status"] = "error"
        _jobs[job_id]["error"] = error
        _jobs[job_id]["message"] = error


def get_job(job_id: str) -> Optional[Dict[str, Any]]:
    with _lock:
        j = _jobs.get(job_id)
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
