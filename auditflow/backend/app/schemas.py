from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class AnalyzeResponse(BaseModel):
    reportId: str


class ReportStats(BaseModel):
    total_ops: int
    matched_ops: int
    mismatch_ops: int
    errors_count: int
    warnings_count: int


class ReportListItem(BaseModel):
    id: str
    title: Optional[str] = None
    branch1_name: str
    branch2_name: str
    status: str
    created_at: dt.datetime
    stats: ReportStats


class ReportDetail(BaseModel):
    id: str
    title: Optional[str] = None
    branch1_name: str
    branch2_name: str
    status: str
    created_at: dt.datetime
    stats: ReportStats

    file1_original: Optional[str] = None
    file2_original: Optional[str] = None

    stats_json: Dict[str, Any] = Field(default_factory=dict)
    analysis_json: Dict[str, Any] = Field(default_factory=dict)


class AnalyzeFormResponse(BaseModel):
    message: str


class AnalyzeErrorResponse(BaseModel):
    detail: str

