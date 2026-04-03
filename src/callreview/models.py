from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class CallRecord:
    id: int
    uuid: str
    system: str
    filename: str
    source_path: str
    current_path: str
    archive_path: Optional[str]
    file_hash: Optional[str]
    file_size: int
    modified_ts: float
    recorded_at: Optional[str]
    discovered_at: str
    status: str
    transcript_status: str
    transcript_text: Optional[str]
    summary_text: Optional[str]
    tags_csv: Optional[str]
    priority_score: int
    review_status: str
    error_message: Optional[str]
    created_at: str
    updated_at: str