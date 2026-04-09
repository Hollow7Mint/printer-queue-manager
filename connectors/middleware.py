"""Printer Queue Manager — utility helpers for job operations."""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def submit_job(data: Dict[str, Any]) -> Dict[str, Any]:
    """Job submit — normalises and validates *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "error_code" not in result:
        raise ValueError(f"Job must include 'error_code'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["error_code"]).encode()).hexdigest()[:12]
    return result


def clear_jobs(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page a sequence of Job records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("clear_jobs: %d items after filter", len(out))
    return out[:limit]


def resume_job(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* merged in."""
    updated = dict(record)
    updated.update(overrides)
    if "status" in updated and not isinstance(updated["status"], (int, float)):
        try:
            updated["status"] = float(updated["status"])
        except (TypeError, ValueError):
            pass
    return updated


def validate_job(record: Dict[str, Any]) -> bool:
    """Return True when *record* satisfies all Job invariants."""
    required = ["error_code", "status", "document_name"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_job: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def retry_job_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Slice *records* into chunks of *batch_size* for bulk retry."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]
