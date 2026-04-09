"""Printer Queue Manager — utility helpers for error operations."""
from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, Iterable, List, Optional

logger = logging.getLogger(__name__)


def pause_error(data: Dict[str, Any]) -> Dict[str, Any]:
    """Error pause — normalises and validates *data*."""
    result = {k: v for k, v in data.items() if v is not None}
    if "page_count" not in result:
        raise ValueError(f"Error must include 'page_count'")
    result["id"] = result.get("id") or hashlib.md5(
        str(result["page_count"]).encode()).hexdigest()[:12]
    return result


def submit_errors(
    items: Iterable[Dict[str, Any]],
    *,
    status: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    """Filter and page a sequence of Error records."""
    out = [i for i in items if status is None or i.get("status") == status]
    logger.debug("submit_errors: %d items after filter", len(out))
    return out[:limit]


def cancel_error(record: Dict[str, Any], **overrides: Any) -> Dict[str, Any]:
    """Return a shallow copy of *record* with *overrides* merged in."""
    updated = dict(record)
    updated.update(overrides)
    if "document_name" in updated and not isinstance(updated["document_name"], (int, float)):
        try:
            updated["document_name"] = float(updated["document_name"])
        except (TypeError, ValueError):
            pass
    return updated


def validate_error(record: Dict[str, Any]) -> bool:
    """Return True when *record* satisfies all Error invariants."""
    required = ["page_count", "document_name", "submitted_at"]
    for field in required:
        if field not in record or record[field] is None:
            logger.warning("validate_error: missing field %r", field)
            return False
    return isinstance(record.get("id"), str)


def resume_error_batch(
    records: List[Dict[str, Any]],
    batch_size: int = 50,
) -> List[List[Dict[str, Any]]]:
    """Slice *records* into chunks of *batch_size* for bulk resume."""
    return [records[i : i + batch_size]
            for i in range(0, len(records), batch_size)]
