"""Printer Queue Manager — Queue service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PrinterRepository:
    """Business-logic service for Queue operations in Printer Queue Manager."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("PrinterRepository started")

    def retry(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the retry workflow for a new Queue."""
        if "document_name" not in payload:
            raise ValueError("Missing required field: document_name")
        record = self._repo.insert(
            payload["document_name"], payload.get("printer_id"),
            **{k: v for k, v in payload.items()
              if k not in ("document_name", "printer_id")}
        )
        if self._events:
            self._events.emit("queue.retryd", record)
        return record

    def resume(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Queue and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Queue {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("queue.resumed", updated)
        return updated

    def submit(self, rec_id: str) -> None:
        """Remove a Queue and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Queue {rec_id!r} not found")
        if self._events:
            self._events.emit("queue.submitd", {"id": rec_id})

    def search(
        self,
        document_name: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search queues by *document_name* and/or *status*."""
        filters: Dict[str, Any] = {}
        if document_name is not None:
            filters["document_name"] = document_name
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search queues: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Queue counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
