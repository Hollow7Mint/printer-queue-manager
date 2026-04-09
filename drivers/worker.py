"""Printer Queue Manager — Printer service layer."""
from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class PrinterWorker:
    """Business-logic service for Printer operations in Printer Queue Manager."""

    def __init__(
        self,
        repo: Any,
        events: Optional[Any] = None,
    ) -> None:
        self._repo   = repo
        self._events = events
        logger.debug("PrinterWorker started")

    def clear(
        self, payload: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the clear workflow for a new Printer."""
        if "submitted_at" not in payload:
            raise ValueError("Missing required field: submitted_at")
        record = self._repo.insert(
            payload["submitted_at"], payload.get("error_code"),
            **{k: v for k, v in payload.items()
              if k not in ("submitted_at", "error_code")}
        )
        if self._events:
            self._events.emit("printer.cleard", record)
        return record

    def resume(self, rec_id: str, **changes: Any) -> Dict[str, Any]:
        """Apply *changes* to a Printer and emit a change event."""
        ok = self._repo.update(rec_id, **changes)
        if not ok:
            raise KeyError(f"Printer {rec_id!r} not found")
        updated = self._repo.fetch(rec_id)
        if self._events:
            self._events.emit("printer.resumed", updated)
        return updated

    def pause(self, rec_id: str) -> None:
        """Remove a Printer and emit a removal event."""
        ok = self._repo.delete(rec_id)
        if not ok:
            raise KeyError(f"Printer {rec_id!r} not found")
        if self._events:
            self._events.emit("printer.paused", {"id": rec_id})

    def search(
        self,
        submitted_at: Optional[Any] = None,
        status: Optional[str] = None,
        limit:  int = 50,
    ) -> List[Dict[str, Any]]:
        """Search printers by *submitted_at* and/or *status*."""
        filters: Dict[str, Any] = {}
        if submitted_at is not None:
            filters["submitted_at"] = submitted_at
        if status is not None:
            filters["status"] = status
        rows, _ = self._repo.query(filters, limit=limit)
        logger.debug("search printers: %d hits", len(rows))
        return rows

    @property
    def stats(self) -> Dict[str, int]:
        """Quick summary of Printer counts by status."""
        result: Dict[str, int] = {}
        for status in ("active", "pending", "closed"):
            _, count = self._repo.query({"status": status}, limit=0)
            result[status] = count
        return result
