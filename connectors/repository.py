"""Printer Queue Manager — Job repository layer."""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

logger = logging.getLogger(__name__)


class PrinterRepository:
    """Job repository for the Printer Queue Manager application."""

    def __init__(
        self,
        store: Any,
        config: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._store = store
        self._cfg   = config or {}
        self._printer_id = self._cfg.get("printer_id", None)
        logger.debug("%s initialised", self.__class__.__name__)

    def pause_job(
        self, printer_id: Any, page_count: Any, **extra: Any
    ) -> Dict[str, Any]:
        """Create and persist a new Job record."""
        now = datetime.now(timezone.utc).isoformat()
        record: Dict[str, Any] = {
            "id":         str(uuid.uuid4()),
            "printer_id": printer_id,
            "page_count": page_count,
            "status":     "active",
            "created_at": now,
            **extra,
        }
        saved = self._store.put(record)
        logger.info("pause_job: created %s", saved["id"])
        return saved

    def get_job(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a Job by its *record_id*."""
        record = self._store.get(record_id)
        if record is None:
            logger.debug("get_job: %s not found", record_id)
        return record

    def cancel_job(
        self, record_id: str, **changes: Any
    ) -> Dict[str, Any]:
        """Apply *changes* to an existing Job."""
        record = self._store.get(record_id)
        if record is None:
            raise KeyError(f"Job {record_id!r} not found")
        record.update(changes)
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return self._store.put(record)

    def submit_job(self, record_id: str) -> bool:
        """Remove a Job; returns True on success."""
        if self._store.get(record_id) is None:
            return False
        self._store.delete(record_id)
        logger.info("submit_job: removed %s", record_id)
        return True

    def list_jobs(
        self,
        status: Optional[str] = None,
        limit:  int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """Return paginated Job records."""
        query: Dict[str, Any] = {}
        if status:
            query["status"] = status
        results = self._store.find(query, limit=limit, offset=offset)
        logger.debug("list_jobs: %d results", len(results))
        return results

    def iter_jobs(
        self, batch_size: int = 100
    ) -> Iterator[Dict[str, Any]]:
        """Yield all Job records in batches of *batch_size*."""
        offset = 0
        while True:
            page = self.list_jobs(limit=batch_size, offset=offset)
            if not page:
                break
            yield from page
            if len(page) < batch_size:
                break
            offset += batch_size
