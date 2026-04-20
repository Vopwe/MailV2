"""
Background task runner — threading + asyncio for single-user local app.
Tasks are persisted to SQLite so they survive restarts.
"""
import threading
import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class TaskStatus:
    task_id: str
    task_type: str = ""
    campaign_id: int | None = None
    status: str = "running"   # running | completed | failed | cancelled
    progress: int = 0
    total: int = 0
    message: str = ""
    error: str = ""
    started_at: str = ""
    completed_at: str = ""
    cancel_requested: bool = False

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "campaign_id": self.campaign_id,
            "status": self.status,
            "progress": self.progress,
            "total": self.total,
            "message": self.message,
            "error": self.error,
            "percent": round((self.progress / self.total * 100) if self.total > 0 else 0),
            "started_at": self.started_at,
            "completed_at": self.completed_at,
            "cancel_requested": self.cancel_requested,
        }


# In-memory cache (fast reads); DB is source of truth
_tasks: dict[str, TaskStatus] = {}
_lock = threading.Lock()


def _persist(task: TaskStatus):
    """Write task state to DB."""
    try:
        import database
        database.upsert_task(
            task_id=task.task_id,
            task_type=task.task_type,
            campaign_id=task.campaign_id,
            status=task.status,
            progress=task.progress,
            total=task.total,
            message=task.message,
            error=task.error,
            started_at=task.started_at,
            completed_at=task.completed_at,
        )
    except Exception:
        pass  # don't break task flow on DB error


def _task_from_row(row: dict) -> TaskStatus:
    return TaskStatus(
        task_id=row.get("task_id", ""),
        task_type=row.get("task_type", ""),
        campaign_id=row.get("campaign_id"),
        status=row.get("status", "completed"),
        progress=row.get("progress", 0),
        total=row.get("total", 0),
        message=row.get("message", ""),
        error=row.get("error", ""),
        started_at=row.get("started_at", ""),
        completed_at=row.get("completed_at", ""),
    )


def _sync_task_from_db(task_id: str) -> TaskStatus | None:
    try:
        import database
        row = database.get_db_task(task_id)
    except Exception:
        return None
    if not row:
        return None

    task = _task_from_row(row)
    with _lock:
        existing = _tasks.get(task_id)
        if existing and existing.started_at >= task.started_at:
            return existing
        _tasks[task_id] = task
    return task


def _sync_all_from_db():
    try:
        import database
        rows = database.get_db_tasks()
    except Exception:
        return

    with _lock:
        for row in rows:
            task = _task_from_row(row)
            existing = _tasks.get(task.task_id)
            if existing and existing.started_at >= task.started_at:
                continue
            _tasks[task.task_id] = task


def _load_from_db():
    """Load persisted tasks into memory on startup."""
    try:
        import database
        rows = database.get_db_tasks()
        with _lock:
            for row in rows:
                tid = row["task_id"]
                if tid not in _tasks:
                    _tasks[tid] = TaskStatus(
                        task_id=tid,
                        task_type=row.get("task_type", ""),
                        campaign_id=row.get("campaign_id"),
                        status=row.get("status", "completed"),
                        progress=row.get("progress", 0),
                        total=row.get("total", 0),
                        message=row.get("message", ""),
                        error=row.get("error", ""),
                        started_at=row.get("started_at", ""),
                        completed_at=row.get("completed_at", ""),
                    )
                    # Mark orphaned running tasks as failed
                    if _tasks[tid].status == "running":
                        _tasks[tid].status = "failed"
                        _tasks[tid].error = "Server restarted during task"
                        _tasks[tid].completed_at = datetime.now().isoformat()
                        _persist(_tasks[tid])
    except Exception:
        pass


def init_tasks():
    """Call after database.init_db() to restore persisted tasks."""
    _load_from_db()


def create_task(task_type: str = "", campaign_id: int | None = None) -> str:
    task_id = uuid.uuid4().hex[:12]
    task = TaskStatus(
        task_id=task_id,
        task_type=task_type,
        campaign_id=campaign_id,
        started_at=datetime.now().isoformat(),
    )
    with _lock:
        _tasks[task_id] = task
    _persist(task)
    return task_id


def get_task(task_id: str) -> TaskStatus | None:
    task = _tasks.get(task_id)
    if task is not None:
        return task
    return _sync_task_from_db(task_id)


def find_latest_task(
    task_type: str | None = None,
    campaign_id: int | None = None,
    statuses: tuple[str, ...] | None = None,
) -> TaskStatus | None:
    _sync_all_from_db()
    with _lock:
        matches = list(_tasks.values())

    if task_type is not None:
        matches = [task for task in matches if task.task_type == task_type]
    if campaign_id is not None:
        matches = [task for task in matches if task.campaign_id == campaign_id]
    if statuses is not None:
        matches = [task for task in matches if task.status in statuses]

    matches.sort(key=lambda task: task.started_at or "")
    return matches[-1] if matches else None


def update_task(task_id: str, **kwargs):
    with _lock:
        task = _tasks.get(task_id)
        if task:
            for k, v in kwargs.items():
                setattr(task, k, v)
    if task:
        _persist(task)


def complete_task(task_id: str, message: str = "Done"):
    with _lock:
        task = _tasks.get(task_id)
        if task:
            task.status = "completed"
            task.message = message
            task.completed_at = datetime.now().isoformat()
    if task:
        _persist(task)


def cancel_task(task_id: str) -> bool:
    """Flag a task for cancellation. Runner checks `is_cancelled()` at loop boundaries."""
    with _lock:
        task = _tasks.get(task_id)
        if task and task.status == "running":
            task.cancel_requested = True
            _persist(task)
            return True
    return False


def is_cancelled(task_id: str) -> bool:
    task = _tasks.get(task_id)
    return bool(task and task.cancel_requested)


def mark_cancelled(task_id: str, message: str = "Cancelled by user"):
    with _lock:
        task = _tasks.get(task_id)
        if task:
            task.status = "cancelled"
            task.message = message
            task.completed_at = datetime.now().isoformat()
    if task:
        _persist(task)


def fail_task(task_id: str, error: str):
    with _lock:
        task = _tasks.get(task_id)
        if task:
            task.status = "failed"
            task.error = error
            task.completed_at = datetime.now().isoformat()
    if task:
        _persist(task)


def run_in_background(async_func, task_id: str, *args, **kwargs):
    """Run an async function in a background thread with its own event loop."""
    def wrapper():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(async_func(task_id, *args, **kwargs))
        except Exception as e:
            fail_task(task_id, str(e))
        finally:
            try:
                import database
                database.close_db()
            except Exception:
                pass
            loop.close()

    thread = threading.Thread(target=wrapper, daemon=True)
    thread.start()
    return task_id


def get_all_tasks() -> list[dict]:
    _sync_all_from_db()
    with _lock:
        return [t.to_dict() for t in _tasks.values()]
