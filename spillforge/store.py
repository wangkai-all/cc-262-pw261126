"""Durable metadata store.

SQLite is used as the write-ahead journal for the executor: every byte that the
executor may leave behind is described by an artifact row before the file is
created, and references that keep an artifact alive are rows in
``artifact_refs``. Recovery only ever reasons about those two tables, so a
process death at any point can be reconciled without guessing.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator

SCHEMA_VERSION = 3

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('sort', 'dedupe', 'join')),
    status TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT 'created',
    source_path TEXT,
    source_bytes INTEGER NOT NULL DEFAULT 0,
    source_upload_id TEXT,
    second_path TEXT,
    second_bytes INTEGER NOT NULL DEFAULT 0,
    second_upload_id TEXT,
    params TEXT NOT NULL DEFAULT '{}',
    plan TEXT NOT NULL DEFAULT '{}',
    stats TEXT NOT NULL DEFAULT '{}',
    error TEXT,
    result_path TEXT,
    result_bytes INTEGER NOT NULL DEFAULT 0,
    result_artifact_id TEXT,
    source_artifact_id TEXT,
    priority INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    submitted_at REAL,
    started_at REAL,
    finished_at REAL,
    updated_at REAL NOT NULL,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 3,
    retry_after REAL,
    parked_at REAL,
    park_count INTEGER NOT NULL DEFAULT 0,
    park_bytes INTEGER NOT NULL DEFAULT 0,
    run_epoch INTEGER NOT NULL DEFAULT 0,
    note TEXT,
    scheduler_note TEXT
);
CREATE INDEX IF NOT EXISTS ix_jobs_status ON jobs(status, created_at);

CREATE TABLE IF NOT EXISTS reservations (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('disk', 'memory')),
    amount INTEGER NOT NULL,
    used INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'active',
    debt_granted INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    UNIQUE(job_id, kind)
);
CREATE INDEX IF NOT EXISTS ix_reservations_active ON reservations(kind, state);

CREATE TABLE IF NOT EXISTS artifacts (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    reservation_id TEXT NOT NULL REFERENCES reservations(id),
    path TEXT NOT NULL,
    role TEXT NOT NULL,
    state TEXT NOT NULL,             -- open | complete | reclaiming | deleted | kept
    accounted_bytes INTEGER NOT NULL DEFAULT 0,
    expected_bytes INTEGER NOT NULL DEFAULT 0,
    records INTEGER NOT NULL DEFAULT 0,
    epoch INTEGER NOT NULL DEFAULT 0,
    meta TEXT NOT NULL DEFAULT '{}',
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_artifacts_job ON artifacts(job_id, state);
CREATE INDEX IF NOT EXISTS ix_artifacts_state ON artifacts(state);
CREATE TABLE IF NOT EXISTS artifact_refs (
    artifact_id TEXT NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    owner_kind TEXT NOT NULL,
    owner_id TEXT NOT NULL,
    created_at REAL NOT NULL,
    PRIMARY KEY (artifact_id, owner_kind, owner_id)
);
CREATE INDEX IF NOT EXISTS ix_refs_owner ON artifact_refs(owner_kind, owner_id);

CREATE TABLE IF NOT EXISTS checkpoints (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    stage TEXT NOT NULL,
    state TEXT NOT NULL,
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS leases (
    name TEXT PRIMARY KEY,
    holder TEXT NOT NULL,
    expires_at REAL NOT NULL,
    acquired_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS heartbeats (
    job_id TEXT PRIMARY KEY REFERENCES jobs(id) ON DELETE CASCADE,
    pid INTEGER NOT NULL,
    rss_bytes INTEGER NOT NULL DEFAULT 0,
    buffer_bytes INTEGER NOT NULL DEFAULT 0,
    stage TEXT NOT NULL DEFAULT '',
    progress REAL NOT NULL DEFAULT 0,
    message TEXT NOT NULL DEFAULT '',
    updated_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS uploads (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    path TEXT NOT NULL UNIQUE,
    bytes INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    created_at REAL NOT NULL,
    job_id TEXT
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    job_id TEXT,
    kind TEXT NOT NULL,
    message TEXT NOT NULL,
    data TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS ix_events_job ON events(job_id, id);
CREATE INDEX IF NOT EXISTS ix_events_kind ON events(kind, id);

CREATE TABLE IF NOT EXISTS ledger_entries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    tx_id TEXT NOT NULL,
    job_id TEXT,
    kind TEXT NOT NULL,
    reserved_delta INTEGER NOT NULL DEFAULT 0,
    actual_delta INTEGER NOT NULL DEFAULT 0,
    reason TEXT NOT NULL,
    balance_reserved INTEGER NOT NULL,
    balance_actual INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_ledger_job ON ledger_entries(job_id, id);
"""

JOB_COLUMNS = {
    "name",
    "kind",
    "status",
    "stage",
    "source_path",
    "source_bytes",
    "source_upload_id",
    "second_path",
    "second_bytes",
    "second_upload_id",
    "params",
    "plan",
    "stats",
    "error",
    "result_path",
    "result_bytes",
    "result_artifact_id",
    "source_artifact_id",
    "priority",
    "submitted_at",
    "started_at",
    "finished_at",
    "updated_at",
    "cancel_requested",
    "attempts",
    "max_attempts",
    "retry_after",
    "parked_at",
    "park_count",
    "park_bytes",
    "run_epoch",
    "note",
    "scheduler_note",
}

JSON_JOB_COLUMNS = {"params", "plan", "stats"}

TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "purged"}
ACTIVE_STATUSES = {"uploading", "queued", "running", "paused_budget", "cancelling", "finishing"}


def new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class Store:
    """Thread-safe SQLite access with a single writer connection.

    All executor writes are short transactions that never wrap file I/O. Reads
    (the HTTP polling) share the same connection under the same lock, which
    keeps the ordering of events and ledger entries obvious.
    """

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            str(self.path), timeout=10.0, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=10000")
        with self.tx() as cur:
            cur.executescript(SCHEMA)
            cur.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            # ``CREATE TABLE IF NOT EXISTS`` does not add columns to a database
            # created by an older build; do the additive migrations here so an
            # existing var/ directory survives an upgrade.
            cols = {row["name"] for row in cur.execute("PRAGMA table_info(jobs)")}
            for name, ddl in (
                ("park_count", "INTEGER NOT NULL DEFAULT 0"),
                ("park_bytes", "INTEGER NOT NULL DEFAULT 0"),
                ("run_epoch", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if name not in cols:
                    cur.execute(f"ALTER TABLE jobs ADD COLUMN {name} {ddl}")
            cur.execute(
                "UPDATE meta SET value=? WHERE key='schema_version'",
                (str(SCHEMA_VERSION),),
            )

    # ---------------------------------------------------------------- basics

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def tx(self, immediate: bool = True) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                yield cur
                self._conn.commit()
            except BaseException:
                try:
                    self._conn.rollback()
                except sqlite3.Error:
                    pass
                raise
            finally:
                cur.close()

    @contextmanager
    def read(self) -> Iterator[sqlite3.Cursor]:
        with self._lock:
            cur = self._conn.cursor()
            try:
                yield cur
            finally:
                cur.close()

    def q(self, sql: str, params: Iterable[Any] = ()) -> list[sqlite3.Row]:
        with self.read() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchall()

    def q1(self, sql: str, params: Iterable[Any] = ()) -> sqlite3.Row | None:
        rows = self.q(sql, params)
        return rows[0] if rows else None

    def scalar(self, sql: str, params: Iterable[Any] = ()) -> Any:
        row = self.q1(sql, params)
        return row[0] if row else None

    def now(self) -> float:
        return time.time()

    def meta(self, key: str, default: str | None = None) -> str | None:
        row = self.q1("SELECT value FROM meta WHERE key=?", (key,))
        return row["value"] if row else default

    def set_meta(self, key: str, value: str) -> None:
        with self.tx() as cur:
            cur.execute(
                "INSERT INTO meta(key, value) VALUES(?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )

    # --------------------------------------------------------------- events

    def event(
        self, job_id: str | None, kind: str, message: str, **data: Any
    ) -> int:
        with self.tx() as cur:
            cur.execute(
                "INSERT INTO events(ts, job_id, kind, message, data) VALUES(?,?,?,?,?)",
                (time.time(), job_id, kind, message, json.dumps(data, default=str)),
            )
            event_id = cur.lastrowid
            if event_id % 50 == 0:
                cur.execute(
                    "DELETE FROM events WHERE id < "
                    "(SELECT COALESCE(MAX(id), 0) - 5000 FROM events)"
                )
            return int(event_id)

    def events(
        self, after: int = 0, job_id: str | None = None, limit: int = 200
    ) -> list[dict]:
        sql = "SELECT * FROM events WHERE id > ?"
        params: list[Any] = [after]
        if job_id:
            sql += " AND job_id = ?"
            params.append(job_id)
        sql += " ORDER BY id ASC LIMIT ?"
        params.append(min(max(limit, 1), 1000))
        out = []
        for row in self.q(sql, params):
            item = dict(row)
            item["data"] = json.loads(item["data"] or "{}")
            out.append(item)
        return out

    # ----------------------------------------------------------------- jobs

    def create_job(
        self,
        name: str,
        kind: str,
        params: dict | None = None,
        status: str = "uploading",
        job_id: str | None = None,
    ) -> dict:
        job_id = job_id or new_id("job")
        ts = time.time()
        with self.tx() as cur:
            cur.execute(
                "INSERT INTO jobs(id,name,kind,status,stage,params,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?)",
                (job_id, name, kind, status, "created", json.dumps(params or {}), ts, ts),
            )
        return self.get_job(job_id)

    def get_job(self, job_id: str) -> dict | None:
        row = self.q1("SELECT * FROM jobs WHERE id=?", (job_id,))
        return self._job_row(row) if row else None

    def list_jobs(self, statuses: Iterable[str] | None = None) -> list[dict]:
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            rows = self.q(
                f"SELECT * FROM jobs WHERE status IN ({placeholders}) "
                "ORDER BY created_at ASC",
                list(statuses),
            )
        else:
            rows = self.q("SELECT * FROM jobs ORDER BY created_at ASC")
        return [self._job_row(r) for r in rows]

    @staticmethod
    def _job_row(row: sqlite3.Row) -> dict:
        job = dict(row)
        for col in JSON_JOB_COLUMNS:
            try:
                job[col] = json.loads(job.get(col) or "{}")
            except (TypeError, json.JSONDecodeError):
                job[col] = {}
        job["cancel_requested"] = bool(job.get("cancel_requested"))
        return job

    def update_job(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        unknown = set(fields) - JOB_COLUMNS
        if unknown:
            raise ValueError(f"unknown job columns: {sorted(unknown)}")
        assignments = []
        values: list[Any] = []
        for key, value in fields.items():
            if key in JSON_JOB_COLUMNS and not isinstance(value, str):
                value = json.dumps(value, default=str)
            if key == "cancel_requested":
                value = 1 if value else 0
            assignments.append(f"{key}=?")
            values.append(value)
        assignments.append("updated_at=?")
        values.append(time.time())
        values.append(job_id)
        with self.tx() as cur:
            cur.execute(f"UPDATE jobs SET {', '.join(assignments)} WHERE id=?", values)

    def claim_job_transition(
        self, job_id: str, from_statuses: Iterable[str], to_status: str, **fields: Any
    ) -> bool:
        """Compare-and-swap a status transition, returning True if it applied."""
        allowed = list(from_statuses)
        if not allowed:
            return False
        fields = {**fields, "status": to_status}
        unknown = set(fields) - JOB_COLUMNS
        if unknown:
            raise ValueError(f"unknown job columns: {sorted(unknown)}")
        assignments = []
        values: list[Any] = []
        for key, value in fields.items():
            if key in JSON_JOB_COLUMNS and not isinstance(value, str):
                value = json.dumps(value, default=str)
            if key == "cancel_requested":
                value = 1 if value else 0
            assignments.append(f"{key}=?")
            values.append(value)
        assignments.append("updated_at=?")
        values.append(time.time())
        placeholders = ",".join("?" for _ in allowed)
        values.extend([job_id, *allowed])
        with self.tx() as cur:
            cur.execute(
                f"UPDATE jobs SET {', '.join(assignments)} "
                f"WHERE id=? AND status IN ({placeholders})",
                values,
            )
            return cur.rowcount == 1

    # ------------------------------------------------------------ checkpoint

    def get_checkpoint(self, job_id: str) -> dict | None:
        row = self.q1("SELECT * FROM checkpoints WHERE job_id=?", (job_id,))
        if not row:
            return None
        return {
            "job_id": row["job_id"],
            "stage": row["stage"],
            "state": json.loads(row["state"] or "{}"),
            "updated_at": row["updated_at"],
        }

    def set_checkpoint(self, job_id: str, stage: str, state: dict) -> None:
        with self.tx() as cur:
            cur.execute(
                "INSERT INTO checkpoints(job_id,stage,state,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(job_id) DO UPDATE SET stage=excluded.stage, "
                "state=excluded.state, updated_at=excluded.updated_at",
                (job_id, stage, json.dumps(state, default=str), time.time()),
            )

    def clear_checkpoint(self, job_id: str) -> None:
        with self.tx() as cur:
            cur.execute("DELETE FROM checkpoints WHERE job_id=?", (job_id,))

    # --------------------------------------------------------------- leases

    def acquire_lease(self, name: str, holder: str, ttl: float) -> bool:
        now = time.time()
        with self.tx() as cur:
            row = cur.execute(
                "SELECT holder, expires_at FROM leases WHERE name=?", (name,)
            ).fetchone()
            if row and row["expires_at"] > now and row["holder"] != holder:
                return False
            cur.execute(
                "INSERT INTO leases(name,holder,expires_at,acquired_at) VALUES(?,?,?,?) "
                "ON CONFLICT(name) DO UPDATE SET holder=excluded.holder, "
                "expires_at=excluded.expires_at, acquired_at=excluded.acquired_at",
                (name, holder, now + ttl, now),
            )
            return True

    def renew_lease(self, name: str, holder: str, ttl: float) -> bool:
        with self.tx() as cur:
            cur.execute(
                "UPDATE leases SET expires_at=? WHERE name=? AND holder=?",
                (time.time() + ttl, name, holder),
            )
            return cur.rowcount == 1

    def release_lease(self, name: str, holder: str) -> None:
        with self.tx() as cur:
            cur.execute("DELETE FROM leases WHERE name=? AND holder=?", (name, holder))

    def force_expire_lease(self, name: str) -> None:
        with self.tx() as cur:
            cur.execute("DELETE FROM leases WHERE name=?", (name,))

    def expired_leases(self) -> list[dict]:
        return [dict(r) for r in self.q("SELECT * FROM leases WHERE expires_at <= ?", (time.time(),))]

    # ------------------------------------------------------------- ledger view

    def reservation(self, job_id: str, kind: str = "disk") -> dict | None:
        row = self.q1(
            "SELECT * FROM reservations WHERE job_id=? AND kind=?", (job_id, kind)
        )
        return dict(row) if row else None

    def reserved_total(self, kind: str = "disk") -> int:
        value = self.scalar(
            "SELECT COALESCE(SUM(amount),0) FROM reservations "
            "WHERE state='active' AND kind=?",
            (kind,),
        )
        return int(value or 0)

    def actual_total(self, kind: str = "disk") -> int:
        if kind == "memory":
            # Memory is not backed by artifacts; the reservation's used counter
            # is authoritative for buffers held by running workers.
            value = self.scalar(
                "SELECT COALESCE(SUM(used),0) FROM reservations "
                "WHERE state='active' AND kind='memory'"
            )
            return int(value or 0)
        value = self.scalar(
            "SELECT COALESCE(SUM(accounted_bytes),0) FROM artifacts "
            "WHERE state NOT IN ('deleted','kept')"
        )
        return int(value or 0)

    def retained_total(self) -> int:
        value = self.scalar(
            "SELECT COALESCE(SUM(accounted_bytes),0) FROM artifacts WHERE state='kept'"
        )
        return int(value or 0)

    def ledger_entries(self, job_id: str | None = None, limit: int = 200) -> list[dict]:
        if job_id:
            rows = self.q(
                "SELECT * FROM ledger_entries WHERE job_id=? ORDER BY id DESC LIMIT ?",
                (job_id, limit),
            )
        else:
            rows = self.q(
                "SELECT * FROM ledger_entries ORDER BY id DESC LIMIT ?", (limit,)
            )
        return [dict(r) for r in rows]

    def append_ledger(
        self,
        cur: sqlite3.Cursor,
        tx_id: str,
        job_id: str | None,
        kind: str,
        reserved_delta: int,
        actual_delta: int,
        reason: str,
        balance_reserved: int,
        balance_actual: int,
    ) -> None:
        cur.execute(
            "INSERT INTO ledger_entries(ts,tx_id,job_id,kind,reserved_delta,actual_delta,"
            "reason,balance_reserved,balance_actual) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                time.time(),
                tx_id,
                job_id,
                kind,
                reserved_delta,
                actual_delta,
                reason,
                balance_reserved,
                balance_actual,
            ),
        )

    def artifact_refs(self, artifact_id: str) -> list[dict]:
        rows = self.q(
            "SELECT * FROM artifact_refs WHERE artifact_id=? ORDER BY created_at",
            (artifact_id,),
        )
        return [dict(r) for r in rows]

    def artifacts(self, job_id: str | None = None, include_deleted: bool = False) -> list[dict]:
        sql = "SELECT * FROM artifacts"
        clauses = []
        params: list[Any] = []
        if job_id:
            clauses.append("job_id=?")
            params.append(job_id)
        if not include_deleted:
            clauses.append("state != 'deleted'")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY created_at ASC"
        out = []
        for row in self.q(sql, params):
            item = dict(row)
            item["refs"] = self.artifact_refs(item["id"])
            out.append(item)
        return out

    def set_heartbeat(
        self,
        job_id: str,
        pid: int,
        stage: str,
        progress: float,
        message: str = "",
        rss_bytes: int = 0,
        buffer_bytes: int = 0,
    ) -> None:
        with self.tx() as cur:
            cur.execute(
                "INSERT INTO heartbeats(job_id,pid,rss_bytes,buffer_bytes,stage,progress,"
                "message,updated_at) VALUES(?,?,?,?,?,?,?,?) "
                "ON CONFLICT(job_id) DO UPDATE SET pid=excluded.pid, "
                "rss_bytes=excluded.rss_bytes, buffer_bytes=excluded.buffer_bytes, "
                "stage=excluded.stage, progress=excluded.progress, "
                "message=excluded.message, updated_at=excluded.updated_at",
                (
                    job_id, pid, rss_bytes, buffer_bytes, stage, float(progress),
                    message, time.time(),
                ),
            )

    def heartbeats(self) -> list[dict]:
        return [dict(r) for r in self.q("SELECT * FROM heartbeats ORDER BY job_id")]

    def heartbeat(self, job_id: str) -> dict | None:
        row = self.q1("SELECT * FROM heartbeats WHERE job_id=?", (job_id,))
        return dict(row) if row else None

    def clear_heartbeat(self, job_id: str) -> None:
        with self.tx() as cur:
            cur.execute("DELETE FROM heartbeats WHERE job_id=?", (job_id,))

    # -------------------------------------------------------------- uploads

    def create_upload(self, upload_id: str, name: str, path: Path, sha256: str) -> dict:
        now = time.time()
        size = os.stat(path).st_size
        with self.tx() as cur:
            cur.execute(
                "INSERT INTO uploads(id,name,path,bytes,sha256,created_at) VALUES(?,?,?,?,?,?)",
                (upload_id, name, str(path), size, sha256, now),
            )
        return self.get_upload(upload_id)

    def get_upload(self, upload_id: str) -> dict | None:
        row = self.q1("SELECT * FROM uploads WHERE id=?", (upload_id,))
        return dict(row) if row else None

    def list_uploads(self) -> list[dict]:
        return [dict(r) for r in self.q("SELECT * FROM uploads ORDER BY created_at DESC")]

    def delete_upload(self, upload_id: str) -> None:
        upload = self.get_upload(upload_id)
        if not upload:
            return
        try:
            os.unlink(upload["path"])
        except FileNotFoundError:
            pass
        with self.tx() as cur:
            cur.execute("DELETE FROM uploads WHERE id=?", (upload_id,))

    def expire_orphan_uploads(self, older_than: float) -> list[dict]:
        cutoff = time.time() - older_than
        rows = [
            dict(r)
            for r in self.q(
                "SELECT * FROM uploads WHERE created_at < ? AND "
                "(job_id IS NULL OR job_id NOT IN (SELECT id FROM jobs))",
                (cutoff,),
            )
        ]
        for row in rows:
            self.delete_upload(row["id"])
        return rows

    def total_files_on_disk(self) -> tuple[int, int]:
        count = 0
        total = 0
        for row in self.q("SELECT path FROM artifacts WHERE state != 'deleted'"):
            try:
                st = os.stat(row["path"])
            except OSError:
                continue
            count += 1
            total += st.st_size
        return count, total
