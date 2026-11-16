"""Service facade: submission, uploads, inspection and operator actions.

The HTTP layer is a thin wrapper over this class; the test suite drives the
same methods directly so business logic is exercised without a browser.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from dataclasses import asdict
from pathlib import Path
from typing import BinaryIO

from .config import Config, free_disk_bytes
from .errors import ConfigError, SourceNotFound
from .ledger import Ledger
from .planner import human
from .records import sample_file
from .scheduler import Scheduler
from .store import Store, new_id

KIND_ALIASES = {
    "sort": "sort",
    "sorted": "sort",
    "dedupe": "dedupe",
    "dedup": "dedupe",
    "unique": "dedupe",
    "join": "join",
}


class Service:
    def __init__(self, config: Config):
        self.config = config
        config.ensure_dirs()
        self.store = Store(config.db_path)
        self.ledger = Ledger(self.store, config, log=self._log)
        self.scheduler = Scheduler(self.store, self.ledger, config, log=self._log)
        self._demo_thread: threading.Thread | None = None
        self._demo_stop = threading.Event()
        self._started_at = time.time()

    def _log(self, kind: str, msg: str, job_id: str | None = None, **data) -> None:
        self.store.event(job_id, kind, msg, **data)

    # -------------------------------------------------------------- lifecycle

    def start(self) -> None:
        self.scheduler.start()

    def stop(self) -> None:
        self._demo_stop.set()
        self.scheduler.stop()
        self.store.close()

    # ---------------------------------------------------------------- uploads

    def save_upload(self, name: str, stream: BinaryIO, content_length: int | None = None) -> dict:
        """Persist an uploaded file into the service's input area.

        Uploads are not part of the temp budget: they are user data, retained
        until the job that uses them is done, and they are never deleted by the
        reaper while a job references them.
        """
        safe = _safe_name(name)
        family = new_id("up")
        path = self.config.files_dir / f"{family}-{safe}"
        digest = hashlib.sha256()
        total = 0
        limit = self.config.max_upload_bytes
        with open(path, "wb") as fh:
            while True:
                chunk = stream.read(256 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > limit:
                    fh.close()
                    try:
                        path.unlink()
                    except OSError:
                        pass
                    raise ConfigError(
                        f"upload exceeds max_upload_bytes={limit}"
                    )
                digest.update(chunk)
                fh.write(chunk)
        if content_length is not None and total != content_length:
            raise ConfigError(
                f"expected {content_length} bytes but received {total}"
            )
        if total == 0:
            try:
                path.unlink()
            except OSError:
                pass
            raise ConfigError("refusing empty upload")
        upload_id = family
        with self.store.tx() as cur:
            cur.execute(
                "INSERT INTO uploads(id,name,path,bytes,sha256,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (upload_id, name, str(path), total, digest.hexdigest(), time.time()),
            )
        upload = self.store.get_upload(upload_id)
        self._log("upload", f"received {name}", upload_id=upload_id, bytes=total)
        return upload

    def generate_demo(self, records: int, skew: int = 20, sorted_input: bool = False) -> dict:
        """Create a synthetic input file using the service's own generator.

        ``skew`` biases the key distribution so the planner's hot-key handling
        can be exercised without uploading a real dataset.
        """
        records = max(1, min(int(records), self.config.demo_max_records))
        upload_id = new_id("up")
        path = self.config.files_dir / f"{upload_id}-demo-{records}.tsv"
        rng = _DemoGenerator(records, skew=max(1, int(skew)))
        lines = list(rng.iter_lines())
        if sorted_input:
            lines.sort()
        with open(path, "wb") as fh:
            for line in lines:
                fh.write(line + b"\n")
        size = path.stat().st_size
        with self.store.tx() as cur:
            cur.execute(
                "INSERT INTO uploads(id,name,path,bytes,sha256,created_at) "
                "VALUES(?,?,?,?,?,?)",
                (
                    upload_id, f"demo-{records}.tsv", str(path), size,
                    hashlib.sha256(path.read_bytes()).hexdigest(), time.time(),
                ),
            )
        self._log("upload", f"generated demo input ({records} records)",
                  upload_id=upload_id, bytes=size)
        return self.store.get_upload(upload_id)

    def delete_upload(self, upload_id: str) -> bool:
        upload = self.store.get_upload(upload_id)
        if not upload:
            return False
        in_use = self.store.q1(
            "SELECT id FROM jobs WHERE (source_upload_id=? OR second_upload_id=?) "
            "AND status NOT IN ('succeeded','failed','cancelled','purged')",
            (upload_id, upload_id),
        )
        if in_use:
            return False
        self.store.delete_upload(upload_id)
        return True

    # --------------------------------------------------------------- submit

    def submit_job(
        self,
        kind: str,
        source_upload_id: str,
        second_upload_id: str | None = None,
        name: str | None = None,
        priority: int = 0,
    ) -> dict:
        kind = KIND_ALIASES.get(kind.lower())
        if not kind:
            raise ConfigError(f"unknown job kind; use sort, dedupe or join")
        source = self.store.get_upload(source_upload_id)
        if not source:
            raise SourceNotFound(f"upload {source_upload_id} not found")
        second = None
        if kind == "join":
            if not second_upload_id:
                raise ConfigError("join requires two inputs")
            second = self.store.get_upload(second_upload_id)
            if not second:
                raise SourceNotFound(f"upload {second_upload_id} not found")
        job = self.store.create_job(
            name=name or f"{kind} {Path(source['name']).name}",
            kind=kind,
            params={"priority": priority},
            status="queued",
        )
        job_id = job["id"]
        self.store.update_job(
            job_id,
            source_path=source["path"],
            source_bytes=source["bytes"],
            source_upload_id=source_upload_id,
            second_path=second["path"] if second else None,
            second_bytes=second["bytes"] if second else 0,
            second_upload_id=second_upload_id if second else None,
            priority=priority,
            submitted_at=time.time(),
        )
        for up in (source, second):
            if up:
                with self.store.tx() as cur:
                    cur.execute(
                        "UPDATE uploads SET job_id=? WHERE id=?",
                        (job_id, up["id"]),
                    )
            # Only the job references the input; sample it now so a broken file
            # is rejected before it consumes a scheduling slot.
        try:
            sample = sample_file(Path(source["path"]))
            self.store.update_job(job_id, stats={"source_sample": sample.to_public()})
        except Exception as exc:
            self.store.update_job(job_id, status="failed", error=str(exc),
                                  finished_at=time.time())
            raise
        self._log("submit", f"{kind} job queued", job_id=job_id)
        self.scheduler.wake()
        return self.store.get_job(job_id)

    # ------------------------------------------------------------ operations

    def cancel(self, job_id: str) -> bool:
        return self.scheduler.cancel(job_id)

    def resume(self, job_id: str) -> bool:
        return self.scheduler.resume(job_id)

    def purge(self, job_id: str) -> bool:
        return self.scheduler.recovery.purge_result(job_id)

    def recover(self) -> dict:
        return self.scheduler.recover_now()

    def set_policy(self, policy: str) -> None:
        self.scheduler.set_policy(policy)

    def set_faults(self, points: list[str]) -> list[str]:
        allowed = {
            "after_run_complete", "mid_merge", "before_budget_release",
            "after_publish_before_release", "after_final_write_before_seal",
            "after_bucket_sorted", "during_scatter", "during_partition",
            "mid_join_bucket",
        }
        points = [p for p in points if p in allowed]
        self.config = _with_faults(self.config, tuple(points))
        self.scheduler.config = self.config
        self.ledger.config = self.config
        self.scheduler.set_fault_points(points)
        self.store.event(None, "faults", f"fault points set to {points or 'none'}")
        return points

    # ----------------------------------------------------------------- state

    def state(self, event_cursor: int = 0) -> dict:
        jobs = []
        for job in self.store.list_jobs():
            jobs.append(self._job_public(job))
        snapshot = self.ledger.snapshot()
        try:
            free = free_disk_bytes(self.config.data_dir)
        except OSError:
            free = 0
        return {
            "now": time.time(),
            "started_at": self._started_at,
            "config": self.config.to_public_dict(),
            "policy": self.scheduler.policy,
            "ledger": snapshot,
            "ledger_entries": self.store.ledger_entries(limit=60),
            "volume": {
                "free": free,
                "data_dir": str(self.config.data_dir),
                "files_on_disk": self.store.total_files_on_disk()[1],
            },
            "jobs": jobs,
            "uploads": [
                {k: u[k] for k in ("id", "name", "bytes", "created_at", "job_id")}
                for u in self.store.list_uploads()
            ],
            "events": self.store.events(after=event_cursor, limit=200),
            "event_cursor": self._event_cursor(),
            "recovery": self.scheduler.recovery.last_report,
            "faults": list(self.config.fault_points),
        }

    def _event_cursor(self) -> int:
        value = self.store.scalar("SELECT COALESCE(MAX(id),0) FROM events")
        return int(value or 0)

    def _job_public(self, job: dict) -> dict:
        hb = self.store.heartbeat(job["id"])
        res = self.store.reservation(job["id"], "disk")
        mem = self.store.reservation(job["id"], "memory")
        artifacts = self.store.artifacts(job_id=job["id"], include_deleted=False)
        out = dict(job)
        out["heartbeat"] = hb
        out["reservation"] = (
            {k: res[k] for k in ("amount", "used", "debt_granted", "state")} if res else None
        )
        out["memory_reservation"] = (
            {k: mem[k] for k in ("amount", "used", "state")} if mem else None
        )
        out["artifacts"] = [
            {
                "id": a["id"], "role": a["role"], "state": a["state"],
                "bytes": a["accounted_bytes"], "records": a["records"],
                "refs": len(a["refs"]), "path": a["path"],
            }
            for a in artifacts
        ]
        plan = job.get("plan") or {}
        out["plan_summary"] = plan.get("reason") if plan else None
        return out

    def job_detail(self, job_id: str) -> dict | None:
        job = self.store.get_job(job_id)
        if not job:
            return None
        detail = self._job_public(job)
        detail["events"] = self.store.events(job_id=job_id, limit=200)
        detail["ledger_entries"] = self.store.ledger_entries(job_id=job_id, limit=200)
        cp = self.store.get_checkpoint(job_id)
        detail["checkpoint"] = (
            {"stage": cp["stage"], "updated_at": cp["updated_at"],
             "artifact_count": len(_checkpoint_ids(cp))}
            if cp else None
        )
        detail["artifacts_detail"] = self.store.artifacts(job_id=job_id, include_deleted=True)
        return detail

    def result_path(self, job_id: str) -> Path | None:
        job = self.store.get_job(job_id)
        if not job or not job.get("result_path"):
            return None
        path = Path(job["result_path"])
        return path if path.exists() else None

    def preview(self, job_id: str, limit: int = 40) -> list[str]:
        path = self.result_path(job_id)
        if not path:
            return []
        lines = []
        with open(path, "rb") as fh:
            for _ in range(limit):
                line = fh.readline()
                if not line:
                    break
                lines.append(line.rstrip(b"\n").decode("utf-8", "replace"))
        return lines

    def ledger_log(self, limit: int = 200) -> list[dict]:
        return self.store.ledger_entries(limit=limit)


class _DemoGenerator:
    """Deterministic skewed TSV generator (zipf-ish via modular hashing)."""

    def __init__(self, records: int, skew: int = 20):
        self.records = records
        self.skew = skew

    def iter_lines(self):
        for i in range(self.records):
            # Zipf-ish: a few keys dominate, the rest are uniform.
            if i % 7 == 0:
                key = f"hot-{i % max(1, self.skew // 2):03d}"
            else:
                key = f"key-{(i * 2654435761) % 100000:05d}"
            payload = f"v{i:08d}-{'x' * (i % 13)}"
            yield f"{key}\t{payload}".encode()


def _safe_name(name: str) -> str:
    base = os.path.basename(name or "upload")
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", base).strip("._") or "upload"
    return base[:120]


def _checkpoint_ids(cp: dict) -> set[str]:
    ids: set[str] = set()
    state = cp.get("state") or {}
    for run in state.get("runs") or []:
        if isinstance(run, dict) and run.get("artifact_id"):
            ids.add(run["artifact_id"])
    for key in ("buckets", "sorted"):
        for entry in (state.get(key) or {}).values():
            if isinstance(entry, dict):
                if entry.get("artifact_id"):
                    ids.add(entry["artifact_id"])
                for art in entry.get("artifacts") or []:
                    ids.add(art)
    if state.get("out_artifact"):
        ids.add(state["out_artifact"])
    return ids


def _with_faults(config: Config, points: tuple[str, ...]) -> Config:
    return Config(**{**asdict(config), "fault_points": points})
