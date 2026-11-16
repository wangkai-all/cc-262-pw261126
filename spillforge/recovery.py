"""Crash recovery and terminal-state cleanup.

The recovery pass is deliberately conservative: it only deletes artifacts that
are *provably* worthless (open/partial files, complete files no checkpoint
mentions) and it never touches a file that has a reference owned by another
job. It then makes the ledger and the filesystem agree before any job is
allowed to run again.

Recovery cases covered
----------------------
A. Crash after run files were sealed.  Checkpoint = ``merging``; all runs are
   complete.  Keep the runs, delete only the partial result (``open``), requeue.
B. Crash in the middle of a merge.  Same as A, plus the partial output is
   ``open`` and must be discarded.  Runs are untouched because the merge stage
   consumed nothing.
C. Crash after publishing but before releasing the reservation.  The result is
   complete and the job row has ``result_path``.  Keep the result, delete runs,
   release.  No data movement or recomputation.
D. Crash while a job was parked with a shrunken reservation: unused estimate is
   already returned, artifacts stay referenced, job is requeued.
E. Crash of a cancelled job: cancel_requested is durable, so artifacts and the
   reservation are reclaimed rather than resumed.
"""

from __future__ import annotations

import json
import os
import shutil
import time
from pathlib import Path

from .config import Config
from .errors import ProtectedArtifact
from .ledger import Ledger
from .store import Store

TERMINAL = {"succeeded", "failed", "cancelled", "purged"}


def checkpoint_artifacts(cp: dict | None) -> set[str]:
    """Every artifact id a checkpoint needs to resume."""
    ids: set[str] = set()
    if not cp:
        return ids
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
    for key in ("out_artifact",):
        if state.get(key):
            ids.add(state[key])
    return ids


class Recovery:
    def __init__(self, store: Store, ledger: Ledger, config: Config, log=None):
        self.store = store
        self.ledger = ledger
        self.config = config
        self._log = log or (lambda kind, msg, **data: None)
        self.last_report: dict = {}

    # ---------------------------------------------------------------- startup

    def recover_all(self) -> dict:
        """Run the full pass. Safe to call repeatedly."""
        report = {
            "started_at": time.time(),
            "reconciled": None,
            "requeued": [],
            "finished": [],
            "cancelled": [],
            "failed": [],
            "cleaned_artifacts": 0,
            "orphan_files": 0,
        }
        self.store.q("DELETE FROM leases")  # single-process service
        report["reconciled"] = self.ledger.reconcile_disk()
        report["cleaned_artifacts"] += report["reconciled"].get("missing", 0)
        for job in self.store.list_jobs():
            try:
                self._recover_job(job, report)
            except Exception as exc:  # never abort the whole pass for one job
                self.store.event(
                    job["id"], "recovery_error", f"recovery failed: {exc}"
                )
        report["orphan_files"] = self._clean_orphan_files()
        report["finished_at"] = time.time()
        self.last_report = report
        if any(report[k] for k in ("requeued", "finished", "cancelled", "failed")):
            self._log("recovery", "recovery pass complete", **{
                k: report[k] for k in ("requeued", "finished", "cancelled", "failed")
            })
        return report

    def _recover_job(self, job: dict, report: dict) -> None:
        job_id = job["id"]
        status = job["status"]
        cp = self.store.get_checkpoint(job_id)
        if status == "uploading":
            return
        if status in TERMINAL:
            self._settle_terminal(job)
            return
        if job["cancel_requested"] or status == "cancelling":
            self.reclaim_job(job_id, "cancelled", reason="cancel completed by recovery")
            report["cancelled"].append(job_id)
            return
        if job.get("result_artifact_id"):
            result = self.store.q1(
                "SELECT * FROM artifacts WHERE id=?", (job["result_artifact_id"],)
            )
            if result and result["state"] in ("complete", "kept"):
                self._finalize_published(job, dict(result))
                report["finished"].append(job_id)
                return
        if status == "crashed":
            # A simulated/real crash: the lease is gone and nothing is writing.
            pass
        elif status in ("running", "finishing"):
            # A worker may still be alive in this process; the scheduler only
            # recovers at startup, so reaching here means the process restarted.
            pass
        elif status in ("queued", "paused_budget"):
            # Already waiting for admission. If it never started, there is no
            # checkpoint and nothing to preserve; if it has one, keep it.
            if cp is not None:
                self._fix_reservation(job)
            else:
                self._settle_terminal(job)
            return
        referenced = checkpoint_artifacts(cp)
        self._prune_artifacts(job_id, cp, referenced, report)
        self._fix_reservation(job)
        if self.store.get_checkpoint(job_id) is None:
            # Eviction while fixing the reservation dropped the workspace; the
            # job restarts from input.
            cp = None
            referenced = set()
        if job["attempts"] >= job["max_attempts"]:
            self.reclaim_job(
                job_id, "failed",
                reason="crash recovery exceeded retry limit",
                error=f"crashed {job['attempts']} times",
            )
            report["failed"].append(job_id)
            return
        stage = cp["stage"] if cp else "planned"
        self.store.update_job(
            job_id,
            status="queued",
            stage=stage,
            scheduler_note=(
                f"recovered at stage {stage}; "
                f"{len(referenced)} artifacts preserved"
            ),
            retry_after=None,
        )
        self.store.event(
            job_id, "recovered",
            f"requeued at stage {stage} after crash/restart",
            preserved=sorted(referenced),
        )
        report["requeued"].append(job_id)

    def _settle_terminal(self, job: dict) -> None:
        """Make sure a terminal job holds no reservation and no scratch."""
        if job["status"] == "succeeded":
            return
        job_id = job["id"]
        cp = self.store.get_checkpoint(job_id)
        referenced = checkpoint_artifacts(cp)
        result_id = job.get("result_artifact_id")
        for art in self.store.artifacts(job_id=job_id, include_deleted=False):
            if art["id"] == result_id and art["state"] in ("kept", "complete"):
                self.ledger.mark_kept(job_id, art["id"])
                continue
            if art["id"] in referenced and job["status"] not in TERMINAL:
                continue
            try:
                self.ledger.delete_artifact(art["id"], "terminal cleanup")
            except ProtectedArtifact:
                pass
        for kind in ("disk", "memory"):
            res = self.store.reservation(job_id, kind)
            if res and res["state"] == "active":
                self.ledger._release_reservation(res["id"], "terminal cleanup")
        self.store.clear_checkpoint(job_id)
        self.store.clear_heartbeat(job_id)

    def _finalize_published(self, job: dict, result: dict) -> None:
        job_id = job["id"]
        cp = self.store.get_checkpoint(job_id)
        referenced = checkpoint_artifacts(cp)
        # Drop the merge-output ref; the job row now owns the result.
        self.ledger.drop_ref(result["id"], "merge_output", job_id)
        # Reclaim runs and every other non-result artifact.
        for art in self.store.artifacts(job_id=job_id, include_deleted=False):
            if art["id"] == result["id"]:
                continue
            try:
                self.ledger.delete_artifact(art["id"], "post-publish cleanup")
            except ProtectedArtifact as exc:
                self.store.event(
                    job_id, "cleanup_refused", str(exc), artifact_id=art["id"]
                )
        self.ledger.mark_kept(job_id, result["id"])
        self.store.update_job(
            job_id,
            status="succeeded",
            stage="released",
            finished_at=job.get("finished_at") or time.time(),
            result_bytes=int(result["accounted_bytes"]),
        )
        self.ledger.release_job(job_id, "recovered after publish", keep=[result["id"]])
        self.store.set_checkpoint(job_id, "released",
                                  {"out_artifact": result["id"]})
        self.store.clear_heartbeat(job_id)
        self.store.event(job_id, "recovered", "publish completed during recovery")

    def _prune_artifacts(self, job_id: str, cp: dict | None,
                         referenced: set[str], report: dict) -> None:
        """Delete partial or unreferenced artifacts of an interrupted job."""
        for art in self.store.artifacts(job_id=job_id, include_deleted=False):
            if art["id"] in referenced and art["state"] == "complete":
                # Charge any bytes not covered by the reservation (a crash can
                # land between write() and accounting). Bounded debt applies.
                self._cover_usage(job_id, art)
                continue
            if art["state"] == "reclaiming":
                self.ledger.reconcile_disk()
                continue
            try:
                if self.ledger.delete_artifact(art["id"], "recovery: partial/unreferenced"):
                    report["cleaned_artifacts"] += 1
            except ProtectedArtifact as exc:
                self.store.event(
                    job_id, "cleanup_refused", str(exc), artifact_id=art["id"]
                )

    def _cover_usage(self, job_id: str, art: dict) -> None:
        """Make the reservation cover files that survived the crash."""
        res = self.store.reservation(job_id, "disk")
        if not res:
            return
        accounted = self.ledger._accounted_for(job_id)
        missing = accounted - res["amount"]
        if missing > 0:
            try:
                self.ledger.charge(job_id, "disk", missing,
                                   "recovery: cover unaccounted growth")
            except Exception:
                self.store.event(
                    job_id, "recovery_quota",
                    "could not charge crash overage; job will restart from scratch",
                    needed=missing,
                )
                raise

    def _fix_reservation(self, job: dict) -> None:
        job_id = job["id"]
        res = self.store.reservation(job_id, "disk")
        if res is None:
            return
        accounted = self.ledger._accounted_for(job_id)
        if accounted > res["amount"]:
            try:
                self.ledger.charge(job_id, "disk", accounted - res["amount"],
                                   "recovery: reservation below accounted")
            except Exception:
                # Cannot afford to keep the partial work: drop it and restart.
                self.store.event(
                    job_id, "recovery_evict",
                    "partial artifacts exceed available budget; discarding",
                    accounted=accounted, reserved=res["amount"],
                )
                self.reclaim_job(
                    job_id, "queued",
                    reason="recovery evicted unaffordable partial work",
                    keep_reservation=True,
                )
        mem = self.store.reservation(job_id, "memory")
        if mem and mem["used"]:
            self.ledger.note_memory(job_id, 0)

    # -------------------------------------------------------------- cleanup

    def reclaim_job(
        self,
        job_id: str,
        status: str,
        reason: str,
        error: str | None = None,
        keep_reservation: bool = False,
        expect_epoch: int | None = None,
    ) -> bool:
        """Delete a job's scratch/result artifacts and release its reservation.

        ``keep_reservation`` is used when a job stays queued (recovery evicted
        its partial work) and should still be admitted later; the reservation is
        shrunk to zero rather than released so the queue entry remains valid.

        ``expect_epoch`` guards against a *stale* cleanup: a detached cancel
        thread may wake up after the operator resumed the job. If the job's
        ``run_epoch`` no longer matches, another executor owns the workspace and
        nothing may be deleted. The check and the first state change happen in
        one transaction, so a dispatch cannot slip in between.

        Returns ``False`` when at least one artifact is still referenced (a
        merge reader has not let go yet). In that case the reservation is
        deliberately *kept*: releasing it while the bytes are still on disk
        would make the ledger claim free space it does not have. The caller
        retries after the reader has dropped its refs.
        """
        if status not in TERMINAL | {"queued"}:
            raise ValueError(f"bad reclaim status {status}")
        job = self.store.get_job(job_id)
        if not job:
            return False
        if expect_epoch is not None and int(job.get("run_epoch") or 0) != expect_epoch:
            self.store.event(
                job_id, "cleanup_superseded",
                f"reclaim skipped: job now runs under epoch "
                f"{job.get('run_epoch')} (cleanup expected {expect_epoch})",
            )
            return False
        refused = 0
        for art in self.store.artifacts(job_id=job_id, include_deleted=False):
            try:
                self.ledger.drop_ref(art["id"], "merge_output", job_id)
                self.ledger.delete_artifact(art["id"], f"reclaim: {reason}")
            except ProtectedArtifact as exc:
                refused += 1
                self.store.event(job_id, "cleanup_refused", str(exc),
                                 artifact_id=art["id"])
        if refused:
            self.store.event(
                job_id, "cleanup_deferred",
                f"{refused} artifacts still referenced; will retry cleanup",
            )
            return False
        self.store.clear_checkpoint(job_id)
        self.store.clear_heartbeat(job_id)
        if keep_reservation:
            for kind in ("disk", "memory"):
                res = self.store.reservation(job_id, kind)
                if res:
                    self.ledger.shrink(job_id, kind, 0, reason)
            self.store.update_job(
                job_id, status=status, note=reason, error=error,
                stage="planned", scheduler_note="restart from input",
            )
        else:
            for kind in ("disk", "memory"):
                res = self.store.reservation(job_id, kind)
                if res:
                    self.ledger._release_reservation(res["id"], reason)
            self.store.update_job(
                job_id, status=status, note=reason, error=error,
                stage="done" if status != "queued" else "planned",
                finished_at=time.time() if status in TERMINAL else None,
                scheduler_note="workspace reclaimed",
            )
        self.store.event(job_id, "reclaimed", reason, status=status)
        self._rm_workdir(job_id)
        return True

    def purge_result(self, job_id: str) -> bool:
        job = self.store.get_job(job_id)
        if not job:
            return False
        result_id = job.get("result_artifact_id")
        if result_id:
            try:
                self.ledger.delete_artifact(result_id, "result purged")
            except ProtectedArtifact:
                return False
        self.store.update_job(
            job_id, status="purged", result_path=None, result_artifact_id=None,
            result_bytes=0, finished_at=time.time(),
        )
        self._rm_workdir(job_id)
        self.store.event(job_id, "purged", "result deleted by operator")
        return True

    def _rm_workdir(self, job_id: str) -> None:
        path = self.config.tmp_dir / job_id
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)

    def reclaim_expired_tombstones(self) -> int:
        """Delete results of purged-or-expiring terminal jobs.

        Cancelled/failed jobs already lost their scratch in ``reclaim_job``;
        results of succeeded jobs are retained until purged, because the user
        may still download them. This only expires *scratch* left behind by a
        job that never published (e.g. paused forever and then abandoned).
        """
        now = time.time()
        expired = 0
        for job in self.store.list_jobs({"failed", "cancelled"}):
            finished = job.get("finished_at") or job.get("updated_at") or now
            if now - finished < self.config.tombstone_seconds:
                continue
            self._rm_workdir(job["id"])
            expired += 1
        if expired:
            self.store.event(None, "reaped", f"removed {expired} tombstone workdirs")
        return expired

    def _clean_orphan_files(self) -> int:
        """Remove files nobody accounts for.

        A file with no artifact row was written before its row committed (only
        possible before the ledger insert) or leaked. It is safe to delete
        because the ledger always creates rows first. Kept results live outside
        ``tmp`` and are never touched here.
        """
        known = {
            row["path"]
            for row in self.store.q(
                "SELECT path FROM artifacts WHERE state != 'deleted'"
            )
        }
        removed = 0
        cutoff = time.time() - 10
        for path in self.config.tmp_dir.rglob("*"):
            if not path.is_file():
                continue
            if str(path) in known:
                continue
            try:
                if path.stat().st_mtime > cutoff:
                    continue
                path.unlink()
                removed += 1
            except OSError:
                continue
        # Remove empty job directories.
        for d in self.config.tmp_dir.iterdir():
            if d.is_dir() and not any(d.iterdir()):
                try:
                    d.rmdir()
                except OSError:
                    pass
        if removed:
            self.store.event(None, "orphans_cleaned", f"removed {removed} orphan files")
        return removed

    @staticmethod
    def _job_totals(job_id: str) -> dict:
        return {"job_id": job_id}
