"""Admission and scheduling.

Two policies, both explicit so the throughput/fairness trade-off is testable:

``throughput``
    One spiller at a time, FIFO. This maximises sequential I/O and minimises
    merge fan-in churn, and is the right choice when jobs are large and rare.

``fairness``
    Up to ``max_concurrent_jobs`` spillers, choosing by shortest-estimated-work
    first with aging. Small jobs finish quickly, which keeps latency low, while
    aging guarantees a big job eventually gets the disk even if small jobs keep
    arriving. The cost is interleaved I/O (lower sequential throughput).

``adaptive``
    Fairness while disk pressure is above ``pressure_high_watermark`` (many
    jobs are queued and the volume is nearly full, so finishing small jobs frees
    space fastest), throughput otherwise.

A big under-estimating job cannot starve the rest: admission caps a job's own
reservation, debt is bounded, and a job that would exceed its cap is *parked*
(status ``paused_budget``) with its estimate shrunken back to what it actually
uses. The capacity it gives back is immediately available to other jobs, and it
is retried after a cooldown. That is why one job's bad estimate slows only that
job.
"""

from __future__ import annotations

import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from .config import Config
from .errors import BudgetDenied, QuotaExceeded, SpillForgeError
from .executor import BudgetPause, Cancelled, Executor, InjectedFault
from .ledger import Ledger
from .recovery import Recovery
from .store import Store, new_id

#: Statuses the scheduler may dispatch. ``paused_budget`` is included so a
#: parked job is retried automatically after its backoff (``retry_after``)
#: instead of waiting for the operator; ``_dispatch`` claims both statuses in a
#: single compare-and-swap so a double dispatch is impossible.
RUNNABLE = {"queued", "paused_budget"}
ACTIVE = {"running", "finishing"}
STOPPABLE = {"running", "paused_budget", "queued"}


class Scheduler:
    def __init__(self, store: Store, ledger: Ledger, config: Config, log=None):
        self.store = store
        self.ledger = ledger
        self.config = config
        self.recovery = Recovery(store, ledger, config, log=self._event)
        self._log = log or (lambda kind, msg, **data: None)
        self._stop = threading.Event()
        self._wake = threading.Event()
        self._thread: threading.Thread | None = None
        self._running: dict[str, threading.Event] = {}
        self._cleanup_threads: list[threading.Thread] = []
        self._lock = threading.RLock()
        self._lease_holder = new_id("sched")
        self._last_reap = 0.0
        self._policy_override: str | None = None
        self._fault_points: tuple[str, ...] = tuple(config.fault_points)
        self._worker_pool = ThreadPoolExecutor(
            max_workers=max(1, config.max_concurrent_jobs + 1),
            thread_name_prefix="spillforge",
        )

    def _event(self, kind: str, msg: str, job_id: str | None = None, **data) -> None:
        self.store.event(job_id, kind, msg, **data)

    # --------------------------------------------------------------- control

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self.store.q("DELETE FROM leases")
        self.recovery.recover_all()
        self._thread = threading.Thread(target=self._loop, name="scheduler", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10.0) -> None:
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=timeout)
        with self._lock:
            for ev in self._running.values():
                ev.set()
        self._worker_pool.shutdown(wait=True, cancel_futures=True)
        # ``_finish_cancel`` runs on a detached thread; join them so the store
        # is not closed mid-write (which would surface as a noisy
        # "Cannot operate on a closed database" during shutdown).
        deadline = time.time() + timeout
        for thread in list(self._cleanup_threads):
            remaining = deadline - time.time()
            if remaining <= 0:
                break
            thread.join(timeout=remaining)
        with self._lock:
            self._cleanup_threads = [
                t for t in self._cleanup_threads if t.is_alive()
            ]

    def wake(self) -> None:
        self._wake.set()

    @property
    def policy(self) -> str:
        return self._policy_override or self.config.scheduler_policy

    def set_policy(self, policy: str) -> None:
        if policy not in ("throughput", "fairness", "adaptive"):
            raise SpillForgeError(f"unknown policy {policy}")
        self._policy_override = policy
        self._event("policy", f"scheduler policy set to {policy}")
        self.wake()

    def set_fault_points(self, points: list[str] | tuple[str, ...]) -> None:
        self._fault_points = tuple(points)

    # ------------------------------------------------------------------ loop

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as exc:  # keep the scheduler alive
                self._event("scheduler_error", f"{type(exc).__name__}: {exc}")
            self._wake.wait(timeout=0.5)
            self._wake.clear()

    def _tick(self) -> None:
        now = time.time()
        if now - self._last_reap >= self.config.reap_interval_seconds:
            self._last_reap = now
            self.recovery.reclaim_expired_tombstones()
            self._reap_uploads()
            self._retry_deferred_cleanup()
        concurrency = self._concurrency()
        with self._lock:
            active = len(self._running)
        if active >= concurrency:
            return
        with self._lock:
            busy = set(self._running)
        candidates = [
            job for job in self.store.list_jobs(RUNNABLE)
            if job["id"] not in busy
            and (not job.get("retry_after") or job["retry_after"] <= now)
        ]
        if not candidates:
            return
        ordered = self._order(candidates)
        for job in ordered[: concurrency - active]:
            self._dispatch(job)

    def _concurrency(self) -> int:
        policy = self.policy
        if policy == "throughput":
            return 1
        if policy == "adaptive":
            if self.ledger.pressure("disk") >= self.config.pressure_high_watermark:
                return max(1, self.config.max_concurrent_jobs)
            return 1
        return max(1, self.config.max_concurrent_jobs)

    def _order(self, jobs: list[dict]) -> list[dict]:
        now = time.time()
        policy = self.policy
        if policy == "throughput":
            return sorted(jobs, key=lambda j: (j["created_at"], j["id"]))
        if policy == "adaptive" and self.ledger.pressure("disk") < self.config.pressure_high_watermark:
            return sorted(jobs, key=lambda j: (j["created_at"], j["id"]))

        def key(job: dict):
            est = int(job["source_bytes"]) + int(job["second_bytes"])
            # Aging: waiting beyond the aging window promotes a job to the
            # front regardless of size.
            age = now - (job.get("submitted_at") or job["created_at"])
            aged = age >= self.config.fairness_aging_seconds
            return (0 if aged else 1, est if not aged else 0, job["created_at"], job["id"])

        return sorted(jobs, key=key)

    def _dispatch(self, job: dict) -> None:
        job_id = job["id"]
        holder = f"{self._lease_holder}:{job_id}"
        if not self.store.acquire_lease(f"job:{job_id}", holder, self.config.lease_ttl_seconds):
            return
        cancel = threading.Event()
        with self._lock:
            self._running[job_id] = cancel
        # Every dispatch gets a new run epoch. Detached cleanup threads capture
        # it and refuse to reclaim a *later* run of the same job: without this,
        # a cancel-requested job that the operator resumed could have its fresh
        # workspace deleted by the previous cancel's retry thread.
        epoch = int(job.get("run_epoch") or 0) + 1
        ok = self.store.claim_job_transition(
            job_id, ["queued", "paused_budget"], "running",
            started_at=job.get("started_at") or time.time(),
            # A re-dispatch after a budget pause is a continuation, not a new
            # attempt: charging it would burn the crash-retry budget of a job
            # that simply waited for the disk.
            attempts=int(job["attempts"]) + (0 if job["status"] == "paused_budget" else 1),
            run_epoch=epoch,
            scheduler_note=f"admitted under {self.policy}",
        )
        if not ok:
            with self._lock:
                self._running.pop(job_id, None)
            self.store.release_lease(f"job:{job_id}", holder)
            # ``cancel`` may have observed the ``_running`` entry registered
            # above and concluded that this worker would finish the cleanup.
            # Its status change to ``cancelling`` is exactly what made our
            # compare-and-swap fail, so no worker exists to raise ``Cancelled``
            # and nothing else would ever settle the job: it would sit in
            # ``cancelling`` forever. Finish it here.
            current = self.store.get_job(job_id) or {}
            if current.get("cancel_requested") or current.get("status") == "cancelling":
                self._cancel_cleanup(job_id)
            return
        self.store.event(
            job_id, "scheduled",
            f"admitted by {self.policy} (active={len(self._running)})",
            run_epoch=epoch,
        )
        self._worker_pool.submit(self._run_job, job_id, holder, cancel, epoch)

    def _run_job(self, job_id: str, holder: str, cancel: threading.Event,
                 epoch: int) -> None:
        try:
            job = self.store.get_job(job_id)
            if not job:
                return
            executor = Executor(
                self.store, self.ledger, self.config, job,
                stop_flag=cancel.is_set,
                fault_points=self._fault_points,
                log=lambda kind, msg, **data: self.store.event(job_id, kind, msg, **data),
            )
            result = executor.run()
            if result.get("status") == "succeeded":
                self.store.event(job_id, "done", "job succeeded",
                                 output_bytes=result.get("result_bytes", 0))
            elif result.get("status") == "cancelled":
                pass
        except BudgetPause as exc:
            self._park(job_id, str(exc), exc.requested, exc.available)
        except Cancelled:
            self._cancel_cleanup(job_id, epoch)
        except InjectedFault as exc:
            # Simulated crash: leave the workspace exactly as the fault found
            # it so the recovery pass (on restart or on demand) can handle it.
            self.store.event(job_id, "crash", f"simulated crash: {exc}")
            self.store.update_job(job_id, status="crashed", stage="crashed")
        except Exception as exc:
            detail = traceback.format_exc(limit=6)
            self.store.event(job_id, "error", f"{type(exc).__name__}: {exc}",
                             traceback=detail)
            job = self.store.get_job(job_id)
            attempts = int(job["attempts"]) if job else 1
            if attempts < (job["max_attempts"] if job else 1):
                self.store.update_job(
                    job_id, status="queued",
                    scheduler_note=f"retry after error: {type(exc).__name__}",
                    retry_after=time.time() + 1.5 * attempts,
                )
            else:
                self.recovery.reclaim_job(
                    job_id, "failed", reason="executor error",
                    error=f"{type(exc).__name__}: {exc}",
                    expect_epoch=epoch,
                )
        finally:
            with self._lock:
                self._running.pop(job_id, None)
            self.store.release_lease(f"job:{job_id}", holder)
            self.wake()

    def _park(self, job_id: str, message: str, requested: int, available: int) -> None:
        """Budget pause: keep sealed work, return the unused estimate, retry.

        The reservation is shrunk to exactly what is already on disk, so the
        unusable portion flows back to the global pool and can admit other
        jobs. Debt granted so far is retained so the retry does not thrash.

        Retries use exponential backoff capped at ``park_backoff_max_seconds``.
        A job that keeps failing admission without making progress is failed
        after ``park_attempt_limit`` pauses: silently retrying forever would
        look like a hung job in the UI and keep re-granting debt it cannot use.

        A job that has written nothing yet is only waiting for another job's
        reservation to be released; that is normal queueing, not a failure, so
        the stall counter only accumulates once the job holds sealed runs.
        """
        accounted = self.ledger._accounted_for(job_id)
        self.ledger.shrink(job_id, "disk", accounted, "park: return unused estimate")
        self.ledger.note_memory(job_id, 0)
        job = self.store.get_job(job_id) or {}
        parks = int(job.get("park_count") or 0) + 1
        # Count *consecutive* pauses that made no progress: if the job managed
        # to seal more bytes since the previous pause it is converging (the
        # disk is merely busy) and the counter restarts. Comparing bytes rather
        # than a flag means a run that is written and then parked still counts
        # as progress for the next attempt.
        previously_kept = int(job.get("park_bytes") or 0)
        progressing = accounted > previously_kept
        stalls = 1 if progressing else parks
        held = accounted
        # Proactive rejection: if even an empty queue could not grant the
        # estimate, waiting cannot help. Report that now instead of letting the
        # job spin until the volume actually fills. Only applied when the job
        # holds no work yet; with sealed runs we keep retrying (discarding
        # finished work is worse than queueing) up to the stall limit.
        ceiling = self.ledger.max_grant("disk")
        if requested > ceiling and held == 0:
            self.store.event(
                job_id, "park_exhausted",
                f"estimate {requested} bytes exceeds the largest possible grant "
                f"{ceiling} bytes; failing instead of waiting for the disk to fill",
                requested=requested, available=ceiling,
            )
            self.recovery.reclaim_job(
                job_id, "failed", reason="estimate exceeds the achievable budget",
                error=(
                    f"the job needs {requested} bytes of scratch but the largest "
                    f"admission the current budget can ever grant is {ceiling}. "
                    "Reduce max_job_fraction or the job size, raise "
                    "disk_budget_bytes, or purge retained results."
                ),
            )
            self.wake()
            return
        if stalls >= self.config.park_attempt_limit and held > 0:
            self.store.event(
                job_id, "park_exhausted",
                f"gave up after {stalls} consecutive budget pauses without "
                f"progress; last denial: {message}",
                requested=requested, available=available,
            )
            self.recovery.reclaim_job(
                job_id, "failed", reason="no budget could be secured",
                error=(
                    f"gave up after {stalls} consecutive budget pauses: {message}. "
                    "Lower the job size, raise the disk budget, or purge "
                    "retained results to free admission capacity."
                ),
            )
            self.wake()
            return
        delay = min(
            self.config.park_backoff_max_seconds,
            self.config.park_backoff_seconds * (2 ** min(parks - 1, 16)),
        )
        self.store.update_job(
            job_id,
            status="paused_budget",
            scheduler_note=(
                f"paused: {message} (needed {requested}, had {available}); "
                f"kept {accounted} bytes of runs; retry in {delay:.0f}s"
            ),
            retry_after=time.time() + delay,
            parked_at=time.time(),
            park_count=stalls,
            park_bytes=accounted,
        )
        self.store.event(
            job_id, "paused", message, requested=requested, available=available,
            kept_bytes=accounted, delay=delay, park_count=stalls,
        )

    def _cancel_cleanup(self, job_id: str, epoch: int | None = None) -> None:
        # Do not delete scratch here: the executor may have committed refs.
        # Mark the job cancelled and let recovery/reclaim settle it, so a UI
        # observer can see exactly what was cancelled before it disappears.
        job = self.store.get_job(job_id)
        if job and job["status"] in ("cancelled",):
            return
        if epoch is None:
            epoch = int((job or {}).get("run_epoch") or 0)
        self.store.update_job(job_id, status="cancelling")
        self.store.event(job_id, "cancel", "executor stopped at a checkpoint",
                         run_epoch=epoch)
        thread = threading.Thread(
            target=self._finish_cancel, args=(job_id, epoch), daemon=True
        )
        with self._lock:
            self._cleanup_threads.append(thread)
        thread.start()

    def _finish_cancel(self, job_id: str, epoch: int) -> None:
        for delay in (0.0, 0.05, 0.2, 0.5):
            if delay:
                time.sleep(delay)
            try:
                if self.recovery.reclaim_job(
                    job_id, "cancelled", reason="operator cancel",
                    expect_epoch=epoch,
                ):
                    return
            except Exception as exc:
                self.store.event(job_id, "error", f"cancel cleanup failed: {exc}")
                return
            if self._epoch_moved(job_id, epoch):
                self.store.event(
                    job_id, "cleanup_superseded",
                    "another run started before cleanup finished; leaving it alone",
                )
                return
        # A reader is still holding a reference after ~1s; leave the job in
        # ``cancelled`` with its reservation intact so the periodic reaper
        # retries instead of pretending the bytes are gone.
        if not self._epoch_moved(job_id, epoch):
            self.store.event(
                job_id, "cleanup_deferred",
                "cancel cleanup postponed until running merges release their refs",
            )

    def _epoch_moved(self, job_id: str, epoch: int) -> bool:
        """True when the job has been dispatched again since ``epoch``.

        Cleanup work captured an epoch, so it must not touch a workspace it no
        longer owns: a resumed job has a fresh executor writing into the same
        directory, and deleting it would fail the new run with a vanished
        checkpoint instead of a cancelled one.
        """
        job = self.store.get_job(job_id)
        return bool(job and int(job.get("run_epoch") or 0) != epoch)

    def _reap_uploads(self) -> None:
        self.store.expire_orphan_uploads(self.config.tombstone_seconds * 10)

    def _retry_deferred_cleanup(self) -> None:
        """Finish reclaims that were blocked on a live merge reader.

        ``_finish_cancel`` returns early when a run is still pinned by the
        executor's ``merge_input`` ref. The worker drops that ref when it
        stops, so the periodic reaper can retry safely; until then the job
        keeps its reservation and the UI shows the bytes honestly.
        """
        for job in self.store.list_jobs({"cancelled"}):
            with self._lock:
                if job["id"] in self._running:
                    continue
            if not self.store.artifacts(job_id=job["id"], include_deleted=False):
                continue
            with self._lock:
                if any(t.is_alive() for t in self._cleanup_threads):
                    continue
            try:
                self.recovery.reclaim_job(
                    job["id"], "cancelled",
                    reason="operator cancel (deferred cleanup)",
                    expect_epoch=int(job.get("run_epoch") or 0),
                )
            except Exception as exc:  # keep the loop alive
                self._event("scheduler_error", f"deferred cleanup failed: {exc}",
                            job_id=job["id"])

    # ------------------------------------------------------------------- api

    def submit(self, job_id: str) -> None:
        self.store.update_job(
            job_id, status="queued", submitted_at=time.time(), attempts=0,
            scheduler_note="waiting for admission",
        )
        self._event("submit", f"{job_id} queued")
        self.wake()

    def cancel(self, job_id: str) -> bool:
        job = self.store.get_job(job_id)
        if not job:
            return False
        if job["status"] in ("succeeded", "failed", "cancelled", "purged"):
            return False
        self.store.update_job(job_id, cancel_requested=True)
        with self._lock:
            ev = self._running.get(job_id)
        if ev:
            self.store.update_job(job_id, status="cancelling")
            ev.set()
            self.store.event(job_id, "cancel", "cancel requested; stopping executor")
        else:
            self.store.update_job(job_id, status="cancelling")
            thread = threading.Thread(
                target=self._finish_cancel,
                args=(job_id, int(job.get("run_epoch") or 0)), daemon=True,
            )
            with self._lock:
                self._cleanup_threads.append(thread)
            thread.start()
        self.wake()
        return True

    def resume(self, job_id: str) -> bool:
        job = self.store.get_job(job_id)
        if not job:
            return False
        if job["status"] not in ("paused_budget", "failed", "cancelled"):
            return False
        # Bump the run epoch *before* requeueing. A detached cancel-cleanup
        # thread may still be sleeping in its retry loop; with a new epoch it
        # will see the job was re-dispatched and leave the new workspace alone.
        self.store.update_job(
            job_id, run_epoch=int(job.get("run_epoch") or 0) + 1
        )
        if job["status"] in ("failed", "cancelled"):
            # Only a job whose artifacts survived can be resumed without a
            # full re-read; otherwise the executor restarts from input because
            # the checkpoint was cleared on reclaim.
            cp = self.store.get_checkpoint(job_id)
            if not cp:
                self.recovery._rm_workdir(job_id)
        if not self.store.reservation(job_id, "disk"):
            # The reservation was released; request admission again from the
            # original estimate and let admission reject if it does not fit.
            # Memory is requested exactly like a first admission: asking for
            # the whole cap would be denied by ``admit_share`` and turn an
            # operator resume into a silent pause.
            estimate = int((job.get("plan") or {}).get("expected_temp_bytes")
                           or job["source_bytes"] * 2)
            memory = max(
                0,
                min(
                    self.ledger.job_cap("memory"),
                    self.ledger.admit_share("memory", job_id),
                ),
            )
            try:
                self.ledger.open_job(
                    job_id, min(estimate, self.ledger.job_cap("disk")),
                    memory, "resume",
                )
            except BudgetDenied:
                self.store.update_job(
                    job_id, status="paused_budget",
                    scheduler_note="resume denied: not enough budget yet",
                    retry_after=time.time() + 2.0,
                )
                return False
        self.store.update_job(
            job_id, status="queued", cancel_requested=False,
            retry_after=None, note=None,
            scheduler_note="resumed by operator",
            attempts=0, park_count=0, park_bytes=0,
        )
        self.store.event(job_id, "resume", "operator resumed job")
        self.wake()
        return True

    def recover_now(self) -> dict:
        report = self.recovery.recover_all()
        self.wake()
        return report
