"""Checkpointed external sort / dedupe / join executor.

Failure model
-------------
The executor is a state machine persisted in SQLite (``checkpoints``) plus a set
of artifact rows. It never trusts in-memory state after a restart: each stage
re-reads its inputs, and recovery (``recovery.py``) decides which artifacts from
a dead attempt are worth keeping:

    planned -> spilling -> merging -> publishing -> released

Fault injection
---------------
``FaultInjector`` raises ``InjectedFault`` exactly at named boundaries so the
test suite can prove recovery from the three cases required by the brief:
  * ``after_run_complete`` — run files sealed, merge has not started;
  * ``mid_merge`` — some merge inputs were opened and partially consumed;
  * ``before_budget_release`` / ``after_publish_before_release`` — result is
    published but the reservation has not been released yet.
"""

from __future__ import annotations

import hashlib
import heapq
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO, Iterator

from .config import Config
from .errors import BudgetDenied, BudgetInfeasible, QuotaExceeded, SpillForgeError
from .ledger import Ledger
from .planner import Plan, Planner, human
from .records import iter_lines, key_of, payload_of, sample_file
from .store import Store, new_id


class InjectedFault(SpillForgeError):
    """Raised deliberately at a named boundary (test-only)."""


class BudgetPause(SpillForgeError):
    """The job cannot continue with its current budget; park and retry later."""

    def __init__(self, message: str, requested: int = 0, available: int = 0):
        self.requested = requested
        self.available = available
        super().__init__(message)


class Cancelled(SpillForgeError):
    pass


class FaultInjector:
    #: Two names for the same boundary, kept so the UI and docs can talk about
    #: "published but not yet released" from either direction.
    ALIASES = {
        "after_publish_before_release": "before_budget_release",
        "before_budget_release": "after_publish_before_release",
    }

    def __init__(self, points: tuple[str, ...] | list[str] = (), sink=None):
        self.points = set(points)
        self.sink = sink

    def trip(self, name: str) -> None:
        alias = self.ALIASES.get(name)
        if name in self.points or (alias and alias in self.points):
            if self.sink:
                self.sink(name)
            raise InjectedFault(f"injected fault at {name}")


@dataclass
class RunInfo:
    artifact_id: str
    path: str
    records: int
    bytes: int


class Executor:
    """One job, one lease, one thread (the scheduler guarantees this)."""

    def __init__(
        self,
        store: Store,
        ledger: Ledger,
        config: Config,
        job: dict,
        stop_flag=None,
        fault_points: tuple[str, ...] = (),
        log=None,
    ):
        self.store = store
        self.ledger = ledger
        self.config = config
        self.job = job
        self.job_id = job["id"]
        self.stop_flag = stop_flag or (lambda: False)
        self.faults = FaultInjector(fault_points, sink=self._fault_sink)
        self._log = log or self._default_log
        self.planner = Planner(config)
        self.work_dir = config.tmp_dir / self.job_id
        self.work_dir.mkdir(parents=True, exist_ok=True)
        self.plan: Plan | None = None
        self._last_heartbeat = 0.0
        self._bytes_read = 0
        # Merge bookkeeping for stats and resume checkpoints. ``_started_runs``
        # is the run count before the first merge pass; ``_merge_passes_done``
        # counts completed generations of this attempt.
        self._started_runs = 0
        self._merge_passes_done = 0
        # Names of intermediate merge files must be unique per attempt: a
        # resumed pass re-derives its chunks from the checkpointed run set, and
        # deterministic names would overwrite a live merge output from the
        # crashed attempt. A fresh token makes leftovers harmless orphans that
        # recovery prunes.
        self._attempt_token = new_id("m")[2:]

    # ------------------------------------------------------------- utilities

    def _account_pause(self, exc: QuotaExceeded) -> BudgetPause:
        """Translate a write-time quota failure into a park request.

        ``record_write``/``seal_artifact`` run deep inside a merge loop where
        there is no useful local remedy: the bytes are already on disk and the
        caller cannot shrink them. Treating the quota as a fatal executor error
        (the previous behaviour) burned the job's retry budget and failed the
        job while its sealed runs were still perfectly reusable. Parking keeps
        those runs, returns the unused estimate, and retries later.
        """
        return BudgetPause(
            f"budget exhausted while writing: {exc.reason}",
            requested=getattr(exc, "requested", 0),
            available=getattr(exc, "available", 0),
        )

    def _default_log(self, kind: str, msg: str, **data) -> None:
        self.store.event(self.job_id, kind, msg, **data)

    def _fault_sink(self, name: str) -> None:
        self.store.event(self.job_id, "fault_injected", f"injected {name}", point=name)

    def _check_stop(self) -> None:
        if self.stop_flag():
            raise Cancelled("cancelled")

    def _heartbeat(self, stage: str, progress: float, message: str = "",
                   force: bool = False) -> None:
        now = time.time()
        if not force and now - self._last_heartbeat < 0.3:
            return
        self._last_heartbeat = now
        rss = 0
        try:
            import resource

            rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
        except Exception:
            pass
        self.store.set_heartbeat(
            self.job_id, pid=os.getpid(), stage=stage,
            progress=min(1.0, max(0.0, progress)), message=message,
            rss_bytes=rss, buffer_bytes=self.ledger.memory_used(self.job_id),
        )
        self.store.update_job(self.job_id, stage=stage)

    def _checkpoint(self, stage: str, **state) -> None:
        self.store.set_checkpoint(self.job_id, stage, state)

    def _source(self, path: str | None = None) -> Iterator[bytes]:
        for line in iter_lines(Path(path or self.job["source_path"])):
            self._bytes_read += len(line) + 1
            yield line

    # ---------------------------------------------------------------- public

    def run(self) -> dict:
        job = self.store.get_job(self.job_id)
        if not job:
            raise SpillForgeError(f"job {self.job_id} disappeared")
        self.job = job
        self._ensure_reservation()
        cp = self.store.get_checkpoint(self.job_id)
        stage = cp["stage"] if cp else "planned"
        self._log("job", f"execute start (kind={job['kind']}, stage={stage})")
        try:
            if job["kind"] in ("sort", "dedupe"):
                return self._run_sortlike(stage, cp)
            if job["kind"] == "join":
                return self._run_join(stage, cp)
            raise SpillForgeError(f"unknown job kind {job['kind']}")
        except BudgetInfeasible:
            # Not a park: no future admission can satisfy this plan, so the
            # scheduler must fail the job with the explanation intact.
            raise
        except QuotaExceeded as exc:
            # Any write that outgrows the envelope becomes a park, never a
            # fatal error: the checkpoint already describes what is on disk,
            # so resuming later loses no work.
            raise self._account_pause(exc) from exc

    def _ensure_reservation(self) -> None:
        """Two-phase admission: reserve an estimate before any file exists.

        The estimate is deliberately *conservative*: input + output + one
        rewrite for a possible repartition. Admission charges it against the
        global budget and the job's own share. If it does not fit the job is
        parked (not failed) so the scheduler can try again when capacity frees
        up, which is how a large job waits behind smaller ones without
        starving them.
        """
        job = self.job
        res = self.store.reservation(job["id"], "disk")
        source = int(job["source_bytes"])
        second = int(job["second_bytes"] or 0)
        plan_guess = int((job.get("plan") or {}).get("expected_temp_bytes") or 0)
        estimate = plan_guess or (source + second) * 2 + 8192
        cap = self.ledger.job_cap("disk")
        estimate = min(estimate, cap)
        # Memory is a soft budget: the planner sizes its buffers from the job's
        # memory *cap*, and the reservation only has to make the job visible to
        # the scheduler. Asking for the full cap while admission can only grant
        # ``admit_share`` (cap minus slack, and less under concurrency) would
        # deny every first dispatch on a small memory budget and park a job
        # that has not written a byte. Ask for exactly what can be granted; a
        # zero-memory grant is legal and simply means "run with the minimum
        # buffer", which ``_build_plan`` already clamps.
        memory = max(
            0,
            min(
                self.ledger.job_cap("memory"),
                self.ledger.admit_share("memory", job["id"]),
            ),
        )
        if res and res["amount"] >= estimate:
            return
        if res:
            # A parked job keeps a reservation shrunken to the bytes it still
            # holds. Re-admitting it must only ask for the difference, or every
            # retry would be denied for the full estimate even though the
            # budget already returned most of it.
            try:
                self.ledger.charge(
                    job["id"], "disk", estimate - res["amount"],
                    "re-admission after pause",
                )
                self.ledger.note_memory(job["id"], memory)
            except (QuotaExceeded, BudgetDenied) as exc:
                available = getattr(exc, "available", 0)
                raise BudgetPause(
                    f"re-admission needs {estimate - res['amount']} more bytes: {exc}",
                    requested=estimate, available=res["amount"] + available,
                ) from exc
            self._log(
                "admission",
                f"re-admitted with {human(estimate)} disk / {human(memory)} memory",
                disk=estimate, memory=memory,
            )
            return
        try:
            self.ledger.open_job(
                job["id"], max(estimate, 4096), memory, "admission estimate"
            )
        except (QuotaExceeded, BudgetDenied) as exc:
            available = getattr(exc, "available", 0)
            raise BudgetPause(
                f"admission denied for an estimate of {estimate} bytes: {exc}",
                requested=estimate, available=available,
            ) from exc
        if estimate >= cap and estimate < plan_guess:
            # The plan wanted more than this job is ever allowed to hold. Debt
            # may cover part of the difference later; if not, the write path
            # parks the job rather than overcommitting the volume.
            self._log(
                "admission",
                "plan peak exceeds the per-job cap; debt or parking may follow",
                plan_bytes=plan_guess, cap=cap,
            )
        self._log(
            "admission",
            f"reserved disk {human(estimate)} and memory {human(memory)}",
            disk=estimate, memory=memory,
        )

    # ------------------------------------------------------------------ plan

    def _build_plan(self) -> Plan:
        job = self.job
        left = sample_file(Path(job["source_path"]))
        mem_cap = self.ledger.job_cap("memory")
        # Plan against the job's *cap*, not its current reservation: the
        # reservation starts as a conservative admission estimate and may be
        # topped up (cap, then bounded debt) while the job runs. Planning
        # against a stale smaller number would under-size the run buffer and
        # produce a plan the job then has to deviate from.
        disk_cap = self.ledger.job_cap("disk")
        if job["kind"] == "join":
            right = sample_file(Path(job["second_path"]))
            plan = self.planner.plan_join(left, right, mem_cap, disk_cap)
            plan.estimates["left_sample"] = left.to_public()
            plan.estimates["right_sample"] = right.to_public()
        else:
            plan = self.planner.plan_sort(
                left, job["kind"], mem_cap, disk_cap,
                distinct=job["kind"] == "dedupe",
            )
            plan.estimates["sample"] = left.to_public()
        self.plan = plan
        self.store.update_job(self.job_id, plan=plan.to_dict())
        self._log("plan", f"plan={plan.kind}: {plan.reason}", **plan.estimates)
        self._check_plan_feasible(plan)
        return plan

    def _check_plan_feasible(self, plan: Plan) -> None:
        """Fail before writing if the plan can never fit the volume.

        The plan's peak is the high-water mark of live temp bytes. If it is
        larger than the largest grant this job could get with an otherwise
        empty queue, no amount of waiting, parking or debt will make it fit.
        Detecting that here turns a slow, confusing sequence of parks (and a
        volume filled to the brim) into one clear failure.
        """
        needed = int(plan.expected_temp_bytes)
        # Bytes already sealed count toward the peak: a resumed job does not
        # need a *new* grant for what it already holds, so comparing the full
        # peak with the remaining grant alone would reject jobs that are in
        # fact able to finish.
        held = self.ledger._accounted_for(self.job_id)
        ceiling = held + self.ledger.max_grant("disk")
        if needed > ceiling:
            raise BudgetInfeasible(
                self.job_id, "disk", needed, ceiling,
                f"plan {plan.kind} needs {human(needed)} of temp space but the "
                f"largest achievable grant is {human(ceiling)} "
                f"(held {human(held)}, budget {human(self.ledger.budget('disk'))}, "
                f"job cap {human(self.ledger.job_cap('disk'))}, "
                f"debt {human(self.ledger.debt_cap('disk'))}, "
                f"retained results {human(self.ledger.retained())})",
            )

    def _plan_from_job(self) -> Plan | None:
        if self.plan:
            return self.plan
        raw = self.job.get("plan") or {}
        if not raw:
            return None
        try:
            plan = Plan(**raw)
        except TypeError:
            return None
        # The dict round-trip turns tuples into lists; Plan has no tuples, but
        # guard the estimates dict which may contain nested dicts.
        plan.estimates = dict(plan.estimates or {})
        self.plan = plan
        # A resumed job reuses its stored plan; the feasibility check must run
        # for it too, otherwise a job that can never fit would park forever
        # instead of failing once with the exact numbers.
        self._check_plan_feasible(plan)
        return plan

    # ------------------------------------------------------------ sort/dedupe

    def _run_sortlike(self, stage: str, cp: dict | None) -> dict:
        if stage == "released":
            # A previous attempt got as far as committing the result and then
            # died (or failed) before the reservation was released. Recomputing
            # would duplicate work and can even fail because the old workspace
            # is gone; finish the publish instead.
            return self._finish_published()
        plan = self._plan_from_job()
        if plan is None or stage in ("planned", "created"):
            plan = self._build_plan()
            self._checkpoint(
                "spilling", plan=plan.to_dict(),
                runs=[], part=0, run_bytes=0, source_offset=0,
                phase="runs",
            )
            cp = self.store.get_checkpoint(self.job_id)
            stage = "spilling"
        if plan.kind == "in_memory":
            return self._in_memory_sort(plan)
        if plan.kind == "streaming":
            return self._streaming_sort(plan)
        # The strategy decides where to resume, not the checkpoint stage name:
        # a repartition merge checkpoint and a run-merge checkpoint share the
        # stage "merging" but have different artifacts.
        if plan.kind == "repartition":
            return self._repartition_sort(plan, cp)
        if stage == "spilling":
            return self._spill_runs(plan, cp)
        if stage == "merging":
            return self._resume_merge(plan, cp)
        if stage == "publishing":
            return self._resume_publish(plan, cp)
        raise SpillForgeError(f"unknown stage {stage}")

    def _run_path(self, part: int) -> Path:
        return self.work_dir / f"run-{part:05d}.srt"

    def _spill_runs(self, plan: Plan, cp: dict | None) -> dict:
        """Stage 1: read the source once, write sorted runs.

        Resumability: the checkpoint stores the *byte offset* of the start of
        the current buffer and the list of sealed runs. A crash rewinds to that
        offset and regenerates only the unfinished buffer, so no partial run is
        ever trusted and no input is skipped.
        """
        state = (cp or {}).get("state", {}) if cp else {}
        runs: list[RunInfo] = [RunInfo(**r) for r in (state.get("runs") or [])]
        part = int(state.get("part") or 0)
        resume_offset = int(state.get("source_offset") or 0)
        records_per_buffer = int(plan.estimates.get("records_per_buffer") or 1)
        # Honour the plan's buffer size exactly: if the executor flushed at a
        # different size, the plan's run count/fan-in would be wrong and the
        # UI would explain a plan the job never followed.
        target_bytes = int(plan.estimates.get("buffer_bytes") or 65536)
        line_iter = iter(self._source())
        if resume_offset:
            skipped = 0
            for line in line_iter:
                skipped += len(line) + 1
                if skipped >= resume_offset:
                    break
            resume_offset = skipped
        buffer: list[bytes] = []
        buf_bytes = 0
        total = max(1, self.job["source_bytes"])
        consumed = resume_offset
        for line in line_iter:
            self._check_stop()
            buffer.append(line)
            buf_bytes += len(line) + 1
            consumed += len(line) + 1
            if len(buffer) >= records_per_buffer or buf_bytes >= target_bytes:
                self._write_run(part, buffer, buf_bytes)
                runs.append(RunInfo(
                    artifact_id=self._last_run_id, path=str(self._run_path(part)),
                    records=len(buffer), bytes=buf_bytes,
                ))
                part += 1
                buffer = []
                buf_bytes = 0
                self._checkpoint(
                    "spilling", plan=plan.to_dict(),
                    runs=[r.__dict__ for r in runs], part=part,
                    source_offset=consumed, run_bytes=target_bytes,
                )
                self._heartbeat(
                    "spilling", 0.05 + 0.5 * consumed / total,
                    f"{len(runs)} runs, {human(consumed)} read",
                )
                self.faults.trip("after_run_complete")
        if buffer:
            self._write_run(part, buffer, buf_bytes)
            runs.append(RunInfo(
                artifact_id=self._last_run_id, path=str(self._run_path(part)),
                records=len(buffer), bytes=buf_bytes,
            ))
            part += 1
        self._heartbeat("spilling", 0.6, f"{len(runs)} runs written", force=True)
        self._checkpoint(
            "merging", plan=plan.to_dict(),
            runs=[r.__dict__ for r in runs], merge_cursor=0,
        )
        return self._merge_runs(plan, runs)

    _last_run_id: str = ""

    def _write_run(self, part: int, records: list[bytes], raw_bytes: int) -> None:
        """Sort one in-memory buffer and write it as a sealed artifact."""
        records.sort()
        path = self._run_path(part)
        try:
            art = self.ledger.create_artifact(
                self.job_id, path, "run", max(raw_bytes, 1),
                meta={"part": part, "records": len(records)},
            )
        except QuotaExceeded as exc:
            raise BudgetPause(
                f"run {part} needs {raw_bytes} bytes: {exc}",
                requested=raw_bytes, available=exc.available,
            ) from exc
        pending_bytes = 0
        pending_records = 0
        try:
            with open(path, "wb") as fh:
                for i, rec in enumerate(records):
                    fh.write(rec)
                    fh.write(b"\n")
                    pending_bytes += len(rec) + 1
                    pending_records += 1
                    if pending_bytes >= 64 * 1024:
                        self.ledger.record_write(art["id"], pending_bytes, pending_records)
                        pending_bytes = 0
                        pending_records = 0
                        self._check_stop()
                if pending_bytes:
                    self.ledger.record_write(art["id"], pending_bytes, pending_records)
        except BaseException:
            # Leave the row 'open'. Recovery deletes unreferenced open
            # artifacts; nothing else can observe this file.
            raise
        sealed = self.ledger.seal_artifact(art["id"], records=len(records))
        self._last_run_id = art["id"]
        self._log(
            "spill", f"run {part}: {len(records)} records, {human(sealed['accounted_bytes'])}",
            artifact_id=art["id"], part=part,
        )

    def _open_runs(self, runs: list[RunInfo]):
        """Open the given runs (at most ``fan_in``), pin them, push first lines.

        Each input gets a ``merge_input`` reference before it is opened. Without
        it a concurrent cancel/reclaim (or the recovery pass) sees a zero-ref
        run and may unlink it while the merge is reading it — the exact "delete
        an intermediate another task is still reading" failure the ledger is
        supposed to make impossible. The caller must drop these refs when the
        merge ends; ``_release_run_refs`` does that in a ``finally``.
        """
        heap: list[tuple[bytes, int]] = []
        handles: list[BinaryIO] = []
        pinned: list[str] = []
        try:
            for idx, run in enumerate(runs):
                self.ledger.add_ref(run.artifact_id, "merge_input", self.job_id)
                pinned.append(run.artifact_id)
                fh = open(run.path, "rb")
                handles.append(fh)
                line = fh.readline()
                if line:
                    heapq.heappush(heap, (line.rstrip(b"\n"), idx))
        except BaseException:
            for fh in handles:
                fh.close()
            self._release_run_refs(pinned)
            raise
        return heap, handles, pinned

    def _release_run_refs(self, artifact_ids: list[str]) -> None:
        for art_id in artifact_ids:
            try:
                self.ledger.drop_ref(art_id, "merge_input", self.job_id)
            except Exception as exc:  # pragma: no cover - defensive
                self._log("warn", f"could not drop merge ref on {art_id}: {exc}")

    def _merge_runs(self, plan: Plan, runs: list[RunInfo]) -> dict:
        """Merge the sorted runs into the final result.

        When there are more runs than the plan's fan-in (small memory budget,
        large input), a single heap over every run would ignore the plan and
        open hundreds of files at once. Instead we merge in *generations* of at
        most ``fan_in`` inputs, write each merged generation as intermediate
        artifacts, and repeat until one generation remains. Each chunk within a
        generation is checkpointed, so a crash resumes from the current run set
        rather than redoing the whole merge.
        """
        fan_in = max(2, int(plan.fan_in or 2))
        if not self._started_runs:
            self._started_runs = len(runs)
        generation = self._merge_passes_done
        planned = max(1, int(plan.estimates.get("merge_passes") or 1))
        while len(runs) > fan_in:
            # Map each generation onto the 0.6–0.95 band the progress bar uses
            # for merging, so an intermediate pass does not visually reset the
            # job to zero on every chunk.
            base = 0.6 + 0.3 * min(1.0, generation / planned)
            span = 0.3 / planned
            nxt = self._merge_generation(plan, runs, fan_in, generation, base, span)
            if len(nxt) >= len(runs):
                raise SpillForgeError(
                    f"merge made no progress: {len(runs)} runs with fan-in {fan_in}"
                )
            runs = nxt
            generation += 1
            self._merge_passes_done += 1
        return self._merge_final(plan, runs)

    def _merge_generation(self, plan: Plan, runs: list[RunInfo], fan_in: int,
                          generation: int, progress_base: float = 0.0,
                          progress_span: float = 0.0) -> list[RunInfo]:
        """One intermediate merge pass; returns the runs for the next pass.

        The chunks are cut from an immutable snapshot: replacing entries of a
        list while iterating over indices computed from its original length
        would skip runs (the list shrinks as chunks are merged), silently
        dropping records from the result.
        """
        chunks = [runs[i:i + fan_in] for i in range(0, len(runs), fan_in)]
        next_runs: list[RunInfo] = []
        for index, chunk in enumerate(chunks):
            if len(chunk) == 1:
                # A lone trailing run needs no rewrite; it joins the next pass.
                next_runs.append(chunk[0])
                continue
            path = self.work_dir / f"merge-{self._attempt_token}-g{generation}-{index:04d}.srt"
            expected = max(4096, sum(r.bytes for r in chunk))
            try:
                art = self.ledger.create_artifact(
                    self.job_id, path, "merge_tmp", expected,
                    meta={"generation": generation, "chunk": index},
                )
            except QuotaExceeded as exc:
                raise BudgetPause(
                    f"intermediate merge needs {expected} bytes: {exc}",
                    requested=expected, available=exc.available,
                ) from exc
            # Everything still on disk while this chunk is being written: the
            # outputs already committed, this chunk's inputs, and the chunks
            # not yet processed. Checkpointing only the chunk would make
            # recovery treat the rest as unreferenced and prune live data.
            live = (next_runs + chunk
                    + [r for rest in chunks[index + 1:] for r in rest])
            emitted = self._kway_merge(
                plan, chunk, art, path, plan.job_kind == "dedupe",
                progress_base=progress_base + progress_span * index / len(chunks),
                progress_span=progress_span / len(chunks),
                resume_runs=live, final=False,
            )
            sealed = self.ledger.seal_artifact(art["id"], records=emitted)
            merged = RunInfo(
                artifact_id=art["id"], path=str(path),
                records=emitted, bytes=int(sealed["accounted_bytes"]),
            )
            # Commit the new run set *before* deleting the consumed inputs: a
            # crash in between leaves extra complete files that recovery prunes,
            # whereas the opposite order could leave the checkpoint pointing at
            # files that no longer exist.
            next_runs.append(merged)
            self._checkpoint(
                "merging", plan=plan.to_dict(),
                runs=[r.__dict__ for r in next_runs]
                     + [r.__dict__ for rest in chunks[index + 1:] for r in rest],
                merge_generation=generation, merge_final=False,
                initial_runs=self._started_runs,
            )
            for consumed in chunk:
                try:
                    self.ledger.delete_artifact(
                        consumed.artifact_id,
                        f"merged into generation {generation + 1}",
                    )
                except Exception as exc:  # pragma: no cover - defensive
                    self._log("warn", f"could not delete run {consumed.artifact_id}: {exc}")
        return next_runs

    def _merge_final(self, plan: Plan, runs: list[RunInfo]) -> dict:
        job = self.job
        out_path = self.work_dir / "result.srt"
        output_estimate = max(
            4096, int(plan.estimates.get("output_bytes") or job["source_bytes"])
        )
        try:
            out_art = self.ledger.create_artifact(
                self.job_id, out_path, "result", output_estimate
            )
        except QuotaExceeded as exc:
            raise BudgetPause(
                f"result file needs {output_estimate} bytes: {exc}",
                requested=output_estimate, available=exc.available,
            ) from exc
        # Publish a reference to the output before opening inputs so recovery
        # can distinguish "a merge was in progress" from "a scratch file leaked".
        with self.store.tx() as cur:
            cur.execute(
                "INSERT OR IGNORE INTO artifact_refs(artifact_id,owner_kind,owner_id,created_at) "
                "VALUES(?, 'merge_output', ?, ?)",
                (out_art["id"], self.job_id, time.time()),
            )
        self._checkpoint(
            "merging", plan=plan.to_dict(),
            runs=[r.__dict__ for r in runs], merge_cursor=0,
            out_artifact=out_art["id"], merge_final=True,
            initial_runs=self._started_runs,
            merge_generation=self._merge_passes_done,
        )
        emitted = self._kway_merge(
            plan, runs, out_art, out_path, plan.job_kind == "dedupe",
            progress_base=0.9, progress_span=0.09,
        )
        sealed = self.ledger.seal_artifact(out_art["id"], records=emitted)
        self._checkpoint(
            "publishing", plan=plan.to_dict(),
            runs=[r.__dict__ for r in runs], out_artifact=out_art["id"],
            emitted=emitted, initial_runs=self._started_runs,
            # Generations completed *before* the final pass; ``_publish`` adds
            # one for the final pass, so a resume must not pre-add it too.
            merge_generation=self._merge_passes_done,
        )
        return self._publish(plan, sealed, runs, emitted)

    def _kway_merge(self, plan: Plan, runs: list[RunInfo], out_art: dict,
                    out_path: Path, dedupe: bool,
                    progress_base: float, progress_span: float,
                    resume_runs: list[RunInfo] | None = None,
                    final: bool = True) -> int:
        """Heap-merge at most ``fan_in`` runs into ``out_art``.

        ``resume_runs`` is the run set a checkpoint must preserve while this
        chunk is being written. For an intermediate generation the chunk is
        only a slice of the live set, so checkpointing just the chunk would
        make recovery prune every run outside it — i.e. lose data. The caller
        therefore passes the full set; the final pass defaults to ``runs``.
        """
        resume_runs = runs if resume_runs is None else resume_runs
        total_in = sum(r.records for r in runs) or 1
        emitted = 0
        last: bytes | None = None
        pending_bytes = 0
        pending_records = 0
        cursor = 0
        heap, handles, pinned = self._open_runs(runs)
        try:
            with open(out_path, "wb") as out:
                while heap:
                    line, idx = heapq.heappop(heap)
                    self._check_stop()
                    if dedupe and line == last:
                        pass
                    else:
                        out.write(line)
                        out.write(b"\n")
                        last = line
                        emitted += 1
                        pending_bytes += len(line) + 1
                        pending_records += 1
                    cursor += 1
                    if pending_bytes >= 64 * 1024:
                        self.ledger.record_write(
                            out_art["id"], pending_bytes, pending_records
                        )
                        pending_bytes = 0
                        pending_records = 0
                        self._heartbeat(
                            "merging",
                            progress_base + progress_span * min(1.0, cursor / total_in),
                            f"merged {emitted} records",
                        )
                        self._checkpoint(
                            "merging", plan=plan.to_dict(),
                            runs=[r.__dict__ for r in resume_runs],
                            merge_cursor=cursor, out_artifact=out_art["id"],
                            merge_final=final,
                            initial_runs=self._started_runs,
                            merge_generation=self._merge_passes_done,
                        )
                        self.faults.trip("mid_merge")
                    nxt = handles[idx].readline()
                    if nxt:
                        heapq.heappush(heap, (nxt.rstrip(b"\n"), idx))
                if pending_bytes:
                    self.ledger.record_write(out_art["id"], pending_bytes, pending_records)
        finally:
            for fh in handles:
                fh.close()
            # Inputs are fully consumed here; only now may a reclaim unlink
            # them, and the caller deletes them once the result is committed.
            self._release_run_refs(pinned)
        return emitted

    def _resume_merge(self, plan: Plan, cp: dict) -> dict:
        state = (cp or {}).get("state", {})
        runs = [RunInfo(**r) for r in state.get("runs", [])]
        # Restore merge bookkeeping so the resumed attempt reports and
        # checkpoints the same generation numbering. ``initial_runs`` is the
        # spill output count; the checkpointed run list is the *current* set,
        # which a partially completed generation may already have reduced.
        self._started_runs = int(state.get("initial_runs") or len(runs))
        self._merge_passes_done = int(
            state.get("merge_generation") or state.get("merge_passes_done") or 0
        )
        out_art_id = state.get("out_artifact")
        merge_final = bool(state.get("merge_final", True))
        run_ids = {r.artifact_id for r in runs}
        if out_art_id:
            art = self.store.q1("SELECT * FROM artifacts WHERE id=?", (out_art_id,))
            if art and art["state"] == "complete" and merge_final:
                # The *final* merge finished before the crash; publishing is
                # the only thing left and is idempotent.
                return self._publish(
                    plan, self.ledger.get_artifact(out_art_id), runs,
                    int(art["records"] or 0),
                )
            if art and art["state"] in ("open", "complete") and out_art_id not in run_ids:
                # A completed intermediate chunk is not in the checkpoint's run
                # set yet, and its inputs are still on disk: treat it as
                # uncommitted and merge that generation again. Publishing it as
                # the result would silently truncate the job to one chunk.
                # ``out_art_id in run_ids`` would mean the generation commit
                # landed before the crash, so the artifact must be kept.
                self.ledger.delete_artifact(
                    out_art_id,
                    "discard partial merge" if art["state"] == "open"
                    else "discard uncommitted intermediate merge",
                    force=True,
                )
        else:
            # No output artifact was recorded, yet the checkpoint is in the
            # merge stage: a crash landed after an intermediate generation
            # committed its new run set but before the next chunk created its
            # output. Discard every open artifact of this job — a half-written
            # intermediate is worthless, while all completed runs are kept.
            for art in self.store.artifacts(job_id=self.job_id, include_deleted=False):
                if art["state"] == "open":
                    try:
                        self.ledger.delete_artifact(
                            art["id"], "discard partial intermediate merge",
                            force=True,
                        )
                    except Exception as exc:  # pragma: no cover - defensive
                        self._log("warn", f"could not discard {art['id']}: {exc}")
        # A run listed in the checkpoint must still exist. The merge stage
        # deletes an input only *after* committing the replacement run to the
        # checkpoint, so a missing file means the ledger and disk disagree:
        # fail loudly instead of silently producing a short result.
        live = []
        for run in runs:
            if not os.path.exists(run.path):
                art = self.store.q1("SELECT * FROM artifacts WHERE id=?", (run.artifact_id,))
                if art and art["state"] == "deleted":
                    raise SpillForgeError(
                        f"run {run.artifact_id} was reclaimed before the merge "
                        "completed; rerun the job"
                    )
                raise SpillForgeError(f"run file {run.path} vanished")
            live.append(run)
        if not live:
            raise SpillForgeError("merge has no runs")
        return self._merge_runs(plan, live)

    def _resume_publish(self, plan: Plan, cp: dict) -> dict:
        state = (cp or {}).get("state", {})
        out_art_id = state.get("out_artifact")
        if not out_art_id:
            raise SpillForgeError("publishing stage without result artifact")
        out_art = self.ledger.get_artifact(out_art_id)
        runs = [RunInfo(**r) for r in state.get("runs", [])]
        emitted = int(state.get("emitted") or out_art.get("records") or 0)
        self._started_runs = int(state.get("initial_runs") or len(runs))
        self._merge_passes_done = int(state.get("merge_generation") or 0)
        return self._publish(plan, out_art, runs, emitted)

    def _finish_published(self) -> dict:
        """Complete a publish whose cleanup was interrupted.

        The checkpoint stage ``released`` is written *after* the result is
        committed to the job row and marked ``kept``, so the only remaining
        work is dropping the output ref and releasing the reservation. Doing
        it from the checkpoint instead of restarting the job is what makes
        "crash before budget release" cheap.
        """
        cp = self.store.get_checkpoint(self.job_id) or {}
        state = cp.get("state") or {}
        out_art_id = state.get("out_artifact")
        job = self.store.get_job(self.job_id) or self.job
        result_id = job.get("result_artifact_id") or out_art_id
        if not result_id:
            raise SpillForgeError("released stage without a result artifact")
        art = self.store.q1("SELECT * FROM artifacts WHERE id=?", (result_id,))
        if art is None:
            raise SpillForgeError(f"result artifact {result_id} disappeared")
        self.ledger.drop_ref(result_id, "merge_output", self.job_id)
        for other in self.store.artifacts(job_id=self.job_id, include_deleted=False):
            if other["id"] == result_id:
                continue
            try:
                self.ledger.delete_artifact(other["id"], "post-publish cleanup")
            except Exception as exc:  # pragma: no cover - defensive
                self._log("warn", f"could not delete {other['id']}: {exc}")
        if art["state"] != "kept":
            self.ledger.mark_kept(self.job_id, result_id)
        self.ledger.release_job(self.job_id, "publish finished on resume",
                                keep=[result_id])
        self.store.set_checkpoint(
            self.job_id, "released",
            {"out_artifact": result_id, "plan": state.get("plan") or {}},
        )
        self._heartbeat("done", 1.0, "published", force=True)
        return self._finish("succeeded")

    def _publish(self, plan: Plan, out_art: dict, runs: list[RunInfo],
                 emitted: int) -> dict:
        """Make the result queryable and free everything else.

        Idempotent: it can resume after a crash at any point. Refs are dropped
        before deletes so a shared artifact is never freed here.
        """
        job = self.job
        # Last chance to honour a cancel before the result becomes observable.
        # Everything up to here is scratch and cheap to throw away; once the
        # job row carries ``result_path`` the job is committed and a cancel
        # would have to race the download. Checking here makes "cancel" reliable
        # for any job that has not already published.
        self._check_stop()
        # 1. Record the result on the job row. This is what makes the result
        #    observable and is the commit point for "job produced output".
        self.store.update_job(
            self.job_id,
            result_path=out_art["path"],
            result_bytes=int(out_art["accounted_bytes"]),
            result_artifact_id=out_art["id"],
            stats={
                "output_records": emitted,
                "input_bytes": job["source_bytes"],
                "output_bytes": int(out_art["accounted_bytes"]),
                "runs": self._started_runs or len(runs),
                "plan": plan.kind,
                # Generations executed plus the final pass. Counting real
                # passes (not the plan's prediction) is what proves the
                # executor honoured the declared fan-in.
                "merge_passes": self._merge_passes_done + (1 if runs else 0),
                "planned_merge_passes": plan.estimates.get("merge_passes", 0),
            },
        )
        # 2. Detach the output ref; from here the job row owns the artifact.
        self.ledger.drop_ref(out_art["id"], "merge_output", self.job_id)
        # 3. Free the runs and any other scratch of this attempt. Their only
        #    ref is the job's merge; a refusal here means a bug or a shared run,
        #    and we deliberately leave bytes accounted rather than lying about
        #    free space. Sweeping *all* non-result artifacts (not just ``runs``)
        #    matters because an interrupted generation can leave an ``open``
        #    intermediate behind; ``release_job`` would then refuse to release.
        for run in runs:
            try:
                self.ledger.delete_artifact(run.artifact_id, "merge consumed")
            except Exception as exc:  # pragma: no cover - defensive
                self._log("warn", f"could not delete run {run.artifact_id}: {exc}")
        for other in self.store.artifacts(job_id=self.job_id, include_deleted=False):
            if other["id"] == out_art["id"]:
                continue
            try:
                self.ledger.delete_artifact(other["id"], "post-publish cleanup")
            except Exception as exc:  # pragma: no cover - defensive
                self._log("warn", f"could not delete {other['id']}: {exc}")
        # 4. The result is retained outside the budget accounting (kept).
        self.ledger.mark_kept(self.job_id, out_art["id"])
        self._checkpoint(
            "released", plan=plan.to_dict(), out_artifact=out_art["id"]
        )
        self.faults.trip("before_budget_release")
        self.ledger.release_job(self.job_id, "job complete", keep=[out_art["id"]])
        self._heartbeat("done", 1.0, "published", force=True)
        return self._finish("succeeded")

    def _in_memory_sort(self, plan: Plan) -> dict:
        job = self.job
        self._heartbeat("sorting", 0.15, "reading into memory", force=True)
        records: list[bytes] = []
        for line in self._source():
            self._check_stop()
            records.append(line)
        self._checkpoint("merging", plan=plan.to_dict(), runs=[], in_memory=True)
        records.sort()
        if job["kind"] == "dedupe":
            deduped = []
            for line in records:
                if not deduped or deduped[-1] != line:
                    deduped.append(line)
            records = deduped
        out_path = self.work_dir / "result.srt"
        estimate = max(
            4096, int(plan.estimates.get("output_bytes") or sum(len(r) + 1 for r in records))
        )
        try:
            art = self.ledger.create_artifact(
                self.job_id, out_path, "result", estimate
            )
        except QuotaExceeded as exc:
            raise BudgetPause(
                f"result needs {estimate} bytes: {exc}",
                requested=estimate, available=exc.available,
            ) from exc
        self.faults.trip("after_run_complete")
        lines_out = 0
        pending = 0
        with open(out_path, "wb") as fh:
            for line in records:
                fh.write(line)
                fh.write(b"\n")
                lines_out += 1
                pending += len(line) + 1
                if pending >= 64 * 1024:
                    self.ledger.record_write(art["id"], pending, lines_out)
                    pending = 0
                    self._heartbeat(
                        "merging", 0.5 + 0.45 * lines_out / max(1, len(records)),
                        f"wrote {lines_out} records",
                    )
            if pending:
                self.ledger.record_write(art["id"], pending, lines_out)
        self.faults.trip("after_final_write_before_seal")
        sealed = self.ledger.seal_artifact(art["id"], records=lines_out)
        self._checkpoint(
            "publishing", plan=plan.to_dict(), runs=[], out_artifact=art["id"],
            emitted=lines_out,
        )
        return self._publish(plan, sealed, [], lines_out)

    def _streaming_sort(self, plan: Plan) -> dict:
        """Single pass over already-sorted input.

        Only valid when the sample says the input is ordered. The result is
        written straight from the source, so a crash simply restarts the pass;
        there is no intermediate state whose invariants could be violated.
        """
        job = self.job
        self._heartbeat("streaming", 0.1, "streaming sorted input", force=True)
        out_path = self.work_dir / "result.srt"
        estimate = max(4096, int(plan.estimates.get("output_bytes") or job["source_bytes"]))
        try:
            art = self.ledger.create_artifact(
                self.job_id, out_path, "result", estimate
            )
        except QuotaExceeded as exc:
            raise BudgetPause(
                f"result needs {estimate} bytes: {exc}",
                requested=estimate, available=exc.available,
            ) from exc
        last: bytes | None = None
        emitted = 0
        pending = 0
        total = max(1, job["source_bytes"])
        with open(out_path, "wb") as out:
            for line in self._source():
                self._check_stop()
                if plan.job_kind == "dedupe" and line == last:
                    pass
                else:
                    out.write(line)
                    out.write(b"\n")
                    last = line
                    emitted += 1
                    pending += len(line) + 1
                if pending >= 64 * 1024:
                    self.ledger.record_write(art["id"], pending, emitted)
                    pending = 0
                    self._heartbeat(
                        "streaming", 0.1 + 0.85 * self._bytes_read / total,
                        f"streamed {emitted} records",
                    )
                    self.faults.trip("mid_merge")
            if pending:
                self.ledger.record_write(art["id"], pending, emitted)
        self.faults.trip("after_final_write_before_seal")
        sealed = self.ledger.seal_artifact(art["id"], records=emitted)
        self._checkpoint(
            "publishing", plan=plan.to_dict(), runs=[], out_artifact=art["id"],
            emitted=emitted,
        )
        return self._publish(plan, sealed, [], emitted)

    # ------------------------------------------------------------ repartition

    def _repartition_sort(self, plan: Plan, cp: dict | None) -> dict:
        """Bucketed external sort for when one merge pass would blow the cap.

        Scatter hashes whole lines into P buckets (streamed), each bucket is
        sorted in memory, then the P sorted buckets are merged. A hot key is
        split by hash over the full line, so skew does not turn one bucket into
        a second copy of the input. Buckets are sealed and checkpointed one at
        a time; a crash resumes at the first unfinished bucket.
        """
        state = (cp or {}).get("state", {}) if cp else {}
        # Only a checkpoint written by *this* strategy is resumable. A fresh
        # "spilling/runs" checkpoint means scatter has not started yet; trusting
        # it would skip the scatter phase and merge zero buckets.
        if state.get("phase") not in ("scatter", "sort", "merging"):
            state = {}
        partitions = int(plan.partitions or 8)
        buckets: dict[int, dict] = {
            int(k): v for k, v in (state.get("buckets") or {}).items()
        }
        sorted_buckets: dict[int, dict] = {
            int(k): v for k, v in (state.get("sorted") or {}).items()
        }
        phase = state.get("phase", "scatter")
        if phase == "scatter":
            buckets = self._scatter(plan, partitions, buckets)
            self._checkpoint(
                "spilling", plan=plan.to_dict(),
                buckets={str(k): v for k, v in buckets.items()},
                sorted={}, phase="sort",
            )
            state = {"buckets": buckets, "sorted": {}, "phase": "sort"}
            phase = "sort"
            # Every scatter bucket is sealed at this point: the repartition
            # equivalent of "all runs are on disk, merge has not started".
            self.faults.trip("after_run_complete")
        if phase == "merging":
            # Sorted buckets are already durable. Discard any partial result
            # from the interrupted merge and merge again from them; the merge
            # is deterministic, so this is safe and cheap.
            for art in self.store.artifacts(job_id=self.job_id):
                if art["role"] == "result" and art["state"] == "open":
                    self.ledger.delete_artifact(art["id"], "restart bucket merge",
                                                force=True)
            self._checkpoint(
                "merging", plan=plan.to_dict(),
                sorted={str(k): v for k, v in sorted_buckets.items()},
                buckets={str(k): v for k, v in buckets.items()},
                phase="merging", out_artifact=None,
            )
            return self._merge_sorted_buckets(plan, sorted_buckets)
        for idx in sorted(buckets):
            if idx in sorted_buckets:
                continue
            self._heartbeat(
                "sorting_buckets", 0.4 + 0.2 * idx / max(1, partitions),
                f"sorting bucket {idx + 1}/{partitions}",
            )
            sorted_buckets[idx] = self._sort_bucket(plan, idx, buckets[idx])
            self._checkpoint(
                "spilling", plan=plan.to_dict(),
                buckets={str(k): v for k, v in buckets.items()},
                sorted={str(k): v for k, v in sorted_buckets.items()},
                phase="sort",
            )
            self.faults.trip("after_bucket_sorted")
        # Free the scatter copies now that sorted buckets exist. Their refs are
        # only implicit (the checkpoint), so recovery keeps completed scatter
        # buckets referenced by the job until the sorted bucket is sealed.
        for idx, info in sorted_buckets.items():
            scatter = buckets.get(idx)
            if scatter is None or scatter.get("released"):
                continue
            try:
                self.ledger.delete_artifact(scatter["artifact_id"], "sorted bucket durable")
                scatter["released"] = True
            except Exception as exc:
                self._log("warn", f"could not delete scatter bucket {idx}: {exc}")
        self._checkpoint(
            "merging", plan=plan.to_dict(),
            sorted={str(k): v for k, v in sorted_buckets.items()},
            buckets={str(k): v for k, v in buckets.items()},
            phase="merging", out_artifact=None,
        )
        return self._merge_sorted_buckets(plan, sorted_buckets, buckets)

    def _scatter(self, plan: Plan, partitions: int,
                 existing: dict[int, dict]) -> dict[int, dict]:
        """Stream the input into P bucket files."""
        total = max(1, self.job["source_bytes"])
        writers: dict[int, BinaryIO] = {}
        arts: dict[int, dict] = {}
        done = dict(existing)
        pending: dict[int, int] = {i: 0 for i in range(partitions)}
        pending_records: dict[int, int] = {i: 0 for i in range(partitions)}
        try:
            for idx in range(partitions):
                if idx in done:
                    continue
                path = self.work_dir / f"bucket-{idx:03d}.part"
                estimate = max(1, int(total) // partitions)
                arts[idx] = self.ledger.create_artifact(
                    self.job_id, path, "bucket", estimate,
                    meta={"partition": idx, "phase": "scatter"},
                )
                writers[idx] = open(path, "wb")
            consumed = 0
            for line in self._source():
                self._check_stop()
                idx = _bucket_for(line, partitions)
                consumed += len(line) + 1
                writer = writers.get(idx)
                if writer is None:
                    continue  # bucket finished before a resume
                writer.write(line)
                writer.write(b"\n")
                pending[idx] += len(line) + 1
                pending_records[idx] += 1
                if pending[idx] >= 64 * 1024:
                    self.ledger.record_write(
                        arts[idx]["id"], pending[idx], pending_records[idx]
                    )
                    pending[idx] = 0
                    pending_records[idx] = 0
                if consumed % (256 * 1024) < len(line) + 1:
                    self._heartbeat(
                        "scattering", 0.05 + 0.35 * consumed / total,
                        f"scattered {human(consumed)} into {partitions} buckets",
                    )
                    self.faults.trip("during_scatter")
        finally:
            for idx, fh in writers.items():
                fh.flush()
                os.fsync(fh.fileno())
                fh.close()
                if pending[idx]:
                    self.ledger.record_write(
                        arts[idx]["id"], pending[idx], pending_records[idx]
                    )
                sealed = self.ledger.seal_artifact(arts[idx]["id"])
                done[idx] = {
                    "artifact_id": arts[idx]["id"],
                    "path": str(sealed["path"]),
                    "bytes": int(sealed["accounted_bytes"]),
                    "records": int(sealed["records"]),
                }
        return done

    def _sort_bucket(self, plan: Plan, idx: int, info: dict) -> dict:
        records: list[bytes] = []
        with open(info["path"], "rb") as fh:
            for line in fh:
                records.append(line.rstrip(b"\n"))
        records.sort()
        if plan.job_kind == "dedupe":
            deduped = []
            for line in records:
                if not deduped or deduped[-1] != line:
                    deduped.append(line)
            records = deduped
        estimate = max(1, sum(len(r) + 1 for r in records))
        path = self.work_dir / f"bucket-{idx:03d}.srt"
        try:
            art = self.ledger.create_artifact(
                self.job_id, path, "bucket_sorted", estimate,
                meta={"partition": idx, "phase": "sorted"},
            )
        except QuotaExceeded as exc:
            raise BudgetPause(
                f"sorted bucket {idx} needs {estimate} bytes: {exc}",
                requested=estimate, available=exc.available,
            ) from exc
        pending = 0
        with open(path, "wb") as out:
            for line in records:
                out.write(line)
                out.write(b"\n")
                pending += len(line) + 1
                if pending >= 64 * 1024:
                    self.ledger.record_write(art["id"], pending, 0)
                    pending = 0
            if pending:
                self.ledger.record_write(art["id"], pending, 0)
        sealed = self.ledger.seal_artifact(art["id"], records=len(records))
        return {
            "artifact_id": art["id"], "path": str(path),
            "bytes": int(sealed["accounted_bytes"]), "records": len(records),
        }

    def _merge_sorted_buckets(self, plan: Plan, sorted_buckets: dict[int, dict],
                              buckets: dict[int, dict] | None = None) -> dict:
        job = self.job
        out_path = self.work_dir / "result.srt"
        estimate = max(4096, int(plan.estimates.get("output_bytes") or job["source_bytes"]))
        try:
            out_art = self.ledger.create_artifact(
                self.job_id, out_path, "result", estimate
            )
        except QuotaExceeded as exc:
            raise BudgetPause(
                f"result needs {estimate} bytes: {exc}",
                requested=estimate, available=exc.available,
            ) from exc
        order = sorted(sorted_buckets)
        heap: list[tuple[bytes, int]] = []
        handles: list[BinaryIO] = []
        for pos, idx in enumerate(order):
            fh = open(sorted_buckets[idx]["path"], "rb")
            handles.append(fh)
            line = fh.readline()
            if line:
                heapq.heappush(heap, (line.rstrip(b"\n"), pos))
        total = sum(sorted_buckets[i]["records"] for i in order) or 1
        emitted = 0
        last: bytes | None = None
        pending = 0
        try:
            with open(out_path, "wb") as out:
                while heap:
                    line, pos = heapq.heappop(heap)
                    self._check_stop()
                    if plan.job_kind == "dedupe" and line == last:
                        pass
                    else:
                        out.write(line)
                        out.write(b"\n")
                        last = line
                        emitted += 1
                        pending += len(line) + 1
                    if pending >= 64 * 1024:
                        self.ledger.record_write(out_art["id"], pending, emitted)
                        pending = 0
                        self._heartbeat(
                            "merging", 0.6 + 0.35 * emitted / total,
                            f"merged {emitted} records",
                        )
                        self.faults.trip("mid_merge")
                    nxt = handles[pos].readline()
                    if nxt:
                        heapq.heappush(heap, (nxt.rstrip(b"\n"), pos))
                if pending:
                    self.ledger.record_write(out_art["id"], pending, emitted)
        finally:
            for fh in handles:
                fh.close()
        sealed = self.ledger.seal_artifact(out_art["id"], records=emitted)
        self.ledger.mark_kept(self.job_id, out_art["id"])
        self.store.update_job(
            self.job_id,
            result_path=str(out_path),
            result_bytes=int(sealed["accounted_bytes"]),
            result_artifact_id=out_art["id"],
            stats={
                "output_records": emitted,
                "input_bytes": job["source_bytes"],
                "output_bytes": int(sealed["accounted_bytes"]),
                "partitions": len(order),
                "plan": "repartition",
            },
        )
        for info in sorted_buckets.values():
            try:
                self.ledger.delete_artifact(info["artifact_id"], "bucket merged")
            except Exception:
                pass
        if buckets:
            for info in buckets.values():
                if info.get("released"):
                    continue
                try:
                    self.ledger.delete_artifact(info["artifact_id"], "scatter merged")
                except Exception:
                    pass
        self._checkpoint("released", plan=plan.to_dict(), out_artifact=out_art["id"])
        self.faults.trip("before_budget_release")
        self.ledger.release_job(self.job_id, "job complete", keep=[out_art["id"]])
        return self._finish("succeeded")

    # ------------------------------------------------------------------ join

    def _run_join(self, stage: str, cp: dict | None) -> dict:
        plan = self._plan_from_job()
        if plan is None or stage in ("planned", "created"):
            plan = self._build_plan()
            self._checkpoint("joining", plan=plan.to_dict(), phase="build")
        if plan.kind == "hash_join":
            return self._hash_join(plan)
        return self._partitioned_hash_join(plan, cp)

    def _hash_join(self, plan: Plan) -> dict:
        job = self.job
        self._heartbeat("building", 0.1, "building hash table", force=True)
        table: dict[bytes, list[bytes]] = {}
        build_records = 0
        for line in self._source(job["second_path"]):
            self._check_stop()
            table.setdefault(key_of(line), []).append(payload_of(line))
            build_records += 1
        self.ledger.note_memory(
            self.job_id, min(self.ledger.job_cap("memory"), build_records * 96)
        )
        self._checkpoint(
            "probing", plan=plan.to_dict(),
            build_records=build_records, build_keys=len(table),
        )
        out_path = self.work_dir / "result.joined"
        estimate = max(
            4096,
            int(plan.estimates.get("join_bytes", 0))
            or (job["source_bytes"] + job["second_bytes"]),
        )
        try:
            art = self.ledger.create_artifact(
                self.job_id, out_path, "result", estimate
            )
        except QuotaExceeded as exc:
            raise BudgetPause(
                f"join result needs {estimate} bytes: {exc}",
                requested=estimate, available=exc.available,
            ) from exc
        matched = 0
        emitted = 0
        pending = 0
        total = max(1, job["source_bytes"])
        with open(out_path, "wb") as out:
            for line in self._source(job["source_path"]):
                self._check_stop()
                key = key_of(line)
                payloads = table.get(key)
                if not payloads:
                    continue
                matched += 1
                left = payload_of(line)
                for right in payloads:
                    out.write(key)
                    out.write(b"\t")
                    out.write(left)
                    out.write(b"\t")
                    out.write(right)
                    out.write(b"\n")
                    emitted += 1
                    pending += len(key) + len(left) + len(right) + 3
                if pending >= 64 * 1024:
                    self.ledger.record_write(art["id"], pending, emitted)
                    pending = 0
                self._heartbeat(
                    "probing", 0.1 + 0.85 * self._bytes_read / total,
                    f"emitted {emitted} rows",
                )
            if pending:
                self.ledger.record_write(art["id"], pending, emitted)
        self.faults.trip("after_final_write_before_seal")
        sealed = self.ledger.seal_artifact(art["id"], records=emitted)
        self.ledger.note_memory(self.job_id, 0)
        self.store.update_job(
            self.job_id,
            result_path=str(out_path),
            result_bytes=int(sealed["accounted_bytes"]),
            result_artifact_id=art["id"],
            stats={
                "build_records": build_records,
                "build_keys": len(table),
                "left_matched": matched,
                "output_records": emitted,
                "output_bytes": int(sealed["accounted_bytes"]),
                "plan": "hash_join",
            },
        )
        self.ledger.mark_kept(self.job_id, art["id"])
        self._checkpoint("released", plan=plan.to_dict(), out_artifact=art["id"])
        self.faults.trip("before_budget_release")
        self.ledger.release_job(self.job_id, "job complete", keep=[art["id"]])
        return self._finish("succeeded")

    def _partitioned_hash_join(self, plan: Plan, cp: dict | None) -> dict:
        state = (cp or {}).get("state", {}) if cp else {}
        partitions = int(plan.partitions or 8)
        entries = {int(k): v for k, v in (state.get("buckets") or {}).items()}
        if state.get("phase") != "join":
            entries = self._partition_inputs(plan, partitions, entries)
            self._checkpoint(
                "joining", plan=plan.to_dict(), phase="join",
                buckets={str(k): v for k, v in entries.items()},
            )
        else:
            self._checkpoint(
                "joining", plan=plan.to_dict(), phase="join",
                buckets={str(k): v for k, v in entries.items()},
            )
        out_path = self.work_dir / "result.joined"
        # The result of a join can be much larger than either input (duplicate
        # keys multiply). Use the planner's row-level estimate; falling back to
        # the inputs would under-reserve exactly the case that expands most.
        estimate = max(
            4096,
            int(plan.estimates.get("join_bytes", 0))
            or (
                int(plan.estimates.get("build_bytes", 0))
                + int(plan.estimates.get("probe_bytes", 0))
            ),
        )
        try:
            out_art = self.ledger.create_artifact(
                self.job_id, out_path, "result", estimate
            )
        except QuotaExceeded as exc:
            raise BudgetPause(
                f"join result needs {estimate} bytes: {exc}",
                requested=estimate, available=exc.available,
            ) from exc
        emitted = 0
        pending = 0
        total = max(1, self.job["source_bytes"] + self.job["second_bytes"])
        with open(out_path, "wb") as out:
            for idx in sorted(entries):
                entry = entries[idx]
                table: dict[bytes, list[bytes]] = {}
                with open(entry["build"], "rb") as fh:
                    for line in fh:
                        line = line.rstrip(b"\n")
                        if not line:
                            continue
                        self._check_stop()
                        table.setdefault(key_of(line), []).append(payload_of(line))
                with open(entry["probe"], "rb") as fh:
                    for line in fh:
                        line = line.rstrip(b"\n")
                        if not line:
                            continue
                        self._check_stop()
                        key = key_of(line)
                        payloads = table.get(key)
                        if not payloads:
                            continue
                        left = payload_of(line)
                        for right in payloads:
                            out.write(key + b"\t" + left + b"\t" + right + b"\n")
                            emitted += 1
                            pending += len(key) + len(left) + len(right) + 3
                        if pending >= 64 * 1024:
                            self.ledger.record_write(out_art["id"], pending, emitted)
                            pending = 0
                self._heartbeat(
                    "joining", 0.4 + 0.55 * idx / max(1, len(entries)),
                    f"joined bucket {idx + 1}/{len(entries)}, {emitted} rows",
                )
                self.faults.trip("mid_join_bucket")
            if pending:
                self.ledger.record_write(out_art["id"], pending, emitted)
        sealed = self.ledger.seal_artifact(out_art["id"], records=emitted)
        for entry in entries.values():
            for art_id in entry.get("artifacts", []):
                try:
                    self.ledger.delete_artifact(art_id, "join partition merged")
                except Exception:
                    pass
        self.ledger.mark_kept(self.job_id, out_art["id"])
        self.store.update_job(
            self.job_id,
            result_path=str(out_path),
            result_bytes=int(sealed["accounted_bytes"]),
            result_artifact_id=out_art["id"],
            stats={
                "output_records": emitted,
                "output_bytes": int(sealed["accounted_bytes"]),
                "partitions": partitions,
                "plan": "hash_join_partitioned",
            },
        )
        self._checkpoint("released", plan=plan.to_dict(), out_artifact=out_art["id"])
        self.faults.trip("before_budget_release")
        self.ledger.release_job(self.job_id, "job complete", keep=[out_art["id"]])
        return self._finish("succeeded")

    def _partition_inputs(self, plan: Plan, partitions: int,
                          existing: dict[int, dict]) -> dict[int, dict]:
        job = self.job
        out: dict[int, dict] = dict(existing)
        specs = [
            ("probe", job["source_path"], "source_bytes"),
            ("build", job["second_path"], "second_bytes"),
        ]
        for side, path, size_field in specs:
            writers: dict[int, BinaryIO] = {}
            arts: dict[int, dict] = {}
            pend: dict[int, int] = {i: 0 for i in range(partitions)}
            try:
                side_estimate = max(1, int(job[size_field]) // partitions)
                for idx in range(partitions):
                    if out.get(idx, {}).get(side):
                        continue
                    p = self.work_dir / f"{side}-{idx:03d}.part"
                    arts[idx] = self.ledger.create_artifact(
                        self.job_id, p, f"{side}_partition",
                        max(1, side_estimate),
                        meta={"side": side, "partition": idx},
                    )
                    writers[idx] = open(p, "wb")
                read = 0
                for line in self._source(path):
                    self._check_stop()
                    idx = _bucket_for(line, partitions, key_only=True)
                    writer = writers.get(idx)
                    if writer is None:
                        continue
                    writer.write(line)
                    writer.write(b"\n")
                    read += len(line) + 1
                    pend[idx] += len(line) + 1
                    if pend[idx] >= 64 * 1024:
                        self.ledger.record_write(arts[idx]["id"], pend[idx], 0)
                        pend[idx] = 0
                    if read % (256 * 1024) < len(line) + 1:
                        self._heartbeat(
                            "partitioning",
                            0.05 + 0.3 * read / max(1, int(job[size_field])),
                            f"partitioning {side}",
                        )
                    self.faults.trip("during_partition")
            finally:
                for idx, fh in writers.items():
                    fh.flush()
                    os.fsync(fh.fileno())
                    fh.close()
                    if pend[idx]:
                        self.ledger.record_write(arts[idx]["id"], pend[idx], 0)
                    sealed = self.ledger.seal_artifact(arts[idx]["id"])
                    out.setdefault(idx, {})[side] = str(sealed["path"])
                    out[idx].setdefault("artifacts", []).append(arts[idx]["id"])
        return out

    # ------------------------------------------------------------------ exit

    def _finish(self, status: str) -> dict:
        self.store.update_job(
            self.job_id, status=status, finished_at=time.time(),
            error=None, stage="done",
        )
        self.store.clear_heartbeat(self.job_id)
        self._log("job", f"finished {status}")
        return self.store.get_job(self.job_id) or self.job


def _bucket_for(line: bytes, partitions: int, key_only: bool = False) -> int:
    """Deterministic bucket for a line.

    Sort/dedupe hash the whole line: any stable split is fine because the
    result is a permutation of all lines. A join must hash **only the key**
    (``key_of``), otherwise ``k<TAB>left`` and ``k<TAB>right`` land in
    different partitions and the pair is never compared.
    """
    data = key_of(line) if key_only else line
    digest = hashlib.blake2b(data, digest_size=8).digest()
    return int.from_bytes(digest, "big") % partitions
