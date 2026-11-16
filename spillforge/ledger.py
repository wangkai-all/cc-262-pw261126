"""The resource ledger.

Invariants
----------
1. Every byte job-owned on disk belongs to an artifact row and to a live
   reservation. The ledger, not the executor, writes ``accounted_bytes``.
2. An artifact may only be unlinked when it has zero references. References are
   transactional: the ref row and the decision to consume the file either both
   commit or neither does, so a crash cannot delete a file another job is about
   to read.
3. Admission is conservative: a job reserves an estimate up front against both
   the global budget and its own share. Running over the estimate is only
   allowed through *bounded debt* taken from capacity that nobody reserved, so
   an under-estimating job cannot starve the queue forever.
4. Every byte count is recomputed from rows inside the same transaction; the
   balances in ``ledger_entries`` are a journal, not the source of truth.

Lifecycle of a job's reservation
--------------------------------
    reserve -> top-up (cap, then debt) -> artifact accounting -> seal
      -> [terminal] -> remove refs -> delete artifacts -> release

``park`` does *not* release: a parked job keeps its reservation so that the
scheduler can resume it without re-racing for capacity. ``unpark`` charges it
again (or leaves it in debt) when the estimate grew while parked.
"""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from .config import Config, free_disk_bytes
from .errors import (
    BudgetDenied,
    ConfigError,
    ProtectedArtifact,
    QuotaExceeded,
)
from .store import Store, new_id


@dataclass(frozen=True)
class Reservation:
    id: str
    job_id: str
    kind: str
    amount: int
    used: int
    debt_granted: int
    state: str


class Ledger:
    def __init__(self, store: Store, config: Config, log=None):
        self.store = store
        self.config = config
        self._log = log or (lambda kind, msg, **data: None)
        self._lock = threading.RLock()
        self.epoch = int(store.meta("ledger_epoch", "0") or 0)

    # ------------------------------------------------------------- capacity

    def budget(self, kind: str) -> int:
        return (
            self.config.disk_budget_bytes
            if kind == "disk"
            else self.config.memory_budget_bytes
        )

    def job_cap(self, kind: str) -> int:
        if kind == "disk":
            return int(self.config.disk_budget_bytes * self.config.max_job_fraction)
        return int(self.config.memory_budget_bytes * self.config.max_job_memory_fraction)

    def debt_cap(self, kind: str) -> int:
        if kind != "disk":
            return 0
        return int(self.config.disk_budget_bytes * self.config.debt_allowance_fraction)

    def reserved(self, kind: str) -> int:
        return self.store.reserved_total(kind)

    def retained(self) -> int:
        """Bytes of published results kept for download.

        Kept artifacts are outside the per-job reservation (the producing job
        has already released), but they are still bytes on the same volume.
        Admission must count them or a series of small jobs could fill the disk
        with results while every reservation reports plenty of room.
        """
        return self.store.retained_total()

    def actual(self, kind: str) -> int:
        return self.store.actual_total(kind)

    def global_gap(self, kind: str) -> int:
        used = self.reserved(kind)
        if kind == "disk":
            used += self.retained()
        return max(0, self.budget(kind) - used)

    def admit_share(self, kind: str, job_id: str | None = None) -> int:
        """The largest *new* reservation that admission will currently grant.

        ``reservation_slack`` keeps a slice of the budget unclaimed so that an
        underestimated job can still spill its way out of trouble. Without it a
        full pipeline of perfectly-sized estimates would leave zero room for
        error and every misestimate would deadlock.
        """
        slack = int(self.budget(kind) * self.config.reservation_slack)
        share = max(0, self.global_gap(kind) - slack)
        cap = self.job_cap(kind)
        if job_id:
            existing = self.store.reservation(job_id, kind)
            if existing:
                cap = max(0, cap + self.debt_cap(kind) - existing["amount"])
        return min(share, cap)

    def pressure(self, kind: str = "disk") -> float:
        budget = self.budget(kind)
        if not budget:
            return 0.0
        actual = self.actual(kind)
        if kind == "disk":
            actual += self.retained()
        return actual / budget

    def max_grant(self, kind: str = "disk") -> int:
        """The most this job could ever be granted, ignoring other jobs.

        Used to fail a job *before* it waits forever: if even an empty queue
        (only retained results pinning space) could not admit its estimate, no
        amount of waiting will help, so the job is rejected with an explanation
        instead of retrying until the volume really fills.
        """
        slack = int(self.budget(kind) * self.config.reservation_slack)
        pinned = self.retained() if kind == "disk" else 0
        room = max(0, self.budget(kind) - pinned - slack)
        return min(room, self.job_cap(kind) + self.debt_cap(kind))

    # ------------------------------------------------------------- lifecycle

    def open_job(self, job_id: str, estimate_bytes: int, estimate_memory: int, reason: str) -> tuple[Reservation, Reservation]:
        """Reserve both budgets for a newly admitted job (or raise)."""
        with self._lock:
            disk = self._reserve(job_id, "disk", estimate_bytes, reason)
            try:
                mem = self._reserve(job_id, "memory", estimate_memory, reason)
            except BaseException:
                self._release_reservation(disk.id, f"{reason}: rollback memory denial")
                raise
            return disk, mem

    def _reserve(self, job_id: str, kind: str, amount: int, reason: str) -> Reservation:
        if amount < 0:
            raise ConfigError("reservation amount must be non-negative")
        existing = self.store.reservation(job_id, kind)
        if existing:
            return self._load(existing["id"])
        allowed = self.admit_share(kind, job_id)
        if amount > allowed:
            raise QuotaExceeded(
                job_id, kind, amount, allowed,
                f"{reason}: estimate exceeds admission share",
            )
        now = time.time()
        res_id = new_id("res")
        tx_id = uuid.uuid4().hex
        with self.store.tx() as cur:
            cur.execute(
                "INSERT INTO reservations(id,job_id,kind,amount,used,state,debt_granted,"
                "created_at,updated_at) VALUES(?,?,?,?,0,'active',0,?,?)",
                (res_id, job_id, kind, amount, now, now),
            )
            self._journal(cur, tx_id, job_id, f"reserve_{kind}", amount, 0, reason)
        self._log("ledger", f"reserve {kind}", job_id=job_id, bytes=amount)
        return self._load(res_id)

    def charge(self, job_id: str, kind: str, delta: int, reason: str) -> Reservation:
        """Grow an existing reservation; cap first, then bounded debt.

        Raises ``QuotaExceeded`` when neither source has room. The caller (the
        executor) must then either flush/merge to free bytes or park the job —
        it must never ignore the exception and keep writing.
        """
        if delta <= 0:
            existing = self.store.reservation(job_id, kind)
            if existing:
                return self._load(existing["id"])
            return self._reserve(job_id, kind, 0, reason)
        with self._lock:
            existing = self.store.reservation(job_id, kind)
            if not existing:
                return self._reserve(job_id, kind, delta, reason)
            cap_room = max(0, self.job_cap(kind) - existing["amount"])
            debt_room = max(0, self.debt_cap(kind) - existing["debt_granted"])
            global_room = self.global_gap(kind)
            from_cap = min(delta, cap_room)
            debt = min(delta - from_cap, debt_room, global_room)
            if from_cap + debt < delta:
                raise QuotaExceeded(
                    job_id, kind, delta, from_cap + debt,
                    f"{reason}: cap={cap_room} debt_room={debt_room} "
                    f"global_room={global_room}",
                )
            tx_id = uuid.uuid4().hex
            with self.store.tx() as cur:
                cur.execute(
                    "UPDATE reservations SET amount=amount+?, debt_granted=debt_granted+?, "
                    "updated_at=? WHERE id=?",
                    (delta, debt, time.time(), existing["id"]),
                )
                self._journal(
                    cur, tx_id, job_id, f"topup_{kind}", delta, 0,
                    f"{reason} (debt={debt})",
                )
            if debt:
                self._log("ledger", "debt granted", job_id=job_id, bytes=debt)
            return self._load(existing["id"])

    def shrink(self, job_id: str, kind: str, new_amount: int, reason: str) -> None:
        """Give capacity back, never counting debt as repayable twice."""
        with self._lock:
            existing = self.store.reservation(job_id, kind)
            if not existing:
                return
            new_amount = max(0, min(new_amount, existing["amount"]))
            debt_returned = min(existing["debt_granted"], existing["amount"] - new_amount)
            tx_id = uuid.uuid4().hex
            with self.store.tx() as cur:
                cur.execute(
                    "UPDATE reservations SET amount=?, debt_granted=?, updated_at=? WHERE id=?",
                    (new_amount, existing["debt_granted"] - debt_returned, time.time(),
                     existing["id"]),
                )
                self._journal(
                    cur, tx_id, job_id, f"shrink_{kind}",
                    new_amount - existing["amount"], 0, reason,
                )

    def note_memory(self, job_id: str, used: int) -> None:
        with self._lock:
            res = self.store.reservation(job_id, "memory")
            if not res:
                return
            with self.store.tx() as cur:
                cur.execute(
                    "UPDATE reservations SET used=?, updated_at=? WHERE id=?",
                    (max(0, used), time.time(), res["id"]),
                )

    def memory_used(self, job_id: str) -> int:
        res = self.store.reservation(job_id, "memory")
        return int(res["used"]) if res else 0

    def _release_reservation(self, res_id: str, reason: str) -> None:
        tx_id = uuid.uuid4().hex
        with self.store.tx() as cur:
            row = cur.execute("SELECT * FROM reservations WHERE id=?", (res_id,)).fetchone()
            if not row or row["state"] != "active":
                return
            cur.execute(
                "UPDATE reservations SET amount=0, used=0, state='released', updated_at=? "
                "WHERE id=?",
                (time.time(), res_id),
            )
            self._journal(
                cur, tx_id, row["job_id"], f"release_{row['kind']}",
                -row["amount"], 0, reason,
            )

    def _journal(self, cur, tx_id, job_id, kind, reserved_delta, actual_delta, reason) -> None:
        # Balances are read with the same cursor so the journal can never show
        # a value that disagrees with the transaction it describes.
        reserved = cur.execute(
            "SELECT COALESCE(SUM(amount),0) FROM reservations "
            "WHERE state='active' AND kind='disk'"
        ).fetchone()[0]
        actual = cur.execute(
            "SELECT COALESCE(SUM(accounted_bytes),0) FROM artifacts "
            "WHERE state NOT IN ('deleted','kept')"
        ).fetchone()[0]
        cur.execute(
            "INSERT INTO ledger_entries(ts,tx_id,job_id,kind,reserved_delta,actual_delta,"
            "reason,balance_reserved,balance_actual) VALUES(?,?,?,?,?,?,?,?,?)",
            (
                time.time(), tx_id, job_id, kind, reserved_delta, actual_delta,
                reason, int(reserved), int(actual),
            ),
        )

    def _load(self, res_id: str) -> Reservation:
        row = self.store.q1("SELECT * FROM reservations WHERE id=?", (res_id,))
        if not row:
            raise ProtectedArtifact(f"reservation {res_id} vanished")
        return Reservation(
            id=row["id"], job_id=row["job_id"], kind=row["kind"],
            amount=row["amount"], used=row["used"],
            debt_granted=row["debt_granted"], state=row["state"],
        )

    # ------------------------------------------------------------- artifacts

    def create_artifact(
        self,
        job_id: str,
        path: Path,
        role: str,
        expected_bytes: int,
        meta: dict | None = None,
        refs: Iterable[tuple[str, str]] = (),
    ) -> dict:
        """Register intent for a file *before* it is created.

        The job's disk reservation is its envelope; artifacts are accounted
        against it and only a *seal* that grows past the envelope triggers a
        top-up (cap, then bounded debt). If even the planned envelope cannot be
        granted, the executor parks the job instead of writing a file it cannot
        account for.

        The caller may re-create an artifact at the same path after a budget
        pause (result files use a fixed name so publishing is idempotent). The
        previous attempt's row is still ``open`` and would keep charging bytes
        for a file the next ``open("wb")`` truncates; a job that pauses 16
        times therefore ends up accounted at 16x its real size and can never be
        re-admitted. Any ``open`` row for the same job and path is retired here
        first, so accounting always counts the bytes of the file that exists.
        """
        art_id = new_id("art")
        tx_id = uuid.uuid4().hex
        stale = self.store.q(
            "SELECT a.id FROM artifacts a WHERE a.job_id=? AND a.path=? "
            "AND a.state='open' AND NOT EXISTS "
            "(SELECT 1 FROM artifact_refs r WHERE r.artifact_id=a.id)",
            (job_id, str(path)),
        )
        for row in stale:
            self.delete_artifact(
                row["id"], f"recreate {role} at the same path"
            )
        with self._lock:
            res = self.store.reservation(job_id, "disk")
            if res is None:
                raise BudgetDenied(job_id, "disk", expected_bytes, 0, "no disk reservation")
            now = time.time()
            with self.store.tx() as cur:
                cur.execute(
                    "INSERT INTO artifacts(id,job_id,reservation_id,path,role,state,"
                    "accounted_bytes,expected_bytes,records,epoch,meta,created_at,updated_at) "
                    "VALUES(?,?,?,?,?,'open',0,?,0,0,?,?,?)",
                    (
                        art_id, job_id, res["id"], str(path), role, expected_bytes,
                        json.dumps(meta or {}, default=str), now, now,
                    ),
                )
                for owner_kind, owner_id in refs:
                    self._add_ref(cur, art_id, owner_kind, owner_id)
                self._journal(cur, tx_id, job_id, "artifact_create", 0, 0,
                              f"create {role} {path.name}")
        return self.get_artifact(art_id)

    def ensure_envelope(self, job_id: str, needed_bytes: int, reason: str) -> None:
        """Grow the job's disk envelope to cover a planned peak.

        Raises ``QuotaExceeded`` when the global cap/debt cannot cover it; the
        executor turns that into a park, never into silent overcommit.
        """
        res = self.store.reservation(job_id, "disk")
        if res is None:
            raise BudgetDenied(job_id, "disk", needed_bytes, 0, "no disk reservation")
        accounted = self._accounted_for(job_id)
        target = max(needed_bytes, accounted)
        if target > res["amount"]:
            self.charge(job_id, "disk", target - res["amount"], reason)

    def add_ref(self, artifact_id: str, owner_kind: str, owner_id: str) -> None:
        with self.store.tx() as cur:
            self._add_ref(cur, artifact_id, owner_kind, owner_id)

    def _add_ref(self, cur, artifact_id: str, owner_kind: str, owner_id: str) -> None:
        cur.execute(
            "INSERT OR IGNORE INTO artifact_refs(artifact_id,owner_kind,owner_id,created_at) "
            "VALUES(?,?,?,?)",
            (artifact_id, owner_kind, owner_id, time.time()),
        )

    def drop_ref(self, artifact_id: str, owner_kind: str, owner_id: str) -> None:
        with self.store.tx() as cur:
            cur.execute(
                "DELETE FROM artifact_refs WHERE artifact_id=? AND owner_kind=? AND owner_id=?",
                (artifact_id, owner_kind, owner_id),
            )

    def get_artifact(self, artifact_id: str) -> dict:
        row = self.store.q1("SELECT * FROM artifacts WHERE id=?", (artifact_id,))
        if not row:
            raise ProtectedArtifact(f"artifact {artifact_id} not found")
        item = dict(row)
        item["refs"] = self.store.artifact_refs(artifact_id)
        try:
            item["meta"] = json.loads(item.get("meta") or "{}")
        except json.JSONDecodeError:
            item["meta"] = {}
        return item

    def record_write(self, artifact_id: str, account_bytes: int, records: int) -> None:
        """Account bytes that were just written to an open artifact.

        The physical write happens before this call (the caller cannot know a
        byte count in advance). We therefore commit the accounting first and
        only then compare it with the envelope; raising inside the transaction
        would roll back the true byte count and make the ledger disagree with
        the file on disk.
        """
        if account_bytes < 0:
            raise ValueError("negative byte count")
        if account_bytes == 0 and records == 0:
            return
        job_id = None
        with self.store.tx() as cur:
            cur.execute(
                "UPDATE artifacts SET accounted_bytes=accounted_bytes+?, "
                "records=records+?, updated_at=? WHERE id=? AND state='open'",
                (account_bytes, records, time.time(), artifact_id),
            )
            if cur.rowcount != 1:
                art = cur.execute(
                    "SELECT state, job_id FROM artifacts WHERE id=?", (artifact_id,)
                ).fetchone()
                if art and art["state"] in ("complete", "kept"):
                    # Late accounting after seal is reconciled from stat() on
                    # seal, so ignoring it is correct.
                    return
                raise ProtectedArtifact(
                    f"artifact {artifact_id} is not open; refusing to account bytes"
                )
            job_id = cur.execute(
                "SELECT job_id FROM artifacts WHERE id=?", (artifact_id,)
            ).fetchone()["job_id"]
            reserved = cur.execute(
                "SELECT amount FROM reservations WHERE job_id=? AND kind='disk'",
                (job_id,),
            ).fetchone()
            accounted = cur.execute(
                "SELECT COALESCE(SUM(accounted_bytes),0) FROM artifacts "
                "WHERE job_id=? AND state NOT IN ('deleted','kept')",
                (job_id,),
            ).fetchone()[0]
        if reserved and accounted > reserved[0]:
            # The job has outgrown its envelope while writing. Try the normal
            # top-up path (cap, then bounded debt) so a small estimation error
            # does not destroy an otherwise healthy job.
            try:
                self.charge(job_id, "disk", accounted - reserved[0],
                            "write grew past the reserved envelope")
                return
            except QuotaExceeded as exc:
                raise QuotaExceeded(
                    job_id, "disk", accounted - reserved[0], 0,
                    f"write exceeded the job's disk envelope: {exc.reason}",
                ) from exc

    def seal_artifact(self, artifact_id: str, records: int | None = None) -> dict:
        art = self.get_artifact(artifact_id)
        if art["state"] != "open":
            return art
        actual = art["accounted_bytes"]
        try:
            actual = os.stat(art["path"]).st_size
        except OSError:
            pass
        # A sealed file may be larger than what was reserved. Charge the growth
        # here where the caller can still react. If neither the cap nor the
        # debt can cover it, the file is *kept* and its true size recorded: it
        # is already on disk, so deleting it would throw away a run the job
        # could still merge, and under-reporting it would make the ledger lie.
        # The executor turns the exception into a park; the park path charges
        # or reclaims the overage before the job retries.
        res = self.store.reservation(art["job_id"], "disk")
        if res:
            unused = max(0, res["amount"] - self._accounted_for(art["job_id"]))
            growth = max(0, actual - art["accounted_bytes"])
            if growth > unused:
                try:
                    self.charge(art["job_id"], "disk", growth - unused,
                                f"seal growth {Path(art['path']).name}")
                except QuotaExceeded:
                    with self.store.tx() as cur:
                        cur.execute(
                            "UPDATE artifacts SET accounted_bytes=?, updated_at=? WHERE id=?",
                            (actual, time.time(), artifact_id),
                        )
                    raise QuotaExceeded(
                        art["job_id"], "disk", growth, unused,
                        "artifact grew past its reservation while sealing",
                    )
        with self.store.tx() as cur:
            cur.execute(
                "UPDATE artifacts SET state='complete', accounted_bytes=?, "
                "records=COALESCE(?, records), updated_at=? WHERE id=?",
                (actual, records, time.time(), artifact_id),
            )
        return self.get_artifact(artifact_id)

    def _accounted_for(self, job_id: str) -> int:
        value = self.store.scalar(
            "SELECT COALESCE(SUM(accounted_bytes),0) FROM artifacts "
            "WHERE job_id=? AND state NOT IN ('deleted','kept')",
            (job_id,),
        )
        return int(value or 0)

    def unreserved_actual(self, job_id: str) -> int:
        """Bytes the job has on disk beyond its current reservation.

        Normally zero. After a crash or a failed top-up it can be positive;
        recovery must charge or release those bytes before the job resumes.
        """
        res = self.store.reservation(job_id, "disk")
        reserved = res["amount"] if res else 0
        return max(0, self._accounted_for(job_id) - reserved)

    def delete_artifact(self, artifact_id: str, reason: str, force: bool = False) -> bool:
        """Delete a file whose reference count is zero.

        Ordering: refs are checked *inside* the transaction that marks the row
        ``reclaiming``; unlink then happens outside SQLite. If the process dies
        between the two, the next reconcile finds an existing ``reclaiming``
        row and finishes the unlink. If the file is already gone, the row still
        settles to ``deleted`` so accounting converges.
        """
        art = self.get_artifact(artifact_id)
        with self._lock:
            with self.store.tx() as cur:
                cur.execute(
                    "SELECT COUNT(*) AS n FROM artifact_refs WHERE artifact_id=?",
                    (artifact_id,),
                )
                n = cur.fetchone()["n"]
                if n and not force:
                    raise ProtectedArtifact(
                        f"artifact {artifact_id} still has {n} references"
                    )
                if art["state"] == "deleted":
                    return False
                cur.execute(
                    "UPDATE artifacts SET state='reclaiming', updated_at=? WHERE id=?",
                    (time.time(), artifact_id),
                )
            try:
                os.unlink(art["path"])
            except FileNotFoundError:
                pass
            except OSError as exc:
                self._log("ledger", "unlink failed", artifact=artifact_id, error=str(exc))
                return False
            tx_id = uuid.uuid4().hex
            with self.store.tx() as cur:
                cur.execute(
                    "UPDATE artifacts SET state='deleted', accounted_bytes=0, "
                    "updated_at=? WHERE id=?",
                    (time.time(), artifact_id),
                )
                # Kept results are not part of the workspace accounting, so the
                # actual delta is only the job-owned bytes that were released.
                actual_delta = -art["accounted_bytes"] if art["state"] != "kept" else 0
                self._journal(
                    cur, tx_id, art["job_id"], "artifact_delete", 0,
                    actual_delta, reason,
                )
            return True

    def mark_kept(self, job_id: str, artifact_id: str) -> None:
        art = self.get_artifact(artifact_id)
        if art["job_id"] != job_id:
            raise ProtectedArtifact("artifact does not belong to job")
        with self.store.tx() as cur:
            cur.execute(
                "UPDATE artifacts SET state='kept', updated_at=? WHERE id=?",
                (time.time(), artifact_id),
            )

    # -------------------------------------------------------------- terminal

    def release_job(self, job_id: str, reason: str, keep: Iterable[str] = ()) -> None:
        """Release the job's reservations after its artifacts are terminal.

        Deliberately refuses to release while non-deleted job artifacts remain
        unless they are in ``keep``; this is the check that catches a cancelled
        job that forgot to clean a run file. Callers with leftovers are bugs,
        and surfacing the exception is safer than silently leaking bytes.
        """
        keep_set = {a for a in keep}
        with self._lock:
            rows = self.store.artifacts(job_id=job_id)
            leftovers = [
                a for a in rows
                if a["state"] not in ("deleted", "kept") and a["id"] not in keep_set
            ]
            if leftovers:
                raise ProtectedArtifact(
                    f"job {job_id}: {len(leftovers)} artifacts not reclaimed: "
                    + ", ".join(a["id"] for a in leftovers[:4])
                )
            for kind in ("disk", "memory"):
                res = self.store.reservation(job_id, kind)
                if res:
                    self._release_reservation(res["id"], reason)
            self.store.clear_checkpoint(job_id)
            self.store.clear_heartbeat(job_id)

    # ---------------------------------------------------------------- stats

    def snapshot(self) -> dict:
        store = self.store
        try:
            free = free_disk_bytes(self.config.data_dir)
        except OSError:
            free = 0
        disk_budget = self.config.disk_budget_bytes
        mem_budget = self.config.memory_budget_bytes
        disk_actual = store.actual_total("disk")
        disk_reserved = store.reserved_total("disk")
        kept = store.retained_total()
        return {
            "disk": {
                "budget": disk_budget,
                "reserved": disk_reserved,
                "actual": disk_actual,
                "kept": kept,
                # Everything the budget has already handed out: live scratch
                # reservations plus results held for download. Admission uses
                # the same sum, so this is the number the UI must explain.
                "committed": disk_reserved + kept,
                "headroom": self.config.disk_headroom_bytes,
                "free_on_volume": free,
                "pressure": (disk_actual + kept) / disk_budget if disk_budget else 0.0,
                "scratch_pressure": disk_actual / disk_budget if disk_budget else 0.0,
                "admission_share": self.admit_share("disk"),
                "uncommitted": max(0, disk_reserved - disk_actual),
            },
            "memory": {
                "budget": mem_budget,
                "reserved": store.reserved_total("memory"),
                "actual": store.actual_total("memory"),
                "admission_share": self.admit_share("memory"),
            },
            "artifacts": len(store.artifacts(include_deleted=False)),
            "epoch": self.epoch,
        }

    def reconcile_disk(self) -> dict:
        """Make ``accounted_bytes`` match reality for every non-deleted file.

        Called by the reaper before it reclaims anything. Handles:
          * a crashed writer whose last chunk was written but not accounted;
          * a file truncated by an external actor;
          * a ``reclaiming`` row whose unlink never happened.
        Returns a summary so the HTTP endpoint can show self-healing activity.
        """
        drifted = 0
        missing = 0
        finished = 0
        for art in self.store.artifacts(include_deleted=False):
            if art["state"] == "reclaiming":
                try:
                    os.unlink(art["path"])
                except FileNotFoundError:
                    pass
                except OSError:
                    continue
                with self.store.tx() as cur:
                    cur.execute(
                        "UPDATE artifacts SET state='deleted', accounted_bytes=0, "
                        "updated_at=? WHERE id=?",
                        (time.time(), art["id"]),
                    )
                finished += 1
                continue
            try:
                size = os.stat(art["path"]).st_size
            except FileNotFoundError:
                # A complete artifact's file vanished under us: it must not keep
                # consuming budget or a job behind it will be denied forever.
                with self.store.tx() as cur:
                    cur.execute(
                        "UPDATE artifacts SET state='deleted', accounted_bytes=0, "
                        "updated_at=? WHERE id=?",
                        (time.time(), art["id"]),
                    )
                self.store.event(
                    art["job_id"], "artifact_missing",
                    f"file vanished: {art['path']}", artifact_id=art["id"],
                )
                missing += 1
                continue
            if size != art["accounted_bytes"]:
                with self.store.tx() as cur:
                    cur.execute(
                        "UPDATE artifacts SET accounted_bytes=?, updated_at=? WHERE id=?",
                        (size, time.time(), art["id"]),
                    )
                self.store.event(
                    art["job_id"], "artifact_reconciled",
                    f"accounted {art['accounted_bytes']} -> {size}",
                    artifact_id=art["id"],
                )
                drifted += 1
        if drifted or missing or finished:
            self._log(
                "ledger", "reconciled artifacts",
                drifted=drifted, missing=missing, finished_reclaim=finished,
            )
        return {"drifted": drifted, "missing": missing, "finished_reclaim": finished}

    def bump_epoch(self) -> int:
        with self.store.tx() as cur:
            cur.execute(
                "INSERT INTO meta(key,value) VALUES('ledger_epoch','1') "
                "ON CONFLICT(key) DO UPDATE SET value=CAST(CAST(value AS INTEGER)+1 AS TEXT)"
            )
        self.epoch = int(self.store.meta("ledger_epoch", "0") or 0)
        return self.epoch
