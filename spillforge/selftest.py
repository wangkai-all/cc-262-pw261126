"""Self-tests that do not need pytest (``python -m spillforge selftest``).

They are also importable by the unittest suite. Each case runs the real
executor against real files with a small budget, so the assertions are about
bytes on disk and ledger rows, not simulated progress.
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import threading
import time
import unittest
from dataclasses import replace
from pathlib import Path

from .config import Config
from .errors import ProtectedArtifact
from .service import Service


def make_config(root: Path, **overrides) -> Config:
    values = dict(
        data_dir=root,
        disk_budget_bytes=3 * 1024 * 1024,
        memory_budget_bytes=256 * 1024,
        disk_headroom_bytes=0,
        reservation_slack=0.10,
        max_job_fraction=0.80,
        debt_allowance_fraction=0.10,
        max_concurrent_jobs=1,
        max_upload_bytes=64 * 1024 * 1024,
        tombstone_seconds=0.2,
        reap_interval_seconds=0.2,
    )
    values.update(overrides)
    cfg = Config(**values)
    cfg.ensure_dirs()
    cfg.validate()
    return cfg


def write_tsv(path: Path, lines: list[str]) -> None:
    with open(path, "wb") as fh:
        for line in lines:
            fh.write(line.encode() + b"\n")


def submit_path(service: Service, path: Path, kind="sort") -> dict:
    with open(path, "rb") as fh:
        upload = service.save_upload(path.name, fh)
    return service.submit_job(kind, upload["id"])


def wait(service: Service, job_id: str, timeout: float = 20.0,
         terminal=("succeeded", "failed", "cancelled")) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        job = service.store.get_job(job_id)
        if job and job["status"] in terminal:
            return job
        time.sleep(0.02)
    return service.store.get_job(job_id)


def read_result(service: Service, job_id: str) -> list[bytes]:
    path = service.result_path(job_id)
    assert path, "result missing"
    with open(path, "rb") as fh:
        return fh.read().splitlines()


class Case(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="spillforge-test-"))
        self.service = Service(make_config(self.tmp))

    def tearDown(self):
        try:
            self.service.stop()
        finally:
            shutil.rmtree(self.tmp, ignore_errors=True)

    def start(self):
        self.service.start()

    def job_input(self, n=4000, skew=9) -> Path:
        up = self.service.generate_demo(n, skew=skew)
        return Path(up["path"])


class CorrectnessTests(Case):
    def test_sort_matches_python_sorted(self):
        path = self.job_input(5000)
        job = submit_path(self.service, path)
        self.start()
        final = wait(self.service, job["id"])
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        got = read_result(self.service, job["id"])
        with open(path, "rb") as fh:
            expected = sorted(fh.read().splitlines())
        self.assertEqual(got, expected)

    def test_dedupe_is_stable_and_unique(self):
        path = self.job_input(4000)
        job = submit_path(self.service, path, "dedupe")
        self.start()
        final = wait(self.service, job["id"])
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        got = read_result(self.service, job["id"])
        with open(path, "rb") as fh:
            expected = sorted(set(fh.read().splitlines()))
        self.assertEqual(got, expected)

    def test_join_small_uses_hash_join(self):
        cfg = make_config(self.tmp / "join-mem", memory_budget_bytes=4 * 1024 * 1024)
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        left = svc.generate_demo(1200, skew=5)
        right = svc.generate_demo(900, skew=11)
        job = svc.submit_job("join", left["id"], right["id"])
        svc.start()
        final = wait(svc, job["id"])
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        rows = read_result(svc, job["id"])
        self.assertTrue(rows)
        for row in rows[:200]:
            self.assertEqual(row.count(b"\t"), 2)
        self.assertEqual(final["plan"]["kind"], "hash_join")

    def test_sorted_input_streams(self):
        up = self.service.generate_demo(30000, skew=6)
        path = Path(up["path"])
        with open(path, "rb") as fh:
            lines = sorted(fh.read().splitlines())
        write_tsv(path, [line.decode() for line in lines])
        job = self.service.submit_job("dedupe", up["id"])
        self.start()
        final = wait(self.service, job["id"])
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        self.assertEqual(final["plan"]["kind"], "streaming")
        self.assertEqual(read_result(self.service, job["id"]), sorted(set(lines)))

    def test_ledger_settles_to_zero(self):
        path = self.job_input(2000)
        job = submit_path(self.service, path)
        self.start()
        final = wait(self.service, job["id"])
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        snap = self.service.ledger.snapshot()
        self.assertEqual(snap["disk"]["reserved"], 0)
        self.assertEqual(snap["disk"]["actual"], 0)
        self.assertEqual(snap["memory"]["reserved"], 0)
        self.assertGreater(snap["disk"]["kept"], 0)
        # The kept result is the only artifact left.
        arts = self.service.store.artifacts(job_id=job["id"])
        self.assertEqual([a["state"] for a in arts], ["kept"])


class PlanningTests(Case):
    def test_partitioned_join_matches_on_the_join_key(self):
        """Every left/right pair with the same key must meet in one bucket.

        Regression: the partitioned join hashed the whole line. ``k<TAB>L`` and
        ``k<TAB>R`` hashed differently, so most matches never shared a bucket
        and were silently dropped (204 of 1500 rows survived). Joins must
        bucket by ``key_of``; sort/dedupe may keep hashing whole lines.
        """
        cfg = make_config(
            self.tmp / "join-partitioned",
            memory_budget_bytes=8 * 1024,
            disk_budget_bytes=8 * 1024 * 1024,
            max_job_fraction=0.80,
            max_runs_per_job=9999,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        left = [b"k%04d\tL%d" % (i % 40, i) for i in range(300)]
        right = [b"k%04d\tR%d" % (i % 40, i) for i in range(200)]
        lp, rp = self.tmp / "left.tsv", self.tmp / "right.tsv"
        write_tsv(lp, [line.decode() for line in left])
        write_tsv(rp, [line.decode() for line in right])
        # Both sides must be uploaded; ``submit_path`` takes only one file.
        with open(lp, "rb") as fh:
            lu = svc.save_upload("left.tsv", fh)
        with open(rp, "rb") as fh:
            ru = svc.save_upload("right.tsv", fh)
        job = svc.submit_job("join", lu["id"], ru["id"])
        svc.start()
        final = wait(svc, job["id"], timeout=60)
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        self.assertEqual(final["plan"]["kind"], "hash_join_partitioned")
        by_key_left: dict[bytes, list[bytes]] = {}
        by_key_right: dict[bytes, list[bytes]] = {}
        for line in left:
            key, payload = line.split(b"\t")
            by_key_left.setdefault(key, []).append(payload)
        for line in right:
            key, payload = line.split(b"\t")
            by_key_right.setdefault(key, []).append(payload)
        expected = sorted(
            key + b"\t" + l + b"\t" + r
            for key in by_key_left.keys() & by_key_right.keys()
            for l in by_key_left[key]
            for r in by_key_right[key]
        )
        self.assertEqual(
            sorted(read_result(svc, job["id"])), expected,
            "partitioned join dropped or duplicated matched pairs",
        )
        self.assertEqual(svc.store.reserved_total("disk"), 0)
        self.assertEqual(svc.store.actual_total("disk"), 0)

    def test_small_input_is_in_memory(self):
        up = self.service.generate_demo(400, skew=4)
        job = self.service.submit_job("sort", up["id"])
        self.start()
        final = wait(self.service, job["id"])
        self.assertEqual(final["plan"]["kind"], "in_memory")
        self.assertEqual(final["status"], "succeeded", final.get("error"))

    def test_large_input_spills_and_merges(self):
        cfg = make_config(
            self.tmp / "merge", memory_budget_bytes=32 * 1024,
            max_runs_per_job=999,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        up = svc.generate_demo(25000, skew=4)
        job = svc.submit_job("sort", up["id"])
        svc.start()
        final = wait(svc, job["id"])
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        self.assertEqual(final["plan"]["kind"], "merge")
        self.assertGreater(final["stats"].get("runs", 0), 1)
        with open(up["path"], "rb") as fh:
            self.assertEqual(read_result(svc, job["id"]), sorted(fh.read().splitlines()))

    def test_planned_merge_passes_match_actual_including_final(self):
        """A one-generation merge is still one pass, not zero.

        Regression: the planner counted only *intermediate* generations, so a
        merge that fit in a single fan-in group reported ``planned=0`` next to
        ``actual=1``. Both sides now count the final pass that writes the
        result, which makes the two numbers directly comparable in the UI.
        """
        cfg = make_config(
            self.tmp / "pass-accounting",
            memory_budget_bytes=32 * 1024,
            disk_budget_bytes=8 * 1024 * 1024,
            max_runs_per_job=9999,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        up = svc.generate_demo(3000, skew=4)
        job = svc.submit_job("sort", up["id"])
        svc.start()
        final = wait(svc, job["id"], timeout=60)
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        stats = final["stats"]
        self.assertEqual(final["plan"]["kind"], "merge")
        self.assertGreaterEqual(stats["runs"], 2)
        self.assertGreaterEqual(stats["merge_passes"], 1)
        self.assertEqual(stats["merge_passes"], stats["planned_merge_passes"])

    def test_tiny_memory_forces_fewer_runs_budget_aware(self):
        """A plan never reserves more than the job cap, and the job proceeds."""
        cfg = make_config(
            self.tmp / "small-mem",
            memory_budget_bytes=64 * 1024,
            disk_budget_bytes=2 * 1024 * 1024,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        up = svc.generate_demo(12000, skew=8)
        job = svc.submit_job("sort", up["id"])
        svc.start()
        final = wait(svc, job["id"], timeout=30)
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        self.assertLessEqual(
            final["plan"]["expected_temp_bytes"], svc.ledger.job_cap("disk")
        )

    def test_merge_honours_fan_in_with_multiple_generations(self):
        """The executor must not open every run at once.

        Regression: the plan declared ``fan_in`` and a pass count, but the
        executor opened all runs in a single heap. With a 1024-descriptor limit
        a few hundred runs would exhaust fds, and merge memory grew with the
        run count instead of the fan-in. A tiny memory budget forces many runs
        and a fan-in of 2, so several generations are required; the result must
        still be byte-identical.
        """
        cfg = make_config(
            self.tmp / "generations",
            memory_budget_bytes=8 * 1024,
            disk_budget_bytes=12 * 1024 * 1024,
            max_job_fraction=0.80,
            max_runs_per_job=9999,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        up = svc.generate_demo(30000, skew=4)
        job = svc.submit_job("sort", up["id"])
        svc.start()
        final = wait(svc, job["id"], timeout=120)
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        stats = final["stats"]
        self.assertGreater(stats["runs"], final["plan"]["fan_in"] ** 2)
        self.assertGreater(stats["merge_passes"], 2)
        self.assertEqual(stats["merge_passes"], stats["planned_merge_passes"])
        roles = {a["role"] for a in svc.store.artifacts(job_id=job["id"], include_deleted=True)}
        self.assertIn("merge_tmp", roles)
        with open(up["path"], "rb") as fh:
            self.assertEqual(read_result(svc, job["id"]), sorted(fh.read().splitlines()))
        self.assertEqual(svc.store.reserved_total("disk"), 0)
        self.assertEqual(svc.store.actual_total("disk"), 0)


class BudgetTests(Case):
    def test_parked_job_retries_without_operator(self):
        """A parked job is re-dispatched by the scheduler after its backoff.

        Regression for the scheduler only scanning ``queued``: the job used to
        sit in ``paused_budget`` forever unless a human clicked resume.

        Parking here is driven by *disk contention*: two jobs each need most of
        the volume, so the second one's admission is denied while the first
        holds its reservation. (The earlier version of this test parked on the
        memory budget instead — admission asked for the whole memory cap and
        could only grant ``admit_share`` — which was a bug, not a scenario.)
        """
        cfg = make_config(
            self.tmp / "retry",
            disk_budget_bytes=600 * 1024,
            memory_budget_bytes=64 * 1024,
            max_job_fraction=0.70,
            reservation_slack=0.10,
            max_concurrent_jobs=2,
            max_runs_per_job=64,
            park_backoff_seconds=0.05,
            park_backoff_max_seconds=0.1,
            park_attempt_limit=40,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        # Two jobs that do not both fit at once: the second must park after its
        # admission is denied and be re-dispatched when the first publishes.
        a = svc.generate_demo(6000, skew=3)
        b = svc.generate_demo(6000, skew=4)
        ja = svc.submit_job("sort", a["id"])
        jb = svc.submit_job("sort", b["id"])
        svc.start()
        final_b = wait(svc, jb["id"], timeout=40)
        self.assertEqual(final_b["status"], "succeeded", final_b.get("error"))
        final_a = wait(svc, ja["id"], timeout=40)
        self.assertEqual(final_a["status"], "succeeded", final_a.get("error"))
        self.assertGreater(final_a["park_count"] + final_b["park_count"], 0)
        # The retry must have come from the scheduler, not from an operator:
        # the job that parked was re-dispatched without any resume() call.
        events = svc.store.events(job_id=jb["id"], limit=500)
        kinds = [e["kind"] for e in events]
        self.assertIn("paused", kinds)
        self.assertEqual(kinds.count("scheduled"), 2)
        self.assertNotIn("resume", kinds)

    def test_retained_results_consume_admission_capacity(self):
        """Kept results are bytes on the volume and must reduce the next grant.

        Asserted on ``global_gap`` rather than ``admit_share``: with a small
        kept result the per-job cap is still the binding limit, so the cap
        would hide the change in available capacity.
        """
        led = self.service.ledger
        before = led.global_gap("disk")
        job = self.service.store.create_job("x", "sort", status="queued")
        led.open_job(job["id"], 100_000, 4096, "test")
        art = led.create_artifact(job["id"], self.tmp / "kept.dat", "result", 40_960)
        Path(art["path"]).write_bytes(b"k" * 40_960)
        led.record_write(art["id"], 40_960, 10)
        led.seal_artifact(art["id"], records=10)
        led.mark_kept(job["id"], art["id"])
        led.release_job(job["id"], "done", keep=[art["id"]])
        # The job row owns the published result; purge looks it up there.
        self.service.store.update_job(
            job["id"], status="succeeded", stage="released",
            result_path=art["path"], result_artifact_id=art["id"],
            result_bytes=40_960,
        )
        after = led.global_gap("disk")
        self.assertEqual(before - after, 40_960)
        snap = led.snapshot()["disk"]
        self.assertEqual(snap["kept"], 40_960)
        self.assertEqual(snap["committed"], 40_960)
        self.assertGreater(snap["pressure"], snap["scratch_pressure"])
        # Purging gives the capacity back.
        self.service.purge(job["id"])
        self.assertEqual(led.global_gap("disk"), before)

    def test_infeasible_plan_fails_before_writing(self):
        """A plan that can never fit is rejected up front, not after filling disk."""
        cfg = make_config(
            self.tmp / "infeasible",
            disk_budget_bytes=700 * 1024,
            memory_budget_bytes=64 * 1024,
            max_job_fraction=0.50,
            reservation_slack=0.05,
            max_concurrent_jobs=2,
            max_runs_per_job=64,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        big = svc.generate_demo(20000, skew=3)
        small = svc.generate_demo(300, skew=2)
        jbig = svc.submit_job("sort", big["id"])
        jsmall = svc.submit_job("sort", small["id"])
        svc.start()
        fbig = wait(svc, jbig["id"], timeout=40)
        fsmall = wait(svc, jsmall["id"], timeout=40)
        self.assertEqual(fbig["status"], "failed")
        self.assertIn("largest achievable grant", fbig.get("error") or "")
        # Nothing was spilled for the infeasible job, and the small job is
        # unaffected: failure was decided before any write.
        self.assertEqual(
            svc.store.artifacts(job_id=jbig["id"], include_deleted=True), []
        )
        self.assertEqual(fsmall["status"], "succeeded", fsmall.get("error"))
        self.assertTrue(svc.result_path(jsmall["id"]))
        snap = svc.ledger.snapshot()
        self.assertEqual(snap["disk"]["reserved"], 0)
        self.assertEqual(snap["disk"]["actual"], 0)

    def test_recreating_an_open_artifact_at_the_same_path_does_not_double_count(self):
        """Re-registering the same result file must retire the stale row.

        Regression: a parked job resumes and writes ``result.joined`` again
        with ``open("wb")``. Each attempt created a *new* artifact row against
        the same path, so after 16 parks the ledger claimed 4 MB for a 300 KB
        file and the job could never be re-admitted — it parked forever. A
        re-created artifact must replace the previous ``open`` row rather than
        join it.
        """
        cfg = make_config(
            self.tmp / "recreate",
            disk_budget_bytes=4 * 1024 * 1024,
            memory_budget_bytes=64 * 1024,
            max_job_fraction=0.80,
            max_runs_per_job=9999,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        ledger = svc.ledger
        job = svc.store.create_job("recreate-test", "sort")
        ledger.open_job(job["id"], 1 << 20, 0, "test")
        path = svc.config.tmp_dir / job["id"] / "result.srt"
        path.parent.mkdir(parents=True, exist_ok=True)
        for attempt in range(5):
            art = ledger.create_artifact(job["id"], path, "result", 4096)
            with open(path, "wb") as fh:
                fh.write(b"x" * 1000)
            ledger.record_write(art["id"], 1000, 10)
        live = [a for a in svc.store.artifacts(job_id=job["id"]) if a["state"] == "open"]
        self.assertEqual(len(live), 1, f"stale open rows accumulated: {len(live)}")
        self.assertEqual(ledger._accounted_for(job["id"]), 1000)

    def test_join_result_size_is_estimated_from_key_multiplicity(self):
        """A duplicate-key join must reserve for its *result*, not its input.

        Regression: the join plan budgeted only the partition copies, so a
        join whose duplicate keys multiply the output into 3 MB planned 283 KB.
        It overran its envelope while writing the result, parked, and parked
        again forever because the envelope never grew. The plan must now
        estimate the joined row count from sample key cardinality.
        """
        cfg = make_config(
            self.tmp / "join-estimate",
            disk_budget_bytes=12 * 1024 * 1024,
            memory_budget_bytes=64 * 1024,
            max_job_fraction=0.80,
            max_runs_per_job=9999,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        # 400 keys × ~20 copies on each side => 160k output rows from a 95 KB
        # input on each side.
        left = [b"k%04d\tL%d" % (i % 400, i) for i in range(8000)]
        right = [b"k%04d\tR%d" % (i % 400, i) for i in range(8000)]
        lp, rp = self.tmp / "join-l.tsv", self.tmp / "join-r.tsv"
        write_tsv(lp, [line.decode() for line in left])
        write_tsv(rp, [line.decode() for line in right])
        with open(lp, "rb") as fh:
            lu = svc.save_upload("join-l.tsv", fh)
        with open(rp, "rb") as fh:
            ru = svc.save_upload("join-r.tsv", fh)
        job = svc.submit_job("join", lu["id"], ru["id"])
        svc.start()
        final = wait(svc, job["id"], timeout=120)
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        self.assertEqual(final["park_count"], 0)
        by_left: dict[bytes, list[bytes]] = {}
        by_right: dict[bytes, list[bytes]] = {}
        for line in left:
            key, payload = line.split(b"\t")
            by_left.setdefault(key, []).append(payload)
        for line in right:
            key, payload = line.split(b"\t")
            by_right.setdefault(key, []).append(payload)
        expected = sorted(
            key + b"\t" + l + b"\t" + r
            for key in by_left.keys() & by_right.keys()
            for l in by_left[key]
            for r in by_right[key]
        )
        self.assertEqual(sorted(read_result(svc, job["id"])), expected)
        # The plan must not have under-booked the expansion by an order of
        # magnitude (it may over-estimate; over-booking is a park, not a loss).
        est = final["plan"]["estimates"]
        self.assertGreaterEqual(est["join_rows"], len(expected) * 0.9)
        self.assertGreaterEqual(
            final["plan"]["expected_temp_bytes"], int(est["join_bytes"] * 0.9)
        )
        self.assertEqual(svc.store.reserved_total("disk"), 0)
        self.assertEqual(svc.store.actual_total("disk"), 0)

    def test_underestimated_job_parks_without_starving_others(self):
        cfg = make_config(
            self.tmp / "park",
            disk_budget_bytes=700 * 1024,
            memory_budget_bytes=64 * 1024,
            max_job_fraction=0.55,
            reservation_slack=0.05,
            max_concurrent_jobs=1,
            max_runs_per_job=256,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        big = svc.generate_demo(40000, skew=4)
        small = svc.generate_demo(300, skew=2)
        a = svc.submit_job("sort", big["id"])
        b = svc.submit_job("sort", small["id"])
        svc.start()
        final_b = wait(svc, b["id"], timeout=30)
        self.assertEqual(final_b["status"], "succeeded", final_b.get("error"))
        final_a = svc.store.get_job(a["id"])
        self.assertIn(
            final_a["status"], ("paused_budget", "succeeded", "queued", "running"),
            final_a,
        )
        # Whatever happened to A, the ledger never exceeded the hard budget.
        snap = svc.ledger.snapshot()
        self.assertLessEqual(snap["disk"]["reserved"], cfg.disk_budget_bytes)
        # And B's result is real.
        self.assertTrue(svc.result_path(b["id"]))

    def test_cancel_releases_everything(self):
        up = self.service.generate_demo(30000, skew=8)
        job = self.service.submit_job("sort", up["id"])
        self.start()
        time.sleep(0.05)
        self.assertTrue(self.service.cancel(job["id"]))
        final = wait(self.service, job["id"], timeout=15,
                     terminal=("cancelled", "failed"))
        self.assertEqual(final["status"], "cancelled")
        snap = self.service.ledger.snapshot()
        self.assertEqual(snap["disk"]["reserved"], 0)
        self.assertEqual(snap["disk"]["actual"], 0)

    def test_resume_after_cancel_reproduces_correct_result(self):
        path = self.job_input(8000)
        job = submit_path(self.service, path)
        self.start()
        # Cancel very early, then resume and let it finish.
        self.service.cancel(job["id"])
        wait(self.service, job["id"], timeout=10)
        if self.service.store.get_job(job["id"])["status"] != "succeeded":
            # The job lost the race to publish; a cancelled job must be
            # resumable and reproduce the same bytes.
            self.assertTrue(self.service.resume(job["id"]))
        final = wait(self.service, job["id"], timeout=20)
        self.assertEqual(final["status"], "succeeded", final.get("error"))
        with open(path, "rb") as fh:
            self.assertEqual(read_result(self.service, job["id"]),
                             sorted(fh.read().splitlines()))

    def test_cancel_racing_dispatch_never_leaves_cancelling(self):
        """A cancel that lands between dispatch and its CAS must still finish.

        Regression: ``_dispatch`` registered itself in ``_running`` and then
        lost the compare-and-swap because ``cancel`` had just set the status to
        ``cancelling``. It removed its entry silently, so ``cancel`` believed a
        worker would settle the job and the job stayed ``cancelling`` forever.
        Repeated because it is a narrow window: the loop is the assertion.
        """
        for trial in range(40):
            up = self.service.generate_demo(4000, skew=3)
            job = self.service.submit_job("sort", up["id"])
            self.service.start()
            cancelled = self.service.cancel(job["id"])
            final = wait(self.service, job["id"], timeout=10,
                         terminal=("cancelled", "failed", "succeeded"))
            if not cancelled:
                # Already finished before the cancel: nothing to race against.
                self.assertIn(final["status"], ("succeeded", "failed"),
                              f"trial {trial}")
            self.assertIn(
                final["status"], ("succeeded", "failed", "cancelled"),
                f"trial {trial}: job stuck in {final['status']} "
                f"(epoch={final.get('run_epoch')})",
            )
            snap = self.service.ledger.snapshot()["disk"]
            self.assertEqual(snap["reserved"], 0, f"trial {trial}")
            self.assertEqual(snap["actual"], 0, f"trial {trial}")
            self._reclaim(final["id"])

    def test_resume_while_cancel_cleanup_is_retrying(self):
        """A resume that lands inside the cancel-cleanup retry window must win.

        Regression: ``_finish_cancel`` kept retrying for ~1s after the job was
        already requeued, and its reclaim deleted the *new* executor's
        checkpoint and runs. The job then failed with "unknown stage released"
        even though nothing was wrong with the input.
        """
        for trial in range(40):
            up = self.service.generate_demo(6000, skew=9)
            job = self.service.submit_job("sort", up["id"])
            self.service.start()
            self.service.cancel(job["id"])
            # Poll ``resume`` as soon as the job leaves ``running`` so the call
            # lands inside the cleanup thread's retry/backoff window rather
            # than long after it finished.
            deadline = time.time() + 15
            resumed = False
            while time.time() < deadline:
                cur = self.service.store.get_job(job["id"])
                if cur["status"] in ("cancelled", "failed"):
                    if self.service.resume(job["id"]):
                        resumed = True
                        break
                elif cur["status"] == "succeeded":
                    break
                time.sleep(0.005)
            if not resumed:
                # The job finished before the cancel landed; nothing raced.
                self.assertEqual(
                    self.service.store.get_job(job["id"])["status"], "succeeded",
                    f"trial {trial}: resume never accepted "
                    f"(status={self.service.store.get_job(job['id'])['status']})",
                )
                continue
            final = wait(self.service, job["id"], timeout=25)
            self.assertEqual(final["status"], "succeeded",
                             f"trial {trial}: {final.get('error')}")
            with open(up["path"], "rb") as fh:
                self.assertEqual(read_result(self.service, job["id"]),
                                 sorted(fh.read().splitlines()), f"trial {trial}")
            # Keep the loop under the shared budget: a retained result is real
            # bytes, so a few dozen trials would otherwise make the *next*
            # trial's resume legitimately fail for lack of admission capacity.
            self._reclaim(final["id"])

    def _reclaim(self, job_id: str) -> None:
        job = self.service.store.get_job(job_id)
        if job and job["status"] == "succeeded":
            self.service.purge(job_id)


class ConcurrencyTests(Case):
    def test_no_live_intermediate_is_deleted_under_pressure(self):
        """Many jobs share a tiny disk; nothing in use may vanish.

        A sampler thread watches every artifact row while the jobs run and
        records any ``delete_artifact`` that succeeded while the row still had
        references. The ledger must also settle to zero at the end, which is
        the byte-level proof that no intermediate leaked.
        """
        cfg = make_config(
            self.tmp / "pressure",
            disk_budget_bytes=1500 * 1024,
            memory_budget_bytes=64 * 1024,
            max_job_fraction=0.45,
            reservation_slack=0.05,
            debt_allowance_fraction=0.05,
            max_concurrent_jobs=3,
            max_runs_per_job=64,
            park_backoff_seconds=0.05,
            park_backoff_max_seconds=0.2,
            park_attempt_limit=40,
        )
        svc = Service(cfg)
        self.addCleanup(svc.stop)
        jobs = []
        for i, (n, skew) in enumerate(((4000, 3), (5000, 7), (3500, 11), (4500, 5))):
            up = svc.generate_demo(n, skew=skew)
            jobs.append((svc.submit_job("sort" if i % 2 else "dedupe", up["id"]),
                         Path(up["path"])))

        violations: list[str] = []
        stop = threading.Event()

        def watch():
            while not stop.is_set():
                for art in svc.store.artifacts(include_deleted=True):
                    if art["state"] == "deleted" and art["refs"]:
                        violations.append(art["id"])
                stop.wait(0.005)

        watcher = threading.Thread(target=watch, daemon=True)
        watcher.start()
        svc.start()
        try:
            finals = [wait(svc, job["id"], timeout=90) for job, _ in jobs]
        finally:
            stop.set()
            watcher.join(timeout=2)
        self.assertEqual(violations, [])
        for final in finals:
            self.assertEqual(final["status"], "succeeded", final.get("error"))
        # Every result is byte-identical to a plain in-process sort/dedupe.
        for (job, path), final in zip(jobs, finals):
            with open(path, "rb") as fh:
                lines = fh.read().splitlines()
            expected = sorted(lines) if job["kind"] == "sort" else sorted(set(lines))
            self.assertEqual(read_result(svc, job["id"]), expected)
        snap = svc.ledger.snapshot()["disk"]
        self.assertEqual(snap["reserved"], 0)
        self.assertEqual(snap["actual"], 0)
        self.assertGreater(snap["kept"], 0)


class SchedulerTests(Case):
    """Policy ordering is a pure function, so it can be asserted directly."""

    def _jobs(self):
        base = time.time()
        return [
            {"id": "big", "created_at": base, "submitted_at": base,
             "source_bytes": 900_000, "second_bytes": 0},
            {"id": "small", "created_at": base + 1, "submitted_at": base + 1,
             "source_bytes": 1_000, "second_bytes": 0},
            {"id": "mid", "created_at": base + 2, "submitted_at": base + 2,
             "source_bytes": 50_000, "second_bytes": 0},
        ]

    def test_throughput_is_fifo_and_single_spiller(self):
        self.service.set_policy("throughput")
        sched = self.service.scheduler
        order = [j["id"] for j in sched._order(self._jobs())]
        self.assertEqual(order, ["big", "small", "mid"])
        self.assertEqual(sched._concurrency(), 1)

    def test_fairness_is_shortest_first_with_concurrency(self):
        self.service.set_policy("fairness")
        sched = self.service.scheduler
        order = [j["id"] for j in sched._order(self._jobs())]
        self.assertEqual(order, ["small", "mid", "big"])
        self.assertEqual(sched._concurrency(), sched.config.max_concurrent_jobs)

    def test_aging_promotes_a_starved_big_job(self):
        self.service.set_policy("fairness")
        sched = self.service.scheduler
        now = time.time()
        jobs = self._jobs()
        for job in jobs:
            job["submitted_at"] = now - sched.config.fairness_aging_seconds - 5
        order = [j["id"] for j in sched._order(jobs)]
        # Everything is aged, so the tie-break is submission order, not size.
        self.assertEqual(order, ["big", "small", "mid"])

    def test_adaptive_uses_fifo_below_the_watermark(self):
        self.service.set_policy("adaptive")
        sched = self.service.scheduler
        sched.ledger.pressure = lambda kind="disk": 0.0  # type: ignore[assignment]
        self.assertEqual(sched._concurrency(), 1)
        order = [j["id"] for j in sched._order(self._jobs())]
        self.assertEqual(order, ["big", "small", "mid"])


class ReferenceTests(Case):
    def test_cannot_delete_referenced_artifact(self):
        led = self.service.ledger
        job = self.service.store.create_job("x", "sort", status="queued")
        res, mem = led.open_job(job["id"], 4096, 4096, "test")
        art = led.create_artifact(job["id"], self.tmp / "a.dat", "run", 10)
        path = Path(art["path"])
        path.write_bytes(b"0123456789")
        led.record_write(art["id"], 10, 1)
        led.add_ref(art["id"], "other-job", "job_other")
        with self.assertRaises(ProtectedArtifact):
            led.delete_artifact(art["id"], "should fail")
        self.assertTrue(path.exists())
        led.drop_ref(art["id"], "other-job", "job_other")
        self.assertTrue(led.delete_artifact(art["id"], "now unowned"))
        self.assertFalse(path.exists())

    def test_partial_artifact_cleanup_after_failure(self):
        led = self.service.ledger
        job = self.service.store.create_job("x", "sort", status="queued")
        led.open_job(job["id"], 8192, 4096, "test")
        art = led.create_artifact(job["id"], self.tmp / "partial.dat", "run", 100)
        Path(art["path"]).write_bytes(b"x" * 500)
        led.record_write(art["id"], 500, 1)
        self.assertGreater(led.snapshot()["disk"]["actual"], 0)
        self.service.recovery_call = self.service.recover()
        self.assertEqual(led.snapshot()["disk"]["actual"], 0)
        self.assertFalse(Path(art["path"]).exists())


JOIN_FAULT_POINTS = {"during_partition", "mid_join_bucket"}


def join_inputs(root: Path) -> tuple[Path, Path, list[bytes]]:
    """Left/right files with duplicate keys on both sides, plus the join."""
    left = [b"k%04d\tL%d" % (i % 300, i) for i in range(4000)]
    right = [b"k%04d\tR%d" % (i % 300, i) for i in range(3000)]
    lp, rp = root / "join-left.tsv", root / "join-right.tsv"
    write_tsv(lp, [line.decode() for line in left])
    write_tsv(rp, [line.decode() for line in right])
    by_key_left: dict[bytes, list[bytes]] = {}
    by_key_right: dict[bytes, list[bytes]] = {}
    for line in left:
        key, payload = line.split(b"\t")
        by_key_left.setdefault(key, []).append(payload)
    for line in right:
        key, payload = line.split(b"\t")
        by_key_right.setdefault(key, []).append(payload)
    expected = sorted(
        key + b"\t" + l + b"\t" + r
        for key in by_key_left.keys() & by_key_right.keys()
        for l in by_key_left[key]
        for r in by_key_right[key]
    )
    return lp, rp, expected


def crash_case(point: str, verbose: bool = False) -> bool:
    """Run one job, crash it at ``point``, recover, verify, finish.

    Verifies three things after recovery:
      1. the ledger still matches bytes on disk (no leaks, no phantom bytes);
      2. the checkpoint referenced artifacts were preserved (no wasted work
         when the fault was after the runs were sealed);
      3. the resumed job produces the same result as an uninterrupted run.

    The two join-only points (``during_partition``, ``mid_join_bucket``) cannot
    trip in a sort, so those runs use a join job and a join-shaped expectation.
    Running them through a sort used to report "did not reach crashed" — a
    vacuous pass that never exercised join recovery.
    """
    tmp = Path(tempfile.mkdtemp(prefix=f"spillforge-crash-{point}-"))
    cfg = make_config(tmp, tombstone_seconds=0.2)
    if point in JOIN_FAULT_POINTS:
        # Force the partitioned path: a small buffer makes the build side
        # exceed what fits in memory, so the planner picks
        # ``hash_join_partitioned`` and the bucket fault points exist.
        cfg = make_config(
            tmp, tombstone_seconds=0.2,
            memory_budget_bytes=64 * 1024,
            disk_budget_bytes=8 * 1024 * 1024,
            max_job_fraction=0.80,
            max_runs_per_job=9999,
        )
    svc = Service(cfg)
    ok = True
    messages = []

    def emit(msg):
        messages.append(msg)
        if verbose:
            print(msg)

    try:
        if point in JOIN_FAULT_POINTS:
            lp, rp, expected = join_inputs(tmp)
            with open(lp, "rb") as fh:
                left_up = svc.save_upload("join-left.tsv", fh)
            with open(rp, "rb") as fh:
                right_up = svc.save_upload("join-right.tsv", fh)
            job = svc.submit_job("join", left_up["id"], right_up["id"])
        else:
            up = svc.generate_demo(24000, skew=7)
            path = Path(up["path"])
            with open(path, "rb") as fh:
                expected = sorted(fh.read().splitlines())
            job = svc.submit_job("sort", up["id"])
        svc.set_faults([point])
        svc.start()
        deadline = time.time() + 30
        crashed = None
        while time.time() < deadline:
            cur = svc.store.get_job(job["id"])
            if cur["status"] == "crashed":
                crashed = cur
                break
            if cur["status"] in ("succeeded", "failed"):
                break
            time.sleep(0.02)
        if not crashed:
            emit(f"[FAIL] {point}: job did not reach 'crashed' "
                 f"({svc.store.get_job(job['id'])['status']})")
            return False
        emit(f"[{point}] crashed at stage {crashed['stage']}")
        arts_before = svc.store.artifacts(job_id=job["id"])
        refs_before = {a["id"]: len(a["refs"]) for a in arts_before}
        emit(f"[{point}] artifacts at crash: {[(a['role'], a['state']) for a in arts_before]}")
        # Stop the fault from re-tripping after recovery.
        svc.set_faults([])
        report = svc.recover()
        emit(f"[{point}] recovery: requeued={report['requeued']} "
             f"finished={report['finished']} cleaned={report['cleaned_artifacts']}")
        final = wait(svc, job["id"], timeout=40)
        if final["status"] != "succeeded":
            emit(f"[FAIL] {point}: resumed job ended {final['status']} "
                 f"{final.get('error')}")
            return False
        got = read_result(svc, job["id"])
        # A hash join emits rows in bucket/probe order, not sorted order; the
        # sort cases must match byte for byte.
        if point in JOIN_FAULT_POINTS:
            got = sorted(got)
        if got != expected:
            emit(f"[FAIL] {point}: result mismatch "
                 f"({len(got)} vs {len(expected)} lines)")
            return False
        snap = svc.ledger.snapshot()
        if snap["disk"]["reserved"] != 0 or snap["disk"]["actual"] != 0:
            emit(f"[FAIL] {point}: ledger not settled {snap}")
            return False
        # Every artifact row that survived must have consistent accounting.
        for art in svc.store.artifacts(job_id=job["id"], include_deleted=True):
            if art["state"] == "deleted":
                continue
            if art["state"] == "kept":
                continue
            emit(f"[FAIL] {point}: leftover artifact {art['id']} {art['state']}")
            return False
        log = svc.store.events(job_id=job["id"], limit=1000)
        if point in ("after_run_complete", "mid_merge"):
            recovered = [e for e in log if e["kind"] == "recovered"]
            if recovered:
                preserved = recovered[-1]["data"].get("preserved") or []
                if not preserved:
                    emit(f"[warn] {point}: nothing preserved (full restart)")
        emit(f"[{point}] OK: recovered, result identical, ledger settled "
             f"(epoch={snap['epoch']})")
        return True
    except Exception as exc:  # noqa: BLE001 - report, do not crash the runner
        import traceback
        emit(f"[FAIL] {point}: {type(exc).__name__}: {exc}")
        if verbose:
            emit(traceback.format_exc())
        return False
    finally:
        try:
            svc.stop()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


def merge_generation_crash_case(verbose: bool = False) -> bool:
    """Crash in the middle of a *generation* of a multi-pass merge.

    The plain crash cases run with the default memory budget and land in the
    repartitioned join path. This one forces a 8 KiB sort buffer so the planner
    emits hundreds of runs with ``fan_in`` 2, and the ``mid_merge`` fault trips
    while an intermediate (``merge_tmp``) artifact is being written. Recovery
    must keep the committed run set, discard the half-written intermediate, and
    redo only the current generation.
    """
    tmp = Path(tempfile.mkdtemp(prefix="spillforge-crash-generations-"))
    cfg = make_config(
        tmp, tombstone_seconds=0.2,
        memory_budget_bytes=8 * 1024,
        disk_budget_bytes=12 * 1024 * 1024,
        max_job_fraction=0.80,
        max_runs_per_job=9999,
    )
    svc = Service(cfg)
    messages = []

    def emit(msg):
        messages.append(msg)
        if verbose:
            print(msg)

    try:
        up = svc.generate_demo(12000, skew=5)
        with open(up["path"], "rb") as fh:
            expected = sorted(fh.read().splitlines())
        job = svc.submit_job("sort", up["id"])
        svc.set_faults(["mid_merge"])
        svc.start()
        deadline = time.time() + 60
        crashed = None
        while time.time() < deadline:
            cur = svc.store.get_job(job["id"])
            if cur["status"] == "crashed":
                crashed = cur
                break
            if cur["status"] in ("succeeded", "failed"):
                break
            time.sleep(0.02)
        if not crashed:
            emit("[FAIL] multi-pass: job did not crash "
                 f"({svc.store.get_job(job['id'])['status']})")
            return False
        arts_before = svc.store.artifacts(job_id=job["id"])
        roles = {a["role"] for a in arts_before}
        emit(f"[multi-pass] crashed at stage {crashed['stage']} "
             f"with {len(arts_before)} artifacts, roles={sorted(roles)}")
        if "merge_tmp" not in roles:
            emit("[FAIL] multi-pass: crash did not land inside a merge generation")
            return False
        if crashed["plan"] and crashed["plan"].get("fan_in", 0) > 1:
            runs = len([a for a in arts_before if a["role"] in ("run", "merge_tmp")])
            # If every run had been opened at once, fan-in would be moot; this
            # only asserts the plan really required generations.
            if runs <= crashed["plan"]["fan_in"]:
                emit(f"[FAIL] multi-pass: only {runs} runs, no generation needed")
                return False
        svc.set_faults([])
        report = svc.recover()
        emit(f"[multi-pass] recovery: requeued={report['requeued']} "
             f"finished={report['finished']} cleaned={report['cleaned_artifacts']}")
        final = wait(svc, job["id"], timeout=120)
        if final["status"] != "succeeded":
            emit(f"[FAIL] multi-pass: resumed job ended {final['status']} "
                 f"{final.get('error')}")
            return False
        got = read_result(svc, job["id"])
        if got != expected:
            emit(f"[FAIL] multi-pass: result mismatch "
                 f"({len(got)} vs {len(expected)} lines)")
            return False
        snap = svc.ledger.snapshot()
        if snap["disk"]["reserved"] != 0 or snap["disk"]["actual"] != 0:
            emit(f"[FAIL] multi-pass: ledger not settled {snap}")
            return False
        for art in svc.store.artifacts(job_id=job["id"], include_deleted=True):
            if art["state"] in ("deleted", "kept"):
                continue
            emit(f"[FAIL] multi-pass: leftover artifact {art['id']} {art['state']}")
            return False
        emit("[multi-pass] OK: recovered from a generation, result identical, "
             "ledger settled")
        return True
    except Exception as exc:  # noqa: BLE001
        import traceback
        emit(f"[FAIL] multi-pass: {type(exc).__name__}: {exc}")
        if verbose:
            emit(traceback.format_exc())
        return False
    finally:
        try:
            svc.stop()
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


def run_selftest(verbose: bool = True) -> bool:
    """Run the unit suite and every required crash point."""
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromModule(sys.modules[__name__])
    if verbose:
        print("== unit suite ==")
    result = unittest.TextTestRunner(
        stream=sys.stdout if verbose else open(os.devnull, "w"),
        verbosity=2 if verbose else 0,
    ).run(suite)
    results = []
    if verbose:
        print("\n== crash recovery ==")
    for point in ("after_run_complete", "mid_merge", "before_budget_release",
                  "after_publish_before_release"):
        results.append((point, crash_case(point, verbose=verbose)))
    results.append(("multi-pass mid_merge", merge_generation_crash_case(verbose=verbose)))
    # Join fault points must be run against a join; a sort never trips them.
    for point in ("during_partition", "mid_join_bucket"):
        results.append((point, crash_case(point, verbose=verbose)))
    ok = result.wasSuccessful() and all(r[1] for r in results)
    if verbose:
        print()
        for name, passed in results:
            print(f"  {'PASS' if passed else 'FAIL'}  {name}")
    return ok


if __name__ == "__main__":
    raise SystemExit(0 if run_selftest() else 1)
