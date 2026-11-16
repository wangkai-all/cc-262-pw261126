"""Execution plans.

A plan is a pure data structure computed from samples + budgets. It is stored
on the job row so the UI can show *why* a job chose batching or repartitioning,
and so a resumed job can verify the plan against the budget it still holds.
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Literal

from .records import SampleStats

PlanKind = Literal[
    "streaming", "in_memory", "merge", "repartition", "hash_join",
    "hash_join_partitioned",
]


@dataclass
class Plan:
    kind: PlanKind
    job_kind: str
    fan_in: int
    #: number of sorted runs written before the merge (sort/dedupe)
    run_count: int
    #: number of repartition buckets (0 when not repartitioning)
    partitions: int
    #: bytes the plan reserves for its files
    expected_temp_bytes: int
    #: in-process buffer reserved for sorting
    expected_memory_bytes: int
    #: peak live bytes during merge (all inputs open at once)
    expected_merge_bytes: int
    reason: str
    warnings: list[str] = field(default_factory=list)
    estimates: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        d = asdict(self)
        d["estimates"] = {k: (round(v, 2) if isinstance(v, float) else v)
                          for k, v in self.estimates.items()}
        return d


class Planner:
    """Chooses a strategy from data distribution and current budgets.

    Decision order for sort/dedupe:

    1. If the *whole* input fits in the job's memory slice, sort in memory and
       write twice the input bytes as temp output (one result file, no runs).
    2. Otherwise generate runs with a bounded read buffer and fan-in merge.
       Fan-in is capped by both file descriptors and the merge memory budget.
    3. If the estimated run count exceeds ``max_runs_per_job`` or the single
       largest key bucket (skew) cannot be merged within the job's disk cap,
       repartition by key hash first. Repartitioning wins on skew because it
       splits a hot key *by hash of the full record*, and on run count because
       each bucket is smaller. It costs one extra full rewrite, so we only do
       it when it actually prevents a cap violation or a second pass.
    """

    def __init__(self, config, ledger=None):
        self.config = config
        self.ledger = ledger

    # ------------------------------------------------------------------ sort

    def plan_sort(
        self,
        sample: SampleStats,
        job_kind: str,
        memory_cap: int,
        disk_cap: int,
        distinct: bool = False,
    ) -> Plan:
        input_bytes = sample.file_bytes
        avg = max(1.0, sample.avg_record_bytes)
        warnings: list[str] = []
        # Dedupe output is bounded by distinct keys; sort output equals input.
        if job_kind == "dedupe":
            distinct_ratio = max(
                0.01, sample.distinct_keys / max(1, sample.sample_records)
            )
            output_bytes = int(input_bytes * distinct_ratio * 1.05) + 64
        else:
            output_bytes = input_bytes
        buffer = min(
            self.config.max_read_buffer_bytes,
            max(self.config.min_read_buffer_bytes, memory_cap // 4),
        )
        available_mem = max(buffer, memory_cap // 2)
        records_per_buffer = max(1, int(available_mem // (avg * 2.2)))
        # Each run is a file of its bytes; the merge holds one read buffer per
        # run, so fan-in is limited by merge memory and by descriptors.
        fd_cap = 64
        mem_fan_in = max(2, int(memory_cap // max(buffer, 1)))
        fan_in = max(2, min(fd_cap, mem_fan_in, 32))
        est_records = max(1, sample.est_records)
        # An input that is already sorted needs no runs at all: stream it once
        # through a dedupe filter directly into the result. This is the cheapest
        # plan and is common for logs that were appended in time order.
        if (
            sample.sorted_fraction >= 0.999
            and sample.sample_records >= 200
            and (
                input_bytes > available_mem * 0.75
                or job_kind == "dedupe"
            )
        ):
            return Plan(
                kind="streaming",
                job_kind=job_kind,
                fan_in=0,
                run_count=0,
                partitions=0,
                expected_temp_bytes=int(output_bytes * 1.05) + 4096,
                expected_memory_bytes=min(memory_cap, buffer * 2),
                expected_merge_bytes=min(memory_cap, buffer * 2),
                reason=(
                    f"sample is {sample.sorted_fraction:.1%} ordered; stream the "
                    "sorted input straight to the result and skip the spill/merge"
                ),
                warnings=[],
                estimates={
                    "input_bytes": input_bytes,
                    "est_records": est_records,
                    "buffer_bytes": buffer,
                    "output_bytes": output_bytes,
                    "sorted_fraction": sample.sorted_fraction,
                },
            )
        if input_bytes <= available_mem * 0.75:
            # In-memory: no runs, but we still need the output file. Peak disk
            # is the result file plus a partial while writing.
            return Plan(
                kind="in_memory",
                job_kind=job_kind,
                fan_in=0,
                run_count=0,
                partitions=0,
                expected_temp_bytes=int(output_bytes * 1.1) + 4096,
                expected_memory_bytes=min(memory_cap, int(input_bytes * 1.6) + 4096),
                expected_merge_bytes=min(memory_cap, int(output_bytes * 0.25) + buffer),
                reason=(
                    f"input {human(input_bytes)} fits in a {human(available_mem)} "
                    "sort buffer; single in-memory pass"
                ),
                warnings=warnings,
                estimates={
                    "input_bytes": input_bytes,
                    "est_records": est_records,
                    "buffer_bytes": buffer,
                    "output_bytes": output_bytes,
                },
            )
        runs = max(2, -(-est_records // records_per_buffer))
        merge_passes = _merge_passes(runs, fan_in)
        # Only generations *before* the last one create intermediate runs, so
        # only those can make a chunk of input coexist with the full input.
        intermediate_generations = merge_passes - 1
        merge_bytes_peak = min(
            input_bytes + output_bytes,
            input_bytes * (1 + 1 / max(1, runs)) + output_bytes * 0.5,
        )
        run_write_bytes = input_bytes
        # With more runs than the fan-in the executor merges in generations.
        # While a generation's last chunk is written, its output coexists with
        # every input not yet consumed: peak = input + one chunk output. The
        # final pass then writes the result over the reduced run set, i.e.
        # input + output. Counting both terms at once would double-count, so
        # take the larger of the two. Without this term a dedupe (small output,
        # large input) would reserve too little for its intermediate pass and
        # be parked over a merge it could actually afford.
        intermediate_bytes = (
            min(input_bytes, fan_in * buffer) if intermediate_generations else 0
        )
        expected = int(input_bytes + max(output_bytes, intermediate_bytes)) + 8192
        if expected > disk_cap:
            warnings.append(
                f"plan peak {human(expected)} exceeds the job's disk cap "
                f"{human(disk_cap)}; the job will need debt or will be parked"
            )
        # Repartitioning is a *run-count* remedy, not a disk-peak remedy: it
        # reads and writes the same bytes plus one extra rewrite, so its peak
        # is never lower than the merge it replaces. Only choose it when the
        # merge would need more runs than the configured limit, and only when
        # the resulting peak still fits the job's cap.
        if runs > self.config.max_runs_per_job:
            rep = self._plan_repartition(
                sample, job_kind, memory_cap, disk_cap, runs, buffer, fan_in, output_bytes
            )
            if rep is not None and rep.expected_temp_bytes <= disk_cap:
                return rep
        if runs > fan_in * fan_in:
            warnings.append(
                f"{runs} runs need {merge_passes} merge passes; consider a larger "
                "memory budget for fewer passes"
            )
        return Plan(
            kind="merge",
            job_kind=job_kind,
            fan_in=fan_in,
            run_count=runs,
            partitions=0,
            expected_temp_bytes=expected,
            expected_memory_bytes=min(memory_cap, buffer * min(fan_in, runs) + available_mem),
            expected_merge_bytes=int(min(merge_bytes_peak, self.config.disk_budget_bytes)),
            reason=(
                f"input {human(input_bytes)} > buffer {human(available_mem)}; "
                f"write {runs} sorted runs of ~{human(buffer)} then {fan_in}-way merge"
            ),
            warnings=warnings,
            estimates={
                "input_bytes": input_bytes,
                "est_records": est_records,
                "records_per_buffer": records_per_buffer,
                "buffer_bytes": buffer,
                "output_bytes": output_bytes,
                "merge_passes": merge_passes,
            },
        )

    def _plan_repartition(
        self,
        sample: SampleStats,
        job_kind: str,
        memory_cap: int,
        disk_cap: int,
        runs: int,
        buffer: int,
        fan_in: int,
        output_bytes: int,
    ) -> Plan | None:
        input_bytes = sample.file_bytes
        # Minimum bucket count that brings each bucket's run count under the
        # fan-in limit, so each bucket merges in one pass. Buckets do not
        # reduce peak disk (scatter copies coexist with sorted copies), but
        # they do avoid multi-pass merges of a huge run set.
        need = max(1, -(-runs // max(2, fan_in)))
        partitions = 4
        while partitions < need and partitions < 128:
            partitions *= 2
        if partitions < need:
            return None
        expected = int(input_bytes * 2 + output_bytes) + 16384
        warnings = []
        if expected > disk_cap:
            warnings.append(
                "repartition needs two input copies on disk; this plan exceeds "
                "the job cap"
            )
        merge_mem = min(memory_cap, buffer * 4)
        return Plan(
            kind="repartition",
            job_kind=job_kind,
            fan_in=fan_in,
            run_count=max(1, -(-runs // partitions)),
            partitions=partitions,
            expected_temp_bytes=expected,
            expected_memory_bytes=merge_mem,
            expected_merge_bytes=int(input_bytes + output_bytes),
            reason=(
                f"{runs} runs exceed the {self.config.max_runs_per_job}-run limit; "
                f"hash-partition into {partitions} buckets of "
                f"~{human(input_bytes // partitions)} so each bucket merges in "
                "one pass"
            ),
            warnings=warnings,
            estimates={
                "input_bytes": input_bytes,
                "output_bytes": output_bytes,
                "partitions": partitions,
                "bucket_bytes": input_bytes // partitions,
                "extra_rewrite_bytes": input_bytes,
            },
        )

    # ------------------------------------------------------------------ join

    def plan_join(
        self,
        left: SampleStats,
        right: SampleStats,
        memory_cap: int,
        disk_cap: int,
    ) -> Plan:
        left_bytes, right_bytes = left.file_bytes, right.file_bytes
        avg_left = max(1.0, left.avg_record_bytes)
        avg_right = max(1.0, right.avg_record_bytes)
        build_in_memory = int(right_bytes * 1.8) + int(right.est_records * 96)
        warnings: list[str] = []
        # Estimated result size. A join's output is *not* bounded by its input:
        # with duplicate keys on both sides each pair produces a row, so the
        # result can be an order of magnitude larger than either file. The
        # previous estimate ignored this entirely and budgeted only the
        # partitions, so a join over an expected-to-fit plan overran its
        # envelope while writing the result, parked, and parked again on every
        # retry (the envelope never changed).
        join_rows, join_bytes = _estimate_join_output(left, right, avg_left, avg_right)
        # Skew check: a key worth >=5% of the build side can make an in-memory
        # hash table explode if it is the hot cross-product key.
        hot_share = 0.0
        if right.sample_records:
            hot_share = max(right.key_counts.values(), default=0) / right.sample_records
        skew_note = ""
        if hot_share >= 0.05:
            skey_sample = max(
                (k for k, c in right.key_counts.items() if c == max(right.key_counts.values())),
                default=b"?",
            )
            skew_note = (
                f"build side has a hot key {skey_sample[:24]!r} at "
                f"{hot_share:.0%} of the sample"
            )
            warnings.append(skew_note)
        # Peak for the partitioned path: both inputs partitioned out (+ a
        # little imbalance: the larger side is clipped to 90% extra), both
        # result during the join, and the raw files. ``join_bytes`` is the
        # result itself; partitioning already writes ``left_bytes +
        # right_bytes`` of copies, which the old formula covered.
        partitioned_peak = (
            left_bytes + right_bytes + int(left_bytes * 0.9)
            + join_bytes + 8192
        )
        # An in-memory hash table costs far more than the raw build bytes in
        # Python (dict entry + list + payload copies). The factor below is
        # deliberately pessimistic; underestimating it is how a "small" join
        # turns into an OOM.
        if build_in_memory <= memory_cap * 0.6 and partitioned_peak <= disk_cap:
            return Plan(
                kind="hash_join",
                job_kind="join",
                fan_in=0,
                run_count=0,
                partitions=0,
                expected_temp_bytes=left_bytes + right_bytes + join_bytes + 8192,
                expected_memory_bytes=min(memory_cap, build_in_memory),
                expected_merge_bytes=min(memory_cap, build_in_memory // 2),
                reason=(
                    f"build side {human(right_bytes)} fits a "
                    f"{human(int(memory_cap * 0.6))} hash table; one-pass hash join, "
                    "probe streams from the left file"
                ),
                warnings=warnings,
                estimates={
                    "build_bytes": right_bytes,
                    "probe_bytes": left_bytes,
                    "build_in_memory": build_in_memory,
                    "hot_key_share": hot_share,
                    "join_rows": join_rows,
                    "join_bytes": join_bytes,
                },
            )
        if build_in_memory > memory_cap * 0.6:
            warnings.append(
                f"build side {human(right_bytes)} exceeds the in-memory hash budget; "
                "partition both inputs by key hash"
            )
        else:
            warnings.append(
                f"join needs {human(partitioned_peak)} of scratch against a "
                f"{human(disk_cap)} job cap"
            )
        if join_bytes > max(left_bytes, right_bytes) * 2:
            warnings.append(
                f"duplicate keys expand {human(left_bytes)}+{human(right_bytes)} "
                f"into ~{human(join_bytes)} of result; this join is inherently "
                "larger than its inputs"
            )
        return Plan(
            kind="hash_join_partitioned",
            job_kind="join",
            fan_in=0,
            run_count=0,
            partitions=8,
            expected_temp_bytes=partitioned_peak,
            expected_memory_bytes=min(memory_cap, max(4096, memory_cap // 2)),
            expected_merge_bytes=min(memory_cap, memory_cap // 2),
            reason=(
                "partition both sides by hash(key) into 8 buckets, then hash-join "
                "each pair; a hot key no longer forces the whole build side into RAM"
            ),
            warnings=warnings,
            estimates={
                "build_bytes": right_bytes,
                "probe_bytes": left_bytes,
                "build_in_memory": build_in_memory,
                "hot_key_share": hot_share,
                "partitions": 8,
                "join_rows": join_rows,
                "join_bytes": join_bytes,
            },
        )


def _estimate_join_output(
    left: SampleStats, right: SampleStats,
    avg_left: float, avg_right: float,
) -> tuple[int, int]:
    """Estimate (rows, bytes) of an inner join from the two samples.

    The output of an inner join is ``Σ_k L(k)·R(k)`` over the keys both sides
    share. Samples are ~2k lines, so they cannot enumerate 8k distinct keys;
    what they *can* measure is average multiplicity, and the join expansion
    follows from the key cardinalities:

        rows ≈ matched_keys × (L/matched_keys) × (R/matched_keys)
             = L_est × R_est / matched_keys

    Key cardinality is estimated from Good–Turing sample coverage ("how many
    keys does the file have, given how many the sample saw once"), not by
    scaling the observed count: for a duplicate-heavy file the observed
    sample count *is* the answer, while for a wide key space it is a small
    fraction of it. Footnote: an earlier version used Chao1 alone, which
    over-counted the wide case (Chao1 extrapolates from singletons, and a
    nearly-unique file is all singletons).

    A hot key is handled separately: a key that dominates the build sample will
    dominate the real file too, and its cross-product is not captured by an
    average-multiplicity model, so its share is added directly.
    """
    n_left = max(1, left.sample_records)
    n_right = max(1, right.sample_records)
    l_records = float(max(1, left.est_records))
    r_records = float(max(1, right.est_records))
    keys = _estimate_distinct(left, n_left)
    keys_r = _estimate_distinct(right, n_right)
    # Matching keys are bounded by the smaller side's cardinality; without an
    # overlap estimate, assume the side with fewer distinct keys is fully
    # contained in the other (true for a join whose build side is a subset).
    matched_keys = max(1.0, min(keys, keys_r))
    rows = l_records * r_records / matched_keys
    # Hot-key correction: the average model already charges each key its mean
    # multiplicity, so only the *excess* of an unusually heavy key is added.
    hot_rows = 0.0
    if right.sample_records:
        hot_share = max(right.key_counts.values(), default=0) / right.sample_records
        if hot_share >= 0.05:
            hot_key = max(right.key_counts, key=right.key_counts.get)
            l_count = left.key_counts.get(hot_key, 0)
            if l_count:
                hot_l = l_count / n_left * l_records
                # The average model gave this key a nominal share; replace it
                # with the direct cross-product for the excess only.
                nominal = (r_records / matched_keys) * (l_records / matched_keys)
                hot_rows = max(0.0, hot_l * (hot_share * r_records) - nominal)
    rows = min(rows + hot_rows, l_records * r_records)  # worst case is a full cross-product
    row_bytes = (left.avg_key_bytes + max(0.0, avg_left - left.avg_key_bytes)
                 + max(0.0, avg_right - right.avg_key_bytes) + 2)
    return int(rows), int(rows * max(1.0, row_bytes)) + 4096


def _estimate_distinct(sample: SampleStats, n: int) -> float:
    """Estimate how many distinct keys the *whole file* has.

    Good–Turing sample coverage: the fraction of the key population the sample
    already saw is ``1 − f1/n`` where ``f1`` is the number of keys seen exactly
    once. Hence

        D ≈ observed / coverage

    This behaves correctly at both ends of the spectrum:

    * Duplicate-heavy file (400 keys, ~5 copies each): no singletons in the
      sample, coverage ≈ 1, so ``D ≈ observed`` — the truth.
    * Nearly unique file (each record its own key): essentially every sampled
      key is a singleton, coverage ≈ 0, so ``D`` hits the cap ``N`` — again the
      truth, and the resulting row estimate is ``N²/N = N``.

    Naively scaling ``observed`` by ``N/n`` gets the first case wrong by 4x,
    and the occupancy inversion (solving ``seen = D(1−(1−1/D)^n)``) cannot
    distinguish "400 keys seen because the file has 400" from "400 seen because
    the sample only touched 400 of 20000" — both fit the equation equally well,
    so it silently picks the smaller. Coverage uses the singleton count, which
    *is* the signal that distinguishes them.
    """
    observed = max(1.0, float(sample.distinct_keys))
    N = float(max(1, sample.est_records))
    if not sample.key_counts:
        return observed
    singles = sum(1 for c in sample.key_counts.values() if c == 1)
    coverage = max(1.0 - singles / float(max(1, n)), 1.0 / max(N, 1.0))
    estimate = observed / coverage
    # Chao1 is a second lower bound: it accounts for keys that exist but were
    # not sampled at all (which coverage understates when n << D).
    doubles = sum(1 for c in sample.key_counts.values() if c == 2)
    chao = observed + (singles * singles) / (2.0 * doubles) if doubles else observed
    return max(observed, min(N, max(estimate, chao)))


def _merge_passes(runs: int, fan_in: int) -> int:
    passes = 0
    while runs > 1:
        runs = -(-runs // fan_in)
        passes += 1
    return passes


def human(n: float) -> str:
    n = float(n)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TiB"
