"""Record I/O, sampling and distribution estimation.

Input is newline-delimited UTF-8. A record is compared bytewise over the whole
line (the same order as ``LC_ALL=C sort``). For dedupe/join the key is the
bytes before the first tab; a line without a tab is all key.

Everything here is streaming and bounded: samples read a fixed number of
random chunks, never the whole file, so planning cost stays O(1) in input size.
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator

from .errors import SourceNotFound, SpillForgeError

# Lines longer than this are rejected in the sampler. The executor enforces the
# same limit while streaming so we never build an unbounded buffer from a file
# with no newlines.
MAX_RECORD_BYTES = 64 * 1024


def iter_lines(path: Path, buffer_size: int = 64 * 1024) -> Iterator[bytes]:
    """Yield lines without the trailing newline, bounded per record."""
    with open(path, "rb", buffering=0) as fh:
        buf = b""
        remainder = b""
        while True:
            chunk = fh.read(buffer_size)
            if not chunk:
                break
            parts = (remainder + chunk).split(b"\n")
            remainder = parts.pop()
            if len(remainder) > MAX_RECORD_BYTES:
                raise SpillForgeError(
                    f"record longer than {MAX_RECORD_BYTES} bytes in {path.name}"
                )
            for line in parts:
                if len(line) > MAX_RECORD_BYTES:
                    raise SpillForgeError(
                        f"record longer than {MAX_RECORD_BYTES} bytes in {path.name}"
                    )
                yield line[:-1] if line.endswith(b"\r") else line
        if remainder:
            if len(remainder) > MAX_RECORD_BYTES:
                raise SpillForgeError(
                    f"record longer than {MAX_RECORD_BYTES} bytes in {path.name}"
                )
            yield remainder[:-1] if remainder.endswith(b"\r") else remainder


def key_of(line: bytes) -> bytes:
    tab = line.find(b"\t")
    return line if tab < 0 else line[:tab]


def payload_of(line: bytes) -> bytes:
    tab = line.find(b"\t")
    return b"" if tab < 0 else line[tab + 1:]


def count_lines_slow(path: Path) -> int:
    count = 0
    for _ in iter_lines(path):
        count += 1
    return count


@dataclass
class SampleStats:
    """Everything the planner is allowed to know before executing."""

    path: str
    file_bytes: int
    sample_records: int
    sample_bytes: int
    avg_record_bytes: float
    est_records: int
    distinct_keys: int
    max_key_frequency: int
    avg_key_bytes: float
    duplicate_ratio: float
    sorted_fraction: float
    # key -> (sample count, total bytes) for join estimates
    key_counts: dict[bytes, int] = field(default_factory=dict, repr=False)
    sample_lines: list[bytes] = field(default_factory=list, repr=False)

    def to_public(self) -> dict:
        return {
            "path": self.path,
            "file_bytes": self.file_bytes,
            "sample_records": self.sample_records,
            "avg_record_bytes": round(self.avg_record_bytes, 2),
            "est_records": self.est_records,
            "distinct_keys_in_sample": self.distinct_keys,
            "max_key_frequency_in_sample": self.max_key_frequency,
            "avg_key_bytes": round(self.avg_key_bytes, 2),
            "duplicate_ratio": round(self.duplicate_ratio, 4),
            "sorted_fraction": round(self.sorted_fraction, 4),
        }


def _read_lines_at(fh, offset: int, limit: int, max_bytes: int) -> list[bytes]:
    fh.seek(offset)
    if offset:
        fh.readline()  # discard the partial line at the offset
    out: list[bytes] = []
    used = 0
    while len(out) < limit and used < max_bytes:
        line = fh.readline(MAX_RECORD_BYTES + 2)
        if not line:
            break
        if not line.endswith(b"\n"):
            break
        used += len(line)
        out.append(line.rstrip(b"\n"))
    return out


def sample_file(
    path: Path,
    max_records: int = 2000,
    max_bytes: int = 2 * 1024 * 1024,
    chunks: int = 6,
    seed: int = 7,
) -> SampleStats:
    """Reservoir-ish sample from the head and several random offsets.

    Sampling random offsets rather than only the head matters: a file sorted by
    a low-cardinality key has all its skew at the beginning, and a head-only
    sample would systematically underestimate join explosion.
    """
    try:
        size = path.stat().st_size
    except OSError as exc:
        raise SourceNotFound(f"{path}: {exc}") from exc
    if size == 0:
        return SampleStats(
            path=str(path), file_bytes=0, sample_records=0, sample_bytes=0,
            avg_record_bytes=1.0, est_records=0, distinct_keys=0,
            max_key_frequency=0, avg_key_bytes=0.0, duplicate_ratio=0.0,
            sorted_fraction=1.0,
        )
    rng = random.Random(seed)
    per_chunk = max(1, max_records // (chunks + 1))
    per_chunk_bytes = max(4096, max_bytes // (chunks + 1))
    lines: list[bytes] = []
    ordered_pairs = 0
    pair_total = 0
    offsets = [0] + [rng.randrange(0, max(1, size - 1)) for _ in range(chunks)]
    with open(path, "rb", buffering=0) as fh:
        for off in offsets:
            chunk = _read_lines_at(fh, off, per_chunk, per_chunk_bytes)
            # Sortedness must be measured *within* a contiguous chunk. Comparing
            # the last line of one random offset with the first of the next would
            # report almost any unsorted file as half-sorted.
            pair_total += max(0, len(chunk) - 1)
            ordered_pairs += sum(1 for a, b in zip(chunk, chunk[1:]) if a <= b)
            lines.extend(chunk)
    if not lines:
        # A file with no newline is one huge record; reject it early rather
        # than letting the executor build an unbounded buffer.
        fh = open(path, "rb")
        try:
            head = fh.read(MAX_RECORD_BYTES + 1)
        finally:
            fh.close()
        if len(head) > MAX_RECORD_BYTES:
            raise SpillForgeError(f"record longer than {MAX_RECORD_BYTES} bytes")
        lines = [head]
    sample_bytes = sum(len(line) + 1 for line in lines)
    avg = sample_bytes / len(lines)
    est_records = max(1, round(size / max(avg, 1.0)))
    counts: dict[bytes, int] = {}
    key_bytes = 0
    for line in lines:
        key = key_of(line)
        counts[key] = counts.get(key, 0) + 1
        key_bytes += len(key)
    sample_n = len(lines)
    max_freq = max(counts.values()) if counts else 0
    sorted_fraction = ordered_pairs / max(1, pair_total)
    return SampleStats(
        path=str(path),
        file_bytes=size,
        sample_records=sample_n,
        sample_bytes=sample_bytes,
        avg_record_bytes=avg,
        est_records=est_records,
        distinct_keys=len(counts),
        max_key_frequency=max_freq,
        avg_key_bytes=key_bytes / sample_n,
        duplicate_ratio=1.0 - len(counts) / sample_n,
        sorted_fraction=sorted_fraction,
        key_counts=counts,
        sample_lines=lines,
    )


def estimated_total_frequency(sample: SampleStats, key: bytes) -> float:
    count = sample.key_counts.get(key, 0)
    if not sample.sample_records:
        return 0.0
    return count / sample.sample_records * sample.est_records
