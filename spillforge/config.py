"""Service configuration.

Every number that can affect admission, spilling or fairness lives here so the
same values can be asserted by tests and shown in the UI.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field, asdict
from pathlib import Path

from .errors import ConfigError

# Input format: UTF-8 text, one record per line, ``key<TAB>payload``. Sorting is
# bytewise over the whole line (the same order as ``LC_ALL=C sort``), dedupe and
# join use the bytes before the first tab as the key.
DEFAULT_MAX_RECORD_BYTES = 64 * 1024
SAMPLE_RECORDS = 2000
SAMPLE_MAX_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class Config:
    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 0
    # Hard budget for job-owned temporary bytes. This is the number the
    # ledger refuses to exceed; operators set it below the real free space so
    # that SQLite writes, the Python runtime and other processes keep headroom.
    disk_budget_bytes: int = 16 * 1024 * 1024
    # Budget for in-process sort/join buffers. This is tracked separately from
    # RSS: Python object overhead means RSS is always a little larger.
    memory_budget_bytes: int = 12 * 1024 * 1024
    # What we tell the OS to keep free. The ledger must never reserve the last
    # slice of the volume: tmpfs writes elsewhere on the same filesystem would
    # fail too.
    disk_headroom_bytes: int = 4 * 1024 * 1024
    # Admission never promises more than this fraction of the gap between the
    # budget and current usage. The rest is slack for estimation error.
    reservation_slack: float = 0.20
    # Per-job cap, expressed as a fraction of the disk budget.
    max_job_fraction: float = 0.65
    # A job that under-estimated its output may run into debt above its own cap.
    # Debt is granted only from capacity that no other job has reserved, and is
    # itself capped, so a bad estimate cannot starve the rest of the queue.
    debt_allowance_fraction: float = 0.15
    # Per-job cap as a fraction of total memory budget.
    max_job_memory_fraction: float = 0.50
    # How many jobs may be spilling at the same time.
    max_concurrent_jobs: int = 2
    # "throughput" = FIFO with one active spiller; "fairness" = shortest
    # remaining job first with aging, up to max_concurrent_jobs; "adaptive"
    # switches to fairness while the volume is under pressure.
    scheduler_policy: str = "fairness"
    fairness_aging_seconds: float = 20.0
    pressure_high_watermark: float = 0.80
    # Reject uploads above this size (bytes); keeps the service comfortable on
    # a small demo volume.
    max_upload_bytes: int = 48 * 1024 * 1024
    # A single job's read buffer is the smaller of these two.
    max_read_buffer_bytes: int = 256 * 1024
    min_read_buffer_bytes: int = 4 * 1024
    # Sort runs are allowed to be this many times the per-job memory cap before
    # the planner is asked to choose a different strategy.
    max_runs_per_job: int = 12
    # Synthetic demo generator limits.
    demo_record_bytes: int = 24
    demo_max_records: int = 400_000
    # Event retention; the UI polls, old events are trimmed.
    event_history: int = 2000
    # Leases that guard a job from two workers touching it at once.
    lease_ttl_seconds: float = 15.0
    # A cancelled/failed job's artifacts stay pinned this long so the UI can
    # inspect them, then the reaper may reclaim them.
    tombstone_seconds: float = 60.0
    # How often the reaper reconciles filesystem usage with the ledger.
    reap_interval_seconds: float = 1.0
    # A job that cannot get budget is retried with exponential backoff. The
    # first pause retries after ``park_backoff_seconds``; each further pause
    # doubles it up to ``park_backoff_max_seconds``. Without the cap a huge job
    # would hammer admission once a second forever, and without the growth a
    # job that is genuinely too large would starve the scheduler loop.
    park_backoff_seconds: float = 1.0
    park_backoff_max_seconds: float = 30.0
    # After this many budget pauses with no progress the job is failed with an
    # operator-visible explanation instead of retrying silently forever. A
    # resumed job resets the counter.
    park_attempt_limit: int = 12
    # Fault injection: written by the test suite / UI, not used by normal runs.
    fault_points: tuple[str, ...] = field(default_factory=tuple)

    @property
    def db_path(self) -> Path:
        return self.data_dir / "spillforge.sqlite3"

    @property
    def files_dir(self) -> Path:
        return self.data_dir / "files"

    @property
    def tmp_dir(self) -> Path:
        return self.data_dir / "tmp"

    def ensure_dirs(self) -> None:
        for p in (self.data_dir, self.files_dir, self.tmp_dir):
            p.mkdir(parents=True, exist_ok=True)

    def validate(self) -> None:
        if self.disk_budget_bytes < 1:
            raise ConfigError("disk_budget_bytes must be positive")
        if self.disk_headroom_bytes < 0:
            raise ConfigError("disk_headroom_bytes must be non-negative")
        if not 0 <= self.reservation_slack < 1:
            raise ConfigError("reservation_slack must be in [0, 1)")
        if self.memory_budget_bytes < 1:
            raise ConfigError("memory_budget_bytes must be positive")
        if self.max_upload_bytes < 1:
            raise ConfigError("max_upload_bytes must be positive")
        if not 0 < self.max_job_fraction <= 1:
            raise ConfigError("max_job_fraction must be in (0, 1]")
        if not 0 <= self.debt_allowance_fraction < 1:
            raise ConfigError("debt_allowance_fraction must be in [0, 1)")
        if self.max_concurrent_jobs < 1:
            raise ConfigError("max_concurrent_jobs must be at least 1")
        if self.scheduler_policy not in ("throughput", "fairness", "adaptive"):
            raise ConfigError("scheduler_policy must be throughput, fairness or adaptive")
        if not 0 < self.pressure_high_watermark <= 1:
            raise ConfigError("pressure_high_watermark must be in (0, 1]")
        if not 0 < self.max_job_memory_fraction <= 1:
            raise ConfigError("max_job_memory_fraction must be in (0, 1]")
        if self.park_backoff_seconds <= 0 or self.park_backoff_max_seconds < self.park_backoff_seconds:
            raise ConfigError("park backoff bounds are inconsistent")
        if self.park_attempt_limit < 1:
            raise ConfigError("park_attempt_limit must be at least 1")
        if self.max_read_buffer_bytes < self.min_read_buffer_bytes:
            raise ConfigError("max_read_buffer_bytes below minimum")
        if self.max_runs_per_job < 2:
            raise ConfigError("max_runs_per_job must be at least 2")
        try:
            free = free_disk_bytes(self.data_dir)
        except OSError:
            free = None
        if free is not None and self.disk_budget_bytes > free:
            raise ConfigError(
                f"disk_budget_bytes={self.disk_budget_bytes} exceeds free space "
                f"{free} at {self.data_dir}"
            )

    def to_public_dict(self) -> dict:
        d = {k: v for k, v in asdict(self).items() if k != "data_dir"}
        d["data_dir"] = str(self.data_dir)
        d["db_path"] = str(self.db_path)
        d["disk_budget_bytes"] = self.disk_budget_bytes
        d["disk_headroom_bytes"] = self.disk_headroom_bytes
        return d


def free_disk_bytes(path: Path) -> int:
    vfs = os.statvfs(path)
    return vfs.f_bavail * vfs.f_frsize


def load_config_from_env(data_dir: Path | None = None) -> Config:
    """Build a Config from environment variables, mostly for CLI convenience."""
    root = data_dir or Path(os.environ.get("SPILLFORGE_DATA", "var"))
    if not root.is_absolute():
        root = Path.cwd() / root
    cfg = Config(data_dir=root)
    raw = os.environ.get("SPILLFORGE_CONFIG")
    if raw:
        with open(raw, "r", encoding="utf-8") as fh:
            values = json.load(fh)
        cfg = Config(data_dir=root, **values)
    overrides = {}
    for field_name in Config.__dataclass_fields__:
        env_name = "SPILLFORGE_" + field_name.upper()
        if env_name in os.environ and field_name not in ("data_dir", "fault_points"):
            raw_value = os.environ[env_name]
            typ = type(getattr(cfg, field_name))
            overrides[field_name] = typ(raw_value)
    if "SPILLFORGE_FAULT_POINTS" in os.environ:
        overrides["fault_points"] = tuple(
            p.strip()
            for p in os.environ["SPILLFORGE_FAULT_POINTS"].split(",")
            if p.strip()
        )
    if overrides:
        cfg = Config(**{**asdict(cfg), **overrides, "data_dir": root})
    cfg.validate()
    cfg.ensure_dirs()
    return cfg
