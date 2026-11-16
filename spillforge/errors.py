"""Exception hierarchy for the SpillForge executor and ledger."""


class SpillForgeError(Exception):
    """Base class for all errors raised by SpillForge."""


class ConfigError(SpillForgeError):
    """Invalid configuration (budget, limits, paths)."""


class LedgerError(SpillForgeError):
    """Base class for resource-ledger failures."""


class BudgetDenied(LedgerError):
    """A reservation would exceed the configured hard budget."""

    def __init__(self, job_id, kind, requested, available, reason):
        self.job_id = job_id
        self.kind = kind
        self.requested = requested
        self.available = available
        self.reason = reason
        super().__init__(
            f"budget denied for job={job_id} kind={kind} "
            f"requested={requested} available={available}: {reason}"
        )


class QuotaExceeded(BudgetDenied):
    """A job tried to exceed its own share of the budget."""


class BudgetInfeasible(QuotaExceeded):
    """The plan cannot fit any achievable budget; waiting will never help.

    Raised before any temp bytes are written, so the job fails with a concrete
    explanation instead of spilling until the volume is nearly full and then
    discovering the same fact.
    """


class LeaseExpired(LedgerError):
    """A lease was used after expiry or by a different owner."""


class UnknownLease(LeaseExpired):
    """The lease token is not registered in the ledger."""


class ProtectedArtifact(LedgerError):
    """Cleanup attempted to delete an artifact that is still needed."""


class SourceNotFound(SpillForgeError):
    """The submitted source file is missing or unreadable."""


class CorruptJobState(SpillForgeError):
    """Persisted job state is internally inconsistent."""
