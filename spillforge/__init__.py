"""SpillForge — disk-aware external sort service with an explicit resource ledger.

The package is intentionally dependency-free: SQLite for durable state, the
Python standard library for the executor, and a small JSON/HTTP server for the
operator UI.
"""

__version__ = "0.1.0"
