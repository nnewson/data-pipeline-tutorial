"""Settings that can change while the process runs.

Nothing in this project was live before 0.7. Values like WORKER_DELAY_SECONDS
were read once at import and captured as default arguments, so a ZooKeeper watch
would have changed nothing — a demonstration that only appeared to work because
the process had been restarted.

This holds the mutable copy. A worker reads it as it *begins* a job, so a change
takes effect on the next job rather than the next restart.
"""

import logging
import threading

logger = logging.getLogger("runtime-config")

# A value that fails validation is refused, and the last known-good one kept.
# Validating in `cluster set-config` is not enough: anyone can write the znode
# directly with zkCli, so each process validates what it reads.
MIN_WORKER_DELAY = 0.0
MAX_WORKER_DELAY = 60.0


class InvalidSetting(ValueError):
    """A published value was rejected, so the previous one still applies."""


def validate_worker_delay(raw: object) -> float:
    try:
        value = float(raw)
    except (TypeError, ValueError) as error:
        raise InvalidSetting(f"worker delay {raw!r} is not a number") from error
    if not MIN_WORKER_DELAY <= value <= MAX_WORKER_DELAY:
        raise InvalidSetting(
            f"worker delay {value} outside {MIN_WORKER_DELAY}-{MAX_WORKER_DELAY}"
        )
    return value


class RuntimeConfig:
    """Thread-safe current settings, plus the config version they came from."""

    def __init__(self, worker_delay: float) -> None:
        self._lock = threading.Lock()
        self._worker_delay = worker_delay
        self._version = -1

    @property
    def worker_delay(self) -> float:
        with self._lock:
            return self._worker_delay

    @property
    def version(self) -> int:
        """The znode version this value came from, or -1 for the startup value.

        Reported in each worker's registration, so a test can prove every worker
        applied a change rather than inferring it from aggregate behaviour.
        """
        with self._lock:
            return self._version

    def apply(self, raw: object, version: int) -> float:
        """Validate and adopt a published value, or keep the current one."""
        value = validate_worker_delay(raw)
        with self._lock:
            previous = self._worker_delay
            self._worker_delay = value
            self._version = version
        if previous != value:
            logger.info(
                f"worker delay {previous} -> {value} (config version {version})"
            )
        return value
