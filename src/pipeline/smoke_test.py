"""Bounded end-to-end check of the topology described by docker-compose.yml.

Compose answers "which services run"; this answers "are they actually working".
Keeping the assertions here rather than in the CI workflow means the workflow
stays a caller and the success criteria stay reviewable code.
"""

import subprocess
import sys
import time
import urllib.error
from collections.abc import Callable

from pipeline import config
from pipeline.check_web import fetch

# Every check is bounded, so a broken topology fails rather than hangs.
READY_TIMEOUT_SECONDS = 30
READY_POLL_SECONDS = 1
COMMAND_TIMEOUT_SECONDS = 120

CONTAINER_CHECK_COMMAND = [
    "docker",
    "compose",
    "exec",
    "-T",
    "shell",
    "uv",
    "run",
    "check-web",
]


def wait_for(probe: Callable[[], bool], timeout: float = READY_TIMEOUT_SECONDS) -> bool:
    """Poll probe until it returns True, or the timeout expires."""
    deadline = time.monotonic() + timeout
    while True:
        if probe():
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(READY_POLL_SECONDS)


def host_path_serves_the_published_port() -> tuple[bool, str]:
    """The service is reachable from the host at its published port."""

    def probe() -> bool:
        try:
            status, _ = fetch(config.WEB_URL)
        except (urllib.error.URLError, OSError):
            return False
        return status == 200

    if not wait_for(probe):
        return False, f"{config.WEB_URL} did not serve 200 within the timeout"
    return True, f"{config.WEB_URL} served 200"


def container_path_serves_the_network_alias() -> tuple[bool, str]:
    """The same service is reachable from inside the network by hostname."""
    try:
        result = subprocess.run(
            CONTAINER_CHECK_COMMAND,
            check=False,
            capture_output=True,
            text=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
    except subprocess.TimeoutExpired:
        return False, "the container check did not finish within the timeout"
    except FileNotFoundError:
        return False, "docker is not available on PATH"

    if result.returncode != 0:
        return False, f"the container check failed: {result.stderr.strip()}"

    output = result.stdout.strip()
    if "http://web -> 200" not in output:
        return False, f"unexpected container output: {output!r}"
    return True, "http://web served 200 from inside the network"


CHECKS: list[tuple[str, Callable[[], tuple[bool, str]]]] = [
    ("host path", host_path_serves_the_published_port),
    ("container path", container_path_serves_the_network_alias),
]


def main() -> int:
    failures = 0
    for name, check in CHECKS:
        passed, detail = check()
        print(f"{'PASS' if passed else 'FAIL'}  {name}: {detail}")
        if not passed:
            failures += 1

    if failures:
        print(f"\n{failures} of {len(CHECKS)} checks failed", file=sys.stderr)
        return 1

    print(f"\nall {len(CHECKS)} checks passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
