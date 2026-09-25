"""Coordination tree: create it, and look at it.

`cluster init` is the one path that creates persistent znodes, as `create-topics`
owns topics and `create-schema` owns the keyspace. Coordinators, consumers and
workers wait for it rather than inventing persistent state of their own.

Re-running it is safe and never resets `/epoch` or live configuration —
resetting the epoch would silently un-fence every stale leader.
"""

import argparse
import json
import logging

from pipeline import coordination
from pipeline.config import WORKER_DELAY_SECONDS
from pipeline.coordination import Paths
from pipeline.runtime_config import validate_worker_delay

logger = logging.getLogger("cluster")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inspect coordination state.")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("init", help="create the persistent tree (idempotent)")
    sub.add_parser("status", help="leader, registrations, config, snapshot")
    set_config = sub.add_parser("set-config", help="publish a live setting")
    set_config.add_argument("--worker-delay", type=float, required=True)
    return parser.parse_args(argv)


def show_status(client, paths: Paths) -> None:
    leader = coordination.read_leader(client, paths)
    print(f"  leader      {leader['identity'] if leader else '(none)'}")
    if leader:
        print(f"  epoch       {leader['epoch']}")

    contenders = (
        client.get_children(paths.election) if client.exists(paths.election) else []
    )
    print(f"  contenders  {len(contenders)}")

    for role in ("coordinator", "consumer", "worker"):
        entries = coordination.registrations(client, paths, role)
        print(f"  {role + 's':<11} {len(entries)}")
        versions = {e.get("config_version") for e in entries if "config_version" in e}
        if versions:
            print(f"    config versions applied: {sorted(versions)}")

    delay, stat = client.get(paths.worker_delay)
    print(
        f"  worker_delay {delay.decode() if delay else '(unset)'} "
        f"(version {stat.version})"
    )

    snapshot, version = coordination.read_snapshot(client, paths)
    print(f"  snapshot    version {version}: {json.dumps(snapshot)[:120]}")


def main() -> int:
    arguments = parse_args()
    client = coordination.connect()
    paths = Paths()
    try:
        if arguments.command == "init":
            coordination.initialise(client, paths, WORKER_DELAY_SECONDS)
        elif arguments.command == "set-config":
            # Validated here and again by every process that reads it: this
            # command is not the only way the znode can be written.
            value = validate_worker_delay(arguments.worker_delay)
            client.set(paths.worker_delay, str(value).encode())
            logger.info(f"published worker_delay={value}")
        else:
            show_status(client, paths)
    finally:
        client.stop()
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
