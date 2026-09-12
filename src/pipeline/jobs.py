"""What the workers actually did.

Queue depth says how much work is waiting; it cannot say how much has completed,
because acknowledged messages are gone. Completions are counted in Redis by the
workers instead — see `redis_store.record_execution`, including why that count
is itself not ground truth.
"""

import logging

from pipeline import jobs_queue, redis_store

logger = logging.getLogger("jobs")


def main() -> int:
    client = redis_store.connect()
    try:
        completed, runs = redis_store.job_summary(client)
    finally:
        client.close()

    try:
        waiting, consumers = jobs_queue.queue_state()
    except Exception as error:  # noqa: BLE001 - reported, not swallowed
        print(f"  queue unavailable: {error}")
        waiting = consumers = "?"

    print(f"  waiting     {waiting}")
    print(f"  workers     {consumers}")
    print(f"  completed   {completed}")
    print(f"  distinct    {len(runs)}")

    repeated = {event: count for event, count in runs.items() if count > 1}
    if repeated:
        print(f"\n  {len(repeated)} event(s) ran more than once:")
        for event_id, count in sorted(repeated.items())[:10]:
            print(f"    {event_id}  x{count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
