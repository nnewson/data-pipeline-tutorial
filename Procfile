# Console scripts directly, not `uv run <script>`. Each `uv run` inserts a
# wrapper process, so Honcho supervises the wrapper rather than the process
# doing the work.
#
# On its own that is survivable: Honcho forwards termination to each child's
# process group, wrapper included, and nothing is left behind. It leaks when
# combined with launching Honcho itself through `uv run` — that pairing was
# measured leaving every process running. Removing the layer here means no
# future combination can reintroduce it.
#
# Honcho runs each entry in a process group of its own; they are not members of
# Honcho's group, which is why the smoke test captures their group ids rather
# than checking Honcho's alone.
producer: .venv/bin/producer
consumer_1: .venv/bin/consumer
consumer_2: .venv/bin/consumer
consumer_3: .venv/bin/consumer
consumer_4: .venv/bin/consumer
# Four workers competing for ONE queue, unlike the consumers above which each
# own a Kafka partition. A fifth worker adds capacity, and can raise throughput
# while there is a backlog to work through; a fifth consumer would simply sit
# idle, because a partition has at most one consumer in a group.
worker_1: .venv/bin/worker
worker_2: .venv/bin/worker
worker_3: .venv/bin/worker
worker_4: .venv/bin/worker
# Three coordinators competing for one leadership. Only the winner writes the
# snapshot, and only while its epoch is still current.
coordinator_1: .venv/bin/coordinator
coordinator_2: .venv/bin/coordinator
coordinator_3: .venv/bin/coordinator
# One API process, reading all three stores. uvicorn runs in-process: no
# --reload and no --workers, either of which would put a supervisor of its own
# inside the one Honcho already provides.
api: .venv/bin/api
