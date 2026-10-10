# Flink, pinned hard. The most version-sensitive software in the tutorial, and
# the most likely to break for a reader arriving late, so every input is fixed:
#
#   base image   flink 2.2.1, Java 17, Ubuntu 24.04, by its multi-arch INDEX
#                digest — not a platform manifest's, which would pin amd64 (CI)
#                or arm64 (a Mac) and break the other. The tag alone is not a
#                pin: official images are rebuilt when their base changes, and
#                the 2.2.1 tags were last moved on 2026-10-02.
#   Python       Ubuntu 24.04's python3, so 3.12. The minor version is fixed by
#                the base; the patch level is whatever the Ubuntu archive serves
#                at build time. Pinning the apt version would break the build
#                the day the archive drops it, so it is not pretended.
#   PyFlink      the copy the image ships in /opt/flink/opt/python, not a pip
#                install. On 3.12 it needs typing_extensions to import and
#                ruamel.yaml to execute statements (it parses list-valued config
#                with it) — an import-only check misses the second.
#   connector    Kafka SQL connector 5.0.0-2.2, the version Apache lists as
#                compatible with Flink 2.1.x and 2.2.x. There is none for 2.3,
#                which is why this is 2.2.1 and not 2.3.0.
#
# The connector, verified once and then pinned by hash. Maven Central publishes
# only SHA-1 and a PGP signature for it — no SHA-256 or SHA-512 — so:
#
#   URL          https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
#   signature    good, by Ferenc Csaky (CODE SIGNING KEY) <fcsaky@apache.org>
#   fingerprint  CC33 2388 50B5 A926 24ED  7F62 16AE 0DDB BB2F 380B
#   keys         https://downloads.apache.org/flink/KEYS
#   verified     2026-10-09, GnuPG 2.4.4
#   SHA-256      5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616
#
# Every build checks those exact bytes. Changing the connector means repeating
# the signature check and recording the new values here.

FROM flink:2.2.1-java17@sha256:140c3909f06cbea741be78fc630e809fae9dadcbfa492a684acf39efbbf68946

ARG CONNECTOR_URL=https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar
ARG CONNECTOR_SHA256=5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3 python3-typing-extensions python3-ruamel.yaml \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka.jar "$CONNECTOR_URL" \
    && echo "$CONNECTOR_SHA256  /opt/flink/lib/flink-sql-connector-kafka.jar" | sha256sum -c -

# Created here, owned by flink, so the named volume mounted over it starts out
# writable by the user Flink runs as.
RUN mkdir -p /opt/flink/checkpoints && chown flink:flink /opt/flink/checkpoints

# The job is part of the image: what the smoke test runs is what ships.
COPY --chown=flink:flink src/pipeline/flink_job.py /opt/pipeline/flink_job.py

USER flink
