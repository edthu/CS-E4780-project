#!/usr/bin/env bash
#
# Create the pipeline's Kafka topics with explicit partition counts and configs.
#
# This exists because auto-creation cannot express what we need:
#   - auto-created topics get the broker default of 1 partition, which caps the
#     parallelism of the streams app at one task regardless of how many replicas
#     are running;
#   - auto-created topics are delete-retention, but `symbols` must be COMPACTED
#     so the UI can replay a bounded snapshot of the registry instead of the
#     whole history.
#
# Runs both as the `kafka-init` Compose service and from reset-topics.sh, so the
# two paths cannot drift.
#
# Overridable via env: BOOTSTRAP, PARTITIONS, REPLICATION, KAFKA_BIN,
#                      DATA_TOPICS, SYMBOL_TOPIC.

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:19092}"
PARTITIONS="${PARTITIONS:-6}"
REPLICATION="${REPLICATION:-1}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin}"
SYMBOL_TOPIC="${SYMBOL_TOPIC:-symbols}"
read -r -a DATA_TOPICS <<<"${DATA_TOPICS:-trading-events trading-events-processed advisories}"

kt() { "$KAFKA_BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" "$@"; }

echo "==> waiting for the broker at $BOOTSTRAP"
for attempt in $(seq 1 60); do
  if kt --list >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" == "60" ]]; then
    echo "    broker did not become reachable in time" >&2
    exit 1
  fi
  sleep 2
done

echo "==> creating data topics ($PARTITIONS partitions, RF $REPLICATION)"
for t in "${DATA_TOPICS[@]}"; do
  kt --create --topic "$t" \
     --partitions "$PARTITIONS" \
     --replication-factor "$REPLICATION" \
     --if-not-exists >/dev/null
  echo "    $t"
done

# Keyed by symbol, so compaction collapses the log to one record per symbol:
# retained size tracks symbol cardinality (~5.5k), not the 289M-event stream.
# The aggressive segment/dirty-ratio settings matter because this topic is tiny
# and low-traffic -- with the defaults the active segment would never roll and a
# cold UI would replay uncompacted duplicates.
echo "==> creating compacted symbol registry '$SYMBOL_TOPIC'"
kt --create --topic "$SYMBOL_TOPIC" \
   --partitions "$PARTITIONS" \
   --replication-factor "$REPLICATION" \
   --config cleanup.policy=compact \
   --config min.cleanable.dirty.ratio=0.1 \
   --config segment.ms=60000 \
   --config min.compaction.lag.ms=0 \
   --if-not-exists >/dev/null
echo "    $SYMBOL_TOPIC (cleanup.policy=compact)"

echo "==> topics ready"
kt --list | sed 's/^/    /'
