#!/usr/bin/env bash
#
# Reset the demo Kafka topics and (re)start the streams app as a container.
#
# Order matters: streams must be stopped before its topics are deleted,
# otherwise it holds them open and re-creates offsets against the old topic.
#
#   1. stop the streams container AND any stray local `sbt streams/run`
#   2. delete the data topics + any streams internal topics
#   3. recreate the topics with a fixed partition count (via create-topics.sh)
#   4. start streams as a container
#
# Usage:
#   scripts/reset-topics.sh            # reuse the existing streams image
#   scripts/reset-topics.sh --build    # rebuild the streams jar + image first
#                                       # (needed after changing streams code)
#
# Overridable via env: KAFKA_SVC, STREAMS_SVC, BOOTSTRAP, PARTITIONS, APP_ID.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

KAFKA_SVC="${KAFKA_SVC:-kafka}"
STREAMS_SVC="${STREAMS_SVC:-streams}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
PARTITIONS="${PARTITIONS:-6}"
APP_ID="${APP_ID:-trading-streams}"
TOPICS=("trading-events" "trading-events-processed" "advisories" "symbols")

BUILD=0
[[ "${1:-}" == "--build" ]] && BUILD=1

compose() { docker compose "$@"; }
# kafka-topics.sh runs *inside* the broker container, so localhost:9092 (the
# EXTERNAL listener) is reachable there.
kt() { compose exec -T "$KAFKA_SVC" /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" "$@"; }

echo "==> checking the kafka broker is up"
if ! compose exec -T "$KAFKA_SVC" true 2>/dev/null; then
  echo "    '$KAFKA_SVC' is not running. Start it with: docker compose up -d $KAFKA_SVC" >&2
  exit 1
fi

echo "==> stopping streams (container + any local 'sbt streams/run')"
compose stop "$STREAMS_SVC" >/dev/null 2>&1 || true
# Stop the local forked JVM if you started it via sbt. Match the java process
# only, so an editor with StreamsApp.scala open is not caught.
pkill -f 'java .*StreamsApp' 2>/dev/null && echo "    killed a local StreamsApp fork" || true

echo "==> listing existing topics"
existing="$(kt --list 2>/dev/null || true)"

delete_topic() {
  local t="$1"
  if grep -qx "$t" <<<"$existing"; then
    kt --delete --topic "$t" >/dev/null && echo "    deleted $t"
  fi
}

echo "==> deleting data topics (incl. advisories + the symbol registry)"
for t in "${TOPICS[@]}"; do delete_topic "$t"; done

echo "==> deleting streams internal topics ('${APP_ID}-*', if any)"
while IFS= read -r t; do
  [[ -n "$t" ]] && { kt --delete --topic "$t" >/dev/null && echo "    deleted internal $t"; }
done < <(grep "^${APP_ID}-" <<<"$existing" || true)

echo "==> waiting for deletions to finalize"
for t in "${TOPICS[@]}"; do
  for _ in $(seq 1 30); do
    kt --list 2>/dev/null | grep -qx "$t" || break
    sleep 1
  done
done

# Delegate to create-topics.sh (piped into the broker container) so the reset
# path and the kafka-init service create topics with identical configs -- the
# compacted `symbols` registry in particular cannot be recreated with a plain
# --create.
echo "==> recreating topics via scripts/create-topics.sh"
compose exec -T -e BOOTSTRAP="$BOOTSTRAP" -e PARTITIONS="$PARTITIONS" \
  "$KAFKA_SVC" bash -s <"$ROOT_DIR/scripts/create-topics.sh"

if [[ "$BUILD" == "1" ]]; then
  echo "==> rebuilding streams jar + image"
  sbt -batch streams/assembly
  compose build "$STREAMS_SVC"
fi

echo "==> starting streams container"
compose up -d "$STREAMS_SVC"

echo "==> done. Follow logs with: docker compose logs -f $STREAMS_SVC"
