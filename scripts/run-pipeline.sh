#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$ROOT_DIR"

if ! compgen -G "$ROOT_DIR/data/*.csv" > /dev/null; then
  echo "No CSV files found in data/. Run ./scripts/download-data.sh or add daily CSV files first." >&2
  exit 1
fi

: "${REPLAY_SPEED:=0}"
export REPLAY_SPEED

echo "==> building producer, streams, and consumer jars"
sbt -batch producer/assembly streams/assembly consumer/assembly

echo "==> starting the Kafka pipeline (replay speed: ${REPLAY_SPEED}x)"
docker compose up --build -d

echo "==> pipeline started; UI: http://localhost:8501"
echo "==> follow logs: docker compose logs -f producer streams consumer"