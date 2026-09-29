#!/usr/bin/env bash
# Download the Prometheus JMX exporter agent used by tests/docker-compose.test.yml.
# The jar is gitignored; run this once before the e2e or perf tests.
set -euo pipefail
VERSION="${JMX_AGENT_VERSION:-1.0.1}"
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
URL="https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${VERSION}/jmx_prometheus_javaagent-${VERSION}.jar"
if [[ -s "$DIR/agent.jar" ]]; then
  echo "agent.jar already present"
  exit 0
fi
curl -fsSL -o "$DIR/agent.jar" "$URL"
echo "downloaded $URL -> $DIR/agent.jar"
