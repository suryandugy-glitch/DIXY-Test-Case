#!/usr/bin/env bash
# Usage:
#   ./run.sh                              normal 200-thread test with DIXY agent
#   ./run.sh --threads=200 --duration=60 --contention=high
#   ./run.sh --deadlock-test              controlled deadlock mode
#   NO_AGENT=1 ./run.sh --save-baseline=base.json     baseline run without the agent
#   ./run.sh --baseline=base.json         compare overhead against the baseline
#
# Environment:
#   DIXY_AGENT_JAR   path to the agent (default ./dixy-agent.jar)
#   DIXY_AGENT_ARGS  agent options appended as -javaagent:jar=ARGS (check DIXY's docs for its syntax)
#   NO_AGENT=1       run without the agent
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p out
javac -d out DixyStressTest.java

AGENT_JAR="${DIXY_AGENT_JAR:-./dixy-agent.jar}"
JVM_OPTS=()
if [[ "${NO_AGENT:-0}" != "1" ]]; then
  if [[ ! -f "$AGENT_JAR" ]]; then
    echo "DIXY agent not found at $AGENT_JAR (set DIXY_AGENT_JAR or use NO_AGENT=1)" >&2
    exit 64
  fi
  if [[ -n "${DIXY_AGENT_ARGS:-}" ]]; then
    JVM_OPTS+=("-javaagent:${AGENT_JAR}=${DIXY_AGENT_ARGS}")
  else
    JVM_OPTS+=("-javaagent:${AGENT_JAR}")
  fi
fi

exec java "${JVM_OPTS[@]}" -Xms512m -Xmx1g -cp out DixyStressTest "$@"
