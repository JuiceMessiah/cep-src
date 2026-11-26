#!/bin/sh
# demo.sh: the whole CEP demo on one laptop (compose.demo.yaml). Linux, macOS, WSL2 or Git Bash.
#
#   ./demo.sh up                 build jar + images, start, wait for Camunda, submit the Flink job
#   ./demo.sh replay [file] [s]  replay a scenario (default scenarios/ourense-2025-08.jsonl, 120 s)
#   ./demo.sh status             Flink jobs, CEP alerts so far, XES files
#   ./demo.sh pais "<event>" <case_key> [--unit amb-1]   fake PAIS event (e.g. DCR "Emergency order")
#   ./demo.sh clean              stop everything and wipe state: instances, topics, logs, XES
#
# A take: ./demo.sh clean && ./demo.sh up && ./demo.sh replay, then work Tasklist.
# UIs: Camunda http://localhost:8088/tasklist and /operate (demo/demo), Flink http://localhost:8081
set -eu
cd "$(dirname "$0")"
DC="docker compose -f compose.demo.yaml"
JAR=flink-job/target/cep-job-1.0.jar

say() { printf '\n== %s\n' "$*"; }

build_jar() {
    say "building the Flink job (Maven in a container; first run downloads dependencies)"
    docker run --rm -u "$(id -u):$(id -g)" -e HOME=/tmp -e MAVEN_CONFIG=/tmp/.m2 \
        -v "$PWD/flink-job":/w -w /w maven:3.9-eclipse-temurin-17 \
        sh -c 'sh test-core.sh && mvn -B -q -Dmaven.repo.local=/w/.m2repo package'
    ls -l "$JAR"
}

wait_for() {  # wait_for <service> <extended regex> <seconds> [regex that means failure]
    i=0
    until $DC logs "$1" 2>/dev/null | grep -qE "$2"; do
        if [ -n "${4:-}" ] && $DC logs "$1" 2>/dev/null | grep -qE "$4"; then
            echo "  FAILED: $1 logged '$4':"; $DC logs "$1" 2>/dev/null | grep -E "$4" | tail -3; exit 1
        fi
        i=$((i + 5))
        if [ "$i" -ge "$3" ]; then echo "  timeout waiting for '$2' in $1 logs"; $DC logs --tail 20 "$1"; exit 1; fi
        sleep 5
    done
    echo "  $1 ready"
}

case "${1:-}" in
up)
    [ -f "$JAR" ] || build_jar
    mkdir -p XES logs
    say "starting the stack"
    $DC up -d --build
    say "waiting for Camunda + bridge (1-3 min on a cold start)"
    wait_for bridge '\[bridge\] deployed ' 300 'DEPLOYMENT REJECTED'
    wait_for jobmanager 'Rest endpoint listening' 120
    say "submitting the CEP job (replay topics, fresh consumer group)"
    out=$($DC exec -T jobmanager /opt/flink/bin/flink run -d /jobs/cep-job-1.0.jar \
        --source replay --group "demo-$(date +%s)" 2>&1) || true
    printf '%s\n' "$out" | grep -v 'Unknown module' || true
    printf '%s\n' "$out" | grep -q 'Job has been submitted' || { echo "  job submission FAILED"; exit 1; }
    say "ready: ./demo.sh replay  |  Tasklist http://localhost:8088/tasklist (demo/demo)"
    ;;
replay)
    f="${2:-scenarios/ourense-2025-08.jsonl}"
    $DC run --rm replayer "/scenarios/$(basename "$f")" --duration "${3:-120}"
    ;;
status)
    say "Flink jobs"
    $DC exec -T jobmanager /opt/flink/bin/flink list 2>/dev/null | grep -E 'RUNNING|RESTARTING|No running' || true
    say "CEP alerts"
    $DC logs taskmanager 2>/dev/null | grep -o 'CEP .*' || echo "  none yet"
    say "XES files"
    ls -l XES 2>/dev/null || echo "  none yet"
    ;;
pais)
    shift
    python3 tools/send_pais.py "$@"
    ;;
clean)
    say "stopping and wiping (process instances, topics, bridge state, logs, XES)"
    $DC --profile tools --profile live down -v --remove-orphans
    rm -f XES/* logs/* 2>/dev/null || true
    ;;
build)
    build_jar
    $DC build
    ;;
*)
    sed -n '2,11p' "$0"
    exit 2
    ;;
esac
