#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Run 1 baseline launcher: executes all 9 isolated (driver x protocol x bucketType)
# benchmark suites (WL1..WL7b) on GCE (`gcs-benchmark-client`) with 16-client Native
# Java pool and default UDS settings (`--grpc-subchannels 0`, 1 local UDS channel).

set -euo pipefail

BENCH_ROOT="${BENCH_ROOT:-$HOME/bench-v2}"
JAVA_MOD="$BENCH_ROOT/google-cloud-java/java-storage/google-cloud-storage"
RESULTS_DIR="$BENCH_ROOT/results"
JAVA_BIN="${JAVA_BIN:-/usr/lib/jvm/java-21-openjdk-amd64/bin/java}"
SIDECAR_BIN="$BENCH_ROOT/google-cloud-rust/target/release/gcs-core-server"
INPROC_LIB="$BENCH_ROOT/google-cloud-rust/target/release/libstorage_grpc_uds.so"
CP_FILE="$BENCH_ROOT/cp.txt"

mkdir -p "$RESULTS_DIR"
if [[ ! -s "$CP_FILE" ]]; then
  (cd "$JAVA_MOD" && mvn -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile="$CP_FILE")
fi
CP="$JAVA_MOD/target/test-classes:$JAVA_MOD/target/classes:$(cat "$CP_FILE")"
INST_ID="$(curl -fsSL -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/id)"

JVM_OPTS=(
  -Xms8g
  -Xmx32g
  -XX:+UseG1GC
  -Dgcs.grpc.directpath=false
  -Dgcs.native.channel.pool.size=16
)

cleanup_stray() {
  pkill -f "$SIDECAR_BIN" 2>/dev/null || true
}

on_err() {
  local rc=$?
  echo "[ORCHESTRATOR] ERROR (exit=$rc) at $(date -u +%Y-%m-%dT%H:%M:%SZ)" | tee "$RESULTS_DIR/FAILED"
  cleanup_stray
  exit "$rc"
}
trap on_err ERR

echo "[ORCHESTRATOR] Started 9-suite live benchmark run at $(date -u +%Y-%m-%dT%H:%M:%SZ) on instance_id=$INST_ID"

SUITES=(
  "grpc-regional-native|native|GRPC|regional"
  "grpc-regional-uds-sidecar|uds-sidecar|GRPC|regional"
  "grpc-regional-uds-inproc|uds-inproc|GRPC|regional"
  "grpc-zonal-native|native|GRPC|zonal"
  "grpc-zonal-uds-sidecar|uds-sidecar|GRPC|zonal"
  "grpc-zonal-uds-inproc|uds-inproc|GRPC|zonal"
  "json-regional-native|native|JSON|regional"
  "json-regional-uds-sidecar|uds-sidecar|JSON|regional"
  "json-regional-uds-inproc|uds-inproc|JSON|regional"
)

idx=0
for spec in "${SUITES[@]}"; do
  idx=$((idx + 1))
  IFS="|" read -r name driver proto btype <<< "$spec"
  out_json="$RESULTS_DIR/${name}.json"
  out_log="$RESULTS_DIR/${name}.log"

  if [ -s "$out_json" ]; then
    echo "[ORCHESTRATOR] [$idx/9] Skipping already-completed $name ($out_json exists)"
    continue
  fi

  cleanup_stray
  echo "[ORCHESTRATOR] [$idx/9] Pre-suite 75s zero-I/O quiescence sleep before $name at $(date -u +%Y-%m-%dT%H:%M:%SZ)..."
  sleep 75

  echo "[ORCHESTRATOR] [$idx/9] Launching isolated JVM for $name (driver=$driver protocol=$proto bucketType=$btype) at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  "$JAVA_BIN" "${JVM_OPTS[@]}" -cp "$CP" \
    com.google.cloud.storage.sidecar.benchmark.BenchmarkSuiteRunner \
    --driver="$driver" \
    --protocol="$proto" \
    --bucket-type="$btype" \
    --project=vaibhavpratap-sdk-test \
    --quiescence-sec=75 \
    --sidecar-bin="$SIDECAR_BIN" \
    --inproc-lib="$INPROC_LIB" \
    --output-json="$out_json" \
    > "$out_log" 2>&1

  cleanup_stray
  echo "[ORCHESTRATOR] [$idx/9] Completed $name at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
done

echo "[ORCHESTRATOR] All 9 suites finished! Waiting 120s for Cloud Monitoring flush and generating Markdown report..."
"$JAVA_BIN" -Xms1g -Xmx4g -cp "$CP" \
  com.google.cloud.storage.sidecar.benchmark.BenchmarkReportCli \
  --results-dir="$RESULTS_DIR" \
  --project=vaibhavpratap-sdk-test \
  --instance-id="$INST_ID" \
  --wait-flush-sec=120 \
  --output-md="$RESULTS_DIR/benchmark_report.md" \
  > "$RESULTS_DIR/report_cli.log" 2>&1

date -u +%Y-%m-%dT%H:%M:%SZ > "$RESULTS_DIR/ALL_DONE"
echo "[ORCHESTRATOR] ALL_DONE at $(cat "$RESULTS_DIR/ALL_DONE")"
