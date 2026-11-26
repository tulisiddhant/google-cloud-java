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
# Additional Run launcher (`sym48`): executes all 9 isolated suites across WL3, WL5,
# and WL6 with matched 48 upstream TCP connections (`--native-pool-size=48`,
# `--grpc-subchannels=48`), a 16-channel local AF_UNIX Netty pool (`--uds-channels=16`),
# and 48-thread WL3 JSON concurrency (`-Dwl3.json.threads=48`).

set -euo pipefail

BENCH_ROOT="${BENCH_ROOT:-$HOME/bench-v2}"
JAVA_MOD="$BENCH_ROOT/google-cloud-java/java-storage/google-cloud-storage"
RUST_DIR="$BENCH_ROOT/google-cloud-rust"
SIDECAR_BIN="$RUST_DIR/target/release/gcs-core-server"
INPROC_LIB="$RUST_DIR/target/release/libstorage_grpc_uds.so"
OUT_DIR="$BENCH_ROOT/results-sym48"
CP_FILE="$OUT_DIR/classpath.txt"

mkdir -p "$OUT_DIR"

export JAVA_HOME="${JAVA_HOME:-/usr/lib/jvm/java-21-openjdk-amd64}"
JAVA_BIN="$JAVA_HOME/bin/java"

cd "$JAVA_MOD"
if [[ ! -s "$CP_FILE" ]]; then
  mvn -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile="$CP_FILE"
fi
FULL_CP="$JAVA_MOD/target/test-classes:$JAVA_MOD/target/classes:$(cat "$CP_FILE")"

JVM_OPTS=(
  -Xms8g
  -Xmx32g
  -XX:+UseG1GC
  -Dgcs.grpc.directpath=false
  -Dgcs.native.disable.grpc.metrics=true
  -Dwl3.json.threads=48
)

SUITES=(
  "grpc|regional|native|grpc-regional-native"
  "grpc|regional|uds-sidecar|grpc-regional-uds-sidecar"
  "grpc|regional|uds-inproc|grpc-regional-uds-inproc"
  "grpc|zonal|native|grpc-zonal-native"
  "grpc|zonal|uds-sidecar|grpc-zonal-uds-sidecar"
  "grpc|zonal|uds-inproc|grpc-zonal-uds-inproc"
  "json|regional|native|json-regional-native"
  "json|regional|uds-sidecar|json-regional-uds-sidecar"
  "json|regional|uds-inproc|json-regional-uds-inproc"
)

echo "=== Starting 9 Matched-Symmetry Suites (48 Upstream Conns + 16 Local UDS Channels) at $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="

TOTAL="${#SUITES[@]}"
IDX=0
for entry in "${SUITES[@]}"; do
  IDX=$((IDX + 1))
  IFS='|' read -r proto btype drv name <<< "$entry"

  if [[ "$IDX" -gt 1 ]]; then
    echo "[INTER-SUITE QUIESCENCE] Sleeping 75s before [$IDX/$TOTAL] $name..."
    sleep 75
  fi

  echo "=== [$IDX/$TOTAL] Launching $name at $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  "$JAVA_BIN" "${JVM_OPTS[@]}" -cp "$FULL_CP" \
    com.google.cloud.storage.sidecar.benchmark.BenchmarkSuiteRunner \
    --driver="$drv" \
    --protocol="$proto" \
    --bucket-type="$btype" \
    --project="vaibhavpratap-sdk-test" \
    --workloads="WL3,WL5,WL6" \
    --native-pool-size=48 \
    --grpc-subchannels=48 \
    --uds-channels=16 \
    --quiescence-sec=75 \
    --sidecar-bin="$SIDECAR_BIN" \
    --inproc-lib="$INPROC_LIB" \
    --output-json="$OUT_DIR/${name}.json" \
    2>&1 | tee "$OUT_DIR/${name}.log"
  echo "=== [$IDX/$TOTAL] Completed $name at $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
done

echo "=== Generating Cloud Monitoring Report (120s flush wait) at $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
"$JAVA_BIN" -Xms2g -Xmx8g -cp "$FULL_CP" \
  com.google.cloud.storage.sidecar.benchmark.BenchmarkReportCli \
  --results-dir="$OUT_DIR" \
  --project="vaibhavpratap-sdk-test" \
  --wait-flush-sec=120 \
  --output-md="$OUT_DIR/benchmark_report_sym48.md" \
  2>&1 | tee "$OUT_DIR/report_cli.log"

echo "ALL_DONE at $(date -u +%Y-%m-%dT%H:%M:%SZ)"
