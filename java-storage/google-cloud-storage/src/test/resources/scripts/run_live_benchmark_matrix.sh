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
# Executes Gate 2 (Bucket Fixture & Token/Cleanup Validation) and Gate 3
# (Full Standardized Live Cloudpath Benchmark Matrix + Cloud Monitoring Extraction)
# on the GCE benchmark client (`gcs-benchmark-client`, `c4-standard-192` in `us-central1-b`).
#
# Usage:
#   1. From Cloudtop (after `gcloud auth login`):
#      ./run_live_benchmark_matrix.sh --from-cloudtop
#
#   2. Directly on `gcs-benchmark-client`:
#      ./run_live_benchmark_matrix.sh

set -euo pipefail

PROJECT_ID="${PROJECT_ID:-vaibhavpratap-sdk-test}"
VM_NAME="${VM_NAME:-gcs-benchmark-client}"
VM_ZONE="${VM_ZONE:-us-central1-b}"
REGIONAL_BUCKET="${REGIONAL_BUCKET:-siddhanttuli-java-uds-bench-regional}"
ZONAL_BUCKET="${ZONAL_BUCKET:-siddhanttuli-java-uds-bench-zonal}"

PAUSE_SEC="${PAUSE_SEC:-75}"
MIN_WINDOW_SEC="${MIN_WINDOW_SEC:-120}"
DURATION_SEC="${DURATION_SEC:-300}"
POST_WAIT_SEC="${POST_WAIT_SEC:-120}"
NATIVE_POOL_SIZE="${NATIVE_POOL_SIZE:-16}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODULE_DIR="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
REPO_ROOT="$(cd "${MODULE_DIR}/../.." && pwd)"

if [[ "${1:-}" == "--from-cloudtop" ]]; then
  shift
  LOCAL_RUST_DIR="${LOCAL_RUST_DIR:-${HOME}/gits/java-uds-benchmark/google-cloud-rust}"
  SSH_KEY="${SSH_KEY:-${HOME}/.ssh/google_compute_engine}"
  VM_SSH_USER="${VM_SSH_USER:-$(whoami)_google_com}"
  VM_SSH_HOST="${VM_SSH_HOST:-${VM_SSH_USER}@nic0.${VM_NAME}.${VM_ZONE}.c.${PROJECT_ID}.internal.gcpnode.com}"
  SSH_OPTS=(-o IdentitiesOnly=yes -o BatchMode=yes -i "${SSH_KEY}")

  echo "[Cloudtop] Verifying active gcloud authentication for ${PROJECT_ID}..."
  gcloud compute instances describe "${VM_NAME}" \
    --project="${PROJECT_ID}" \
    --zone="${VM_ZONE}" \
    --format="value(id,machineType,status)"

  echo "[Cloudtop] Uploading release Rust UDS artifacts to ${VM_SSH_HOST}:~/bin/..."
  ssh "${SSH_OPTS[@]}" "${VM_SSH_HOST}" "mkdir -p ~/bin ~/workspace"
  scp "${SSH_OPTS[@]}" \
    "${LOCAL_RUST_DIR}/target/release/gcs-core-server" \
    "${LOCAL_RUST_DIR}/target/release/libstorage_grpc_uds.so" \
    "${VM_SSH_HOST}:~/bin/"

  echo "[Cloudtop] Syncing tulisiddhant/google-cloud-java (grpc-uds-java-inproc) on ${VM_NAME}..."
  ssh "${SSH_OPTS[@]}" "${VM_SSH_HOST}" "
    set -euo pipefail
    chmod +x ~/bin/gcs-core-server ~/bin/libstorage_grpc_uds.so
    if [[ ! -d ~/workspace/google-cloud-java-remediated ]]; then
      git clone --branch grpc-uds-java-inproc --single-branch \
        https://github.com/tulisiddhant/google-cloud-java.git \
        ~/workspace/google-cloud-java-remediated
    else
      cd ~/workspace/google-cloud-java-remediated
      git fetch origin grpc-uds-java-inproc
      git checkout -B grpc-uds-java-inproc FETCH_HEAD
    fi
    ~/workspace/google-cloud-java-remediated/java-storage/google-cloud-storage/src/test/resources/scripts/run_live_benchmark_matrix.sh $*
  "
  exit 0
fi

SIDECAR_BIN="${SIDECAR_BIN:-${HOME}/bin/gcs-core-server}"
INPROC_LIB="${INPROC_LIB:-${HOME}/bin/libstorage_grpc_uds.so}"
OUT_DIR="${OUT_DIR:-/tmp/gcs-uds-remediated-results-$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "${OUT_DIR}"

echo "================================================================================"
echo "  GCS Java UDS Live Remediation Runner (Gates 2 & 3)"
echo "  Output Directory : ${OUT_DIR}"
echo "  Host / vCPUs     : $(hostname) / $(nproc) vCPUs"
echo "  Regional Bucket  : gs://${REGIONAL_BUCKET}"
echo "  Zonal Bucket     : gs://${ZONAL_BUCKET}"
echo "================================================================================"

INSTANCE_ID="${INSTANCE_ID:-}"
if [[ -z "${INSTANCE_ID}" ]]; then
  INSTANCE_ID="$(curl -fsS -H 'Metadata-Flavor: Google' \
    http://metadata.google.internal/computeMetadata/v1/instance/id || true)"
fi
if [[ -z "${INSTANCE_ID}" ]]; then
  echo "ERROR: Could not determine numeric GCE INSTANCE_ID (set INSTANCE_ID env var)." >&2
  exit 1
fi
echo "[Gate 2] Resolved GCE instance_id=${INSTANCE_ID}"

seed_list_100_fixtures() {
  local bucket="$1"
  local existing
  existing="$(gcloud storage ls "gs://${bucket}/list-bench-100/" --project="${PROJECT_ID}" 2>/dev/null | wc -l || echo 0)"
  if [[ "${existing}" -ge 100 ]]; then
    echo "[Gate 2] gs://${bucket}/list-bench-100/ already has ${existing} objects."
    return 0
  fi
  echo "[Gate 2] Seeding 100 list fixtures in gs://${bucket}/list-bench-100/..."
  local seed_dir="${OUT_DIR}/seed-list-100"
  mkdir -p "${seed_dir}"
  for i in $(seq -f "%03g" 0 99); do
    printf "list-fixture-%s\n" "${i}" > "${seed_dir}/item-${i}.bin"
  done
  gcloud storage cp "${seed_dir}"/item-*.bin "gs://${bucket}/list-bench-100/" \
    --project="${PROJECT_ID}" --quiet
}

configure_write_cleanup_lifecycle() {
  local bucket="$1"
  local lc_file="${OUT_DIR}/lifecycle-write-cleanup.json"
  cat > "${lc_file}" <<'JSON'
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {
        "age": 1,
        "matchesPrefix": ["bench-write-"]
      }
    }
  ]
}
JSON
  gcloud storage buckets update "gs://${bucket}" \
    --lifecycle-file="${lc_file}" \
    --project="${PROJECT_ID}" --quiet || true
}

seed_list_100_fixtures "${REGIONAL_BUCKET}"
seed_list_100_fixtures "${ZONAL_BUCKET}"
configure_write_cleanup_lifecycle "${REGIONAL_BUCKET}"
configure_write_cleanup_lifecycle "${ZONAL_BUCKET}"

BUCKET_COUNT="$(gcloud storage buckets list --project="${PROJECT_ID}" --format='value(name)' | wc -l)"
echo "[Gate 2] Project ${PROJECT_ID} currently has ${BUCKET_COUNT} buckets." | tee "${OUT_DIR}/project_bucket_count.txt"

cd "${MODULE_DIR}"
echo "[Gate 2] Compiling test sources & running ITUdsVeneerComplianceTest..."
mvn test-compile -Dcheckstyle.skip -Dfmt.skip=true
mvn test -Dtest=ITUdsVeneerComplianceTest \
  -Dgcs.benchmark.driver=uds \
  -Dgcs.sidecar.binary.path="${SIDECAR_BIN}" \
  -Dcheckstyle.skip -Dfmt.skip=true \
  | tee "${OUT_DIR}/gate2_compliance_sidecar.log"

HAS_INPROC="false"
if [[ -f "${MODULE_DIR}/src/test/java/com/google/cloud/storage/sidecar/RustInprocSidecarManager.java" && -f "${INPROC_LIB}" ]]; then
  HAS_INPROC="true"
  mvn test -Dtest=ITUdsVeneerComplianceTest \
    -Dgcs.benchmark.driver=uds-inproc \
    -Dgcs.inproc.library.path="${INPROC_LIB}" \
    -Dcheckstyle.skip -Dfmt.skip=true \
    | tee "${OUT_DIR}/gate2_compliance_inproc.log"
fi

BENCH_TEST_SPEC="ITUdsVeneerBenchmark#test00b_Open_200MB+test00c_Read50x2x4_Serial+test00d_Read50x2x4_Concurrent+test00e_Random8K+test09a_ListObjects100Benchmark+test09b_ListBucketsBenchmark+test10_CloudpathWorkload4_Read16MbChunkOf200Mb+test11_CloudpathWorkload5_Write200Mb"

run_suite() {
  local suite_label="$1"
  local bucket="$2"
  local driver="$3"
  local transport="$4"
  local test_spec="${5:-${BENCH_TEST_SPEC}}"
  local extra_flags="${6:-}"

  local timeline_file="${OUT_DIR}/timeline_${suite_label}.json"
  local log_file="${OUT_DIR}/bench_${suite_label}.log"
  local gcm_json="${OUT_DIR}/gcm_${suite_label}.json"
  local gcm_txt="${OUT_DIR}/gcm_${suite_label}.txt"

  echo "--------------------------------------------------------------------------------"
  echo "[Gate 3] Running ${suite_label}: bucket=${bucket} driver=${driver} transport=${transport}"
  echo "--------------------------------------------------------------------------------"

  # shellcheck disable=SC2086
  mvn test -Dtest="${test_spec}" \
    -Dgcs.live.benchmark=true \
    -Dgcs.project.id="${PROJECT_ID}" \
    -Dgcs.test.bucket="${bucket}" \
    -Dgcs.benchmark.driver="${driver}" \
    -Dgcs.native.transport="${transport}" \
    -Dgcs.sidecar.transport="${transport}" \
    -Dgcs.sidecar.default.transport="${transport}" \
    -Dgcs.sidecar.grpc.subchannels=0 \
    -Dgcs.native.channel.pool.size="${NATIVE_POOL_SIZE}" \
    -Dgcs.sidecar.binary.path="${SIDECAR_BIN}" \
    -Dgcs.inproc.library.path="${INPROC_LIB}" \
    -Dgcs.bench.pause.sec="${PAUSE_SEC}" \
    -Dgcs.bench.min.window.sec="${MIN_WINDOW_SEC}" \
    -Dgcs.bench.duration.sec="${DURATION_SEC}" \
    -Dgcs.bench.post.wait.sec="${POST_WAIT_SEC}" \
    -Dgcs.bench.timeline.file="${timeline_file}" \
    ${extra_flags} \
    -Dcheckstyle.skip -Dfmt.skip=true \
    | tee "${log_file}"

  python3 "${SCRIPT_DIR}/fetch_gcp_metrics.py" \
    --project "${PROJECT_ID}" \
    --instance-id "${INSTANCE_ID}" \
    --timeline "${timeline_file}" \
    --output "${gcm_json}" \
    | tee "${gcm_txt}"
}

DRIVERS=("native" "uds")
if [[ "${HAS_INPROC}" == "true" ]]; then
  DRIVERS+=("uds-inproc")
fi

SUITES="${SUITES:-regional_grpc regional_json zonal_grpc}"

if [[ " ${SUITES} " == *" regional_grpc "* ]]; then
  # 1. Standard Regional [gRPC] Suite
  for drv in "${DRIVERS[@]}"; do
    run_suite "regional_grpc_${drv}" "${REGIONAL_BUCKET}" "${drv}" "grpc" "${BENCH_TEST_SPEC}" "-Dgcs.bench.strict.list100=true"
  done
fi

if [[ " ${SUITES} " == *" regional_json "* ]]; then
  # 2. Standard Regional [JSON] Suite (ReadObject & WriteObject in JSON)
  JSON_TEST_SPEC="ITUdsVeneerBenchmark#test00c_Read50x2x4_Serial+test00d_Read50x2x4_Concurrent+test10_CloudpathWorkload4_Read16MbChunkOf200Mb+test11_CloudpathWorkload5_Write200Mb"
  for drv in "${DRIVERS[@]}"; do
    run_suite "regional_json_${drv}" "${REGIONAL_BUCKET}" "${drv}" "json" "${JSON_TEST_SPEC}" "-Dgcs.bench.wl5.api=reader"
  done
fi

if [[ " ${SUITES} " == *" zonal_grpc "* ]]; then
  # 3. Zonal Rapid [gRPC] Suite (includes 5-minute BidiWriteObject blobAppendableUpload WL6)
  for drv in "${DRIVERS[@]}"; do
    run_suite "zonal_grpc_${drv}" "${ZONAL_BUCKET}" "${drv}" "grpc" "${BENCH_TEST_SPEC}" "-Dgcs.bench.strict.list100=true"
  done
fi

echo "================================================================================"
echo "  ALL GATE 2 & GATE 3 SUITES COMPLETED: ${OUT_DIR}"
echo "================================================================================"
