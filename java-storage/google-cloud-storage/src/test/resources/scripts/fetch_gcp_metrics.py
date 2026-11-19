#!/usr/bin/env python3
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
"""Extracts Cloud Monitoring CPU and Ops Agent Peak Memory per benchmark workload.

Methodology compliance rules enforced:
1. CPU Utilization (`compute.googleapis.com/instance/cpu/utilization`):
   - When a workload window is >= 120s (`(end - start) >= 120s`), uses the interior
     steady-state window `(start + 60s, end]` so partial 60s boundary buckets
     blended with inter-workload quiescence idle time are excluded.
   - When `< 120s`, queries `[start, end + 60s]` and flags a dilution warning.
2. Peak Memory (`agent.googleapis.com/memory/percent_used`, `state="used"`):
   - Computes `MAX` across `[start, end + 60s]` to capture the peak 60s sample.
"""

import argparse
import datetime
import json
import sys
import urllib.parse
import urllib.request
import subprocess


def get_access_token() -> str:
  out = subprocess.check_output(
      ["gcloud", "auth", "print-access-token"], text=True
  )
  return out.strip()


def parse_iso_utc(ts: str) -> datetime.datetime:
  if ts.endswith("Z"):
    ts = ts[:-1] + "+00:00"
  return datetime.datetime.fromisoformat(ts).astimezone(datetime.timezone.utc)


def fmt_iso_utc(dt: datetime.datetime) -> str:
  return dt.astimezone(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def query_time_series(
    project: str,
    token: str,
    metric_filter: str,
    start_dt: datetime.datetime,
    end_dt: datetime.datetime,
) -> list[float]:
  params = urllib.parse.urlencode({
      "filter": metric_filter,
      "interval.startTime": fmt_iso_utc(start_dt),
      "interval.endTime": fmt_iso_utc(end_dt),
      "view": "FULL",
  })
  url = f"https://monitoring.googleapis.com/v3/projects/{project}/timeSeries?{params}"
  req = urllib.request.Request(
      url, headers={"Authorization": f"Bearer {token}"}
  )
  with urllib.request.urlopen(req, timeout=30) as resp:
    payload = json.loads(resp.read().decode("utf-8"))

  values: list[float] = []
  for series in payload.get("timeSeries", []):
    for pt in series.get("points", []):
      val = pt.get("value", {})
      if "doubleValue" in val:
        values.append(float(val["doubleValue"]))
      elif "int64Value" in val:
        values.append(float(val["int64Value"]))
  return values


def main() -> int:
  parser = argparse.ArgumentParser(
      description="Fetch steady-state GCE CPU & Ops Agent Peak Memory per workload."
  )
  parser.add_argument("--project", required=True, help="GCP Project ID")
  parser.add_argument(
      "--instance-id", required=True, help="Numeric GCE instance_id"
  )
  parser.add_argument(
      "--timeline",
      default="/tmp/benchmark_timeline.json",
      help="Path to benchmark_timeline.json emitted by ITUdsVeneerBenchmark",
  )
  parser.add_argument(
      "--output", default="", help="Optional path to write JSON telemetry output"
  )
  args = parser.parse_args()

  with open(args.timeline, "r", encoding="utf-8") as f:
    timeline = json.load(f)

  token = get_access_token()
  cpu_filter = (
      'metric.type="compute.googleapis.com/instance/cpu/utilization" AND '
      f'resource.labels.instance_id="{args.instance_id}"'
  )
  mem_filter = (
      'metric.type="agent.googleapis.com/memory/percent_used" AND '
      'metric.labels.state="used" AND '
      f'resource.labels.instance_id="{args.instance_id}"'
  )

  results = []
  print(
      f"{'Workload':<30} | {'Duration':>8} | {'CPU Pts':>7} | {'Mean CPU %':>10} | "
      f"{'Max CPU %':>9} | {'Peak Mem %':>10}"
  )
  print("-" * 92)

  for entry in timeline:
    wl = entry["workload"]
    start_dt = parse_iso_utc(entry["start"])
    end_dt = parse_iso_utc(entry["end"])
    duration_sec = (end_dt - start_dt).total_seconds()

    if duration_sec >= 120.0:
      cpu_start = start_dt + datetime.timedelta(seconds=60)
      cpu_end = end_dt
      diluted = False
    else:
      cpu_start = start_dt
      cpu_end = end_dt + datetime.timedelta(seconds=60)
      diluted = True

    mem_start = start_dt
    mem_end = end_dt + datetime.timedelta(seconds=60)

    cpu_pts = [
        v * 100.0
        for v in query_time_series(
            args.project, token, cpu_filter, cpu_start, cpu_end
        )
    ]
    mem_pts = query_time_series(
        args.project, token, mem_filter, mem_start, mem_end
    )

    mean_cpu = (sum(cpu_pts) / len(cpu_pts)) if cpu_pts else 0.0
    max_cpu = max(cpu_pts) if cpu_pts else 0.0
    peak_mem = max(mem_pts) if mem_pts else 0.0
    flag = "*" if diluted else " "

    print(
        f"{wl:<30} | {duration_sec:>7.1f}s | {len(cpu_pts):>6}{flag} | "
        f"{mean_cpu:>9.2f}% | {max_cpu:>8.2f}% | {peak_mem:>9.2f}%"
    )
    results.append({
        "workload": wl,
        "start": entry["start"],
        "end": entry["end"],
        "duration_sec": round(duration_sec, 3),
        "steady_state_interior_window": not diluted,
        "cpu_samples": len(cpu_pts),
        "mean_cpu_percent": round(mean_cpu, 4),
        "max_cpu_percent": round(max_cpu, 4),
        "peak_memory_percent_used": round(peak_mem, 4),
    })

  if args.output:
    with open(args.output, "w", encoding="utf-8") as out_f:
      json.dump(results, out_f, indent=2)

  return 0


if __name__ == "__main__":
  sys.exit(main())
