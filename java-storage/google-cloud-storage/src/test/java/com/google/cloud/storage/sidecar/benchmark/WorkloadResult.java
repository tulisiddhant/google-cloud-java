/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.storage.sidecar.benchmark;

import static java.util.Objects.requireNonNull;

import com.google.cloud.storage.sidecar.UdsStorage;
import com.google.cloud.storage.sidecar.proto.StatsResponse;
import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Immutable steady-state timing, throughput, UTC Cloud Monitoring window boundaries, and
 * supplementary process telemetry for a single completed {@link Workload}.
 */
public final class WorkloadResult {

  public static final double BYTES_PER_MB = 1_000_000.0;
  public static final double BYTES_PER_MIB = 1_048_576.0;
  public static final double NANOS_PER_MS = 1_000_000.0;
  public static final double NANOS_PER_SEC = 1_000_000_000.0;

  private final String workloadId;
  private final String displayName;
  private final Instant wlStartUtc;
  private final Instant wlEndUtc;
  private final long completedOps;
  private final long totalBytes;
  private final double activeSec;
  private final long p50Ns;
  private final long p90Ns;
  private final long p99Ns;
  private final double opsPerSec;
  private final double decimalMbPerSec;
  private final double binaryMibPerSec;
  private final DiagnosticDelta diagnostics;

  private WorkloadResult(
      String workloadId,
      String displayName,
      Instant wlStartUtc,
      Instant wlEndUtc,
      long completedOps,
      long totalBytes,
      double activeSec,
      long p50Ns,
      long p90Ns,
      long p99Ns,
      DiagnosticDelta diagnostics) {
    this.workloadId = requireNonNull(workloadId, "workloadId");
    this.displayName = requireNonNull(displayName, "displayName");
    this.wlStartUtc = requireNonNull(wlStartUtc, "wlStartUtc");
    this.wlEndUtc = requireNonNull(wlEndUtc, "wlEndUtc");
    this.completedOps = completedOps;
    this.totalBytes = totalBytes;
    this.activeSec = activeSec;
    this.p50Ns = p50Ns;
    this.p90Ns = p90Ns;
    this.p99Ns = p99Ns;
    this.opsPerSec = activeSec > 0.0 ? (completedOps / activeSec) : 0.0;
    this.decimalMbPerSec = activeSec > 0.0 ? ((totalBytes / BYTES_PER_MB) / activeSec) : 0.0;
    this.binaryMibPerSec = activeSec > 0.0 ? ((totalBytes / BYTES_PER_MIB) / activeSec) : 0.0;
    this.diagnostics = diagnostics;
  }

  /**
   * Builds a {@link WorkloadResult} where {@code activeSec} is {@code sum(latenciesNs) / 1e9}
   * (used for sequential outer-loop workloads {@code WL1}–{@code WL4}, {@code WL7a}, {@code WL7b}).
   */
  public static WorkloadResult fromSequentialSamples(
      String workloadId,
      String displayName,
      Instant wlStartUtc,
      Instant wlEndUtc,
      long[] latenciesNs,
      long totalBytes,
      DiagnosticDelta diagnostics) {
    if (latenciesNs == null || latenciesNs.length == 0) {
      throw new IllegalArgumentException("latenciesNs must be non-empty");
    }
    long sumNs = 0L;
    for (long d : latenciesNs) {
      sumNs += d;
    }
    double activeSec = sumNs / NANOS_PER_SEC;
    return fromSortedSamples(
        workloadId,
        displayName,
        wlStartUtc,
        wlEndUtc,
        latenciesNs,
        totalBytes,
        activeSec,
        diagnostics);
  }

  /**
   * Builds a {@link WorkloadResult} where {@code activeSec} is outer wall-clock duration {@code
   * (wlEndNs - wlStartNs) / 1e9} (used for 48-thread scale workloads {@code WL5} and {@code WL6}).
   */
  public static WorkloadResult fromParallelScaleSamples(
      String workloadId,
      String displayName,
      Instant wlStartUtc,
      Instant wlEndUtc,
      long[] latenciesNs,
      long totalBytes,
      double wallActiveSec,
      DiagnosticDelta diagnostics) {
    if (latenciesNs == null || latenciesNs.length == 0) {
      throw new IllegalArgumentException("latenciesNs must be non-empty");
    }
    return fromSortedSamples(
        workloadId,
        displayName,
        wlStartUtc,
        wlEndUtc,
        latenciesNs,
        totalBytes,
        wallActiveSec,
        diagnostics);
  }

  private static WorkloadResult fromSortedSamples(
      String workloadId,
      String displayName,
      Instant wlStartUtc,
      Instant wlEndUtc,
      long[] latenciesNs,
      long totalBytes,
      double activeSec,
      DiagnosticDelta diagnostics) {
    long[] sorted = Arrays.copyOf(latenciesNs, latenciesNs.length);
    Arrays.sort(sorted);
    long p50 = percentileSortedNs(sorted, 0.50);
    long p90 = percentileSortedNs(sorted, 0.90);
    long p99 = percentileSortedNs(sorted, 0.99);
    return new WorkloadResult(
        workloadId,
        displayName,
        wlStartUtc,
        wlEndUtc,
        sorted.length,
        totalBytes,
        activeSec,
        p50,
        p90,
        p99,
        diagnostics);
  }

  /**
   * Computes index {@code ceil(p * N) - 1} clamped to {@code [0, N - 1]} on an ascending-sorted
   * sample array per Section 6.1.
   */
  public static long percentileSortedNs(long[] sortedAscending, double p) {
    int n = sortedAscending.length;
    if (n == 0) {
      throw new IllegalArgumentException("sortedAscending must be non-empty");
    }
    int idx = (int) Math.ceil(p * n) - 1;
    if (idx < 0) {
      idx = 0;
    } else if (idx >= n) {
      idx = n - 1;
    }
    return sortedAscending[idx];
  }

  public String workloadId() {
    return workloadId;
  }

  public String displayName() {
    return displayName;
  }

  public Instant wlStartUtc() {
    return wlStartUtc;
  }

  public Instant wlEndUtc() {
    return wlEndUtc;
  }

  public long completedOps() {
    return completedOps;
  }

  public long totalBytes() {
    return totalBytes;
  }

  public double activeSec() {
    return activeSec;
  }

  public long p50Ns() {
    return p50Ns;
  }

  public long p90Ns() {
    return p90Ns;
  }

  public long p99Ns() {
    return p99Ns;
  }

  public double p50Ms() {
    return p50Ns / NANOS_PER_MS;
  }

  public double p90Ms() {
    return p90Ns / NANOS_PER_MS;
  }

  public double p99Ms() {
    return p99Ns / NANOS_PER_MS;
  }

  public double opsPerSec() {
    return opsPerSec;
  }

  public double decimalMbPerSec() {
    return decimalMbPerSec;
  }

  public double binaryMibPerSec() {
    return binaryMibPerSec;
  }

  public DiagnosticDelta diagnostics() {
    return diagnostics;
  }

  /** Supplementary JVM + Rust process CPU and VmHWM snapshot captured around a workload window. */
  public static final class ProcessSnapshot {
    public final long wallNanoTime;
    public final double jvmCpuSecs;
    public final long jvmVmHwmBytes;
    public final double rustCpuSecs;
    public final long rustVmHwmBytes;

    private ProcessSnapshot(
        long wallNanoTime,
        double jvmCpuSecs,
        long jvmVmHwmBytes,
        double rustCpuSecs,
        long rustVmHwmBytes) {
      this.wallNanoTime = wallNanoTime;
      this.jvmCpuSecs = jvmCpuSecs;
      this.jvmVmHwmBytes = jvmVmHwmBytes;
      this.rustCpuSecs = rustCpuSecs;
      this.rustVmHwmBytes = rustVmHwmBytes;
    }

    public static ProcessSnapshot capture(Optional<UdsStorage> udsClient, boolean resetRustPeak) {
      long wallNs = System.nanoTime();
      double jvmCpu = readJvmProcessCpuSecs();
      long jvmHwm = readProcSelfVmHwmBytes();
      double rustCpu = 0.0;
      long rustHwm = 0L;
      if (udsClient != null && udsClient.isPresent()) {
        try {
          StatsResponse st = udsClient.get().getCoreStats(resetRustPeak);
          rustCpu = st.getCpuUserSecs() + st.getCpuSysSecs();
          rustHwm = st.getPeakRssBytes();
        } catch (RuntimeException ignored) {
          // Supplementary diagnostic only.
        }
      }
      return new ProcessSnapshot(wallNs, jvmCpu, jvmHwm, rustCpu, rustHwm);
    }

    private static double readJvmProcessCpuSecs() {
      java.lang.management.OperatingSystemMXBean bean =
          ManagementFactory.getOperatingSystemMXBean();
      if (bean instanceof OperatingSystemMXBean) {
        long ns = ((OperatingSystemMXBean) bean).getProcessCpuTime();
        return ns > 0 ? (ns / NANOS_PER_SEC) : 0.0;
      }
      return 0.0;
    }

    private static long readProcSelfVmHwmBytes() {
      try {
        List<String> lines =
            Files.readAllLines(Paths.get("/proc/self/status"), StandardCharsets.UTF_8);
        for (String line : lines) {
          if (line.startsWith("VmHWM:")) {
            String[] parts = line.trim().split("\\s+");
            if (parts.length >= 2) {
              return Long.parseLong(parts[1]) * 1024L;
            }
          }
        }
      } catch (IOException | NumberFormatException ignored) {
        // Non-Linux or unreadable /proc/self/status.
      }
      return 0L;
    }
  }

  /** Supplementary per-workload process CPU-core equivalents and peak RSS watermarks. */
  public static final class DiagnosticDelta {
    public final double jvmCpuCoresUsed;
    public final double rustCpuCoresUsed;
    public final long jvmPeakRssBytes;
    public final long rustPeakRssBytes;

    public DiagnosticDelta(
        double jvmCpuCoresUsed,
        double rustCpuCoresUsed,
        long jvmPeakRssBytes,
        long rustPeakRssBytes) {
      this.jvmCpuCoresUsed = jvmCpuCoresUsed;
      this.rustCpuCoresUsed = rustCpuCoresUsed;
      this.jvmPeakRssBytes = jvmPeakRssBytes;
      this.rustPeakRssBytes = rustPeakRssBytes;
    }

    public static DiagnosticDelta between(ProcessSnapshot before, ProcessSnapshot after) {
      double elapsedSec = Math.max(1e-9, (after.wallNanoTime - before.wallNanoTime) / NANOS_PER_SEC);
      double jvmCores = Math.max(0.0, (after.jvmCpuSecs - before.jvmCpuSecs) / elapsedSec);
      double rustCores = Math.max(0.0, (after.rustCpuSecs - before.rustCpuSecs) / elapsedSec);
      return new DiagnosticDelta(jvmCores, rustCores, after.jvmVmHwmBytes, after.rustVmHwmBytes);
    }
  }
}
