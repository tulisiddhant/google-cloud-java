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

import com.google.cloud.storage.sidecar.DriverMode;
import com.google.cloud.storage.sidecar.UdsStorage;
import com.google.cloud.storage.sidecar.proto.StatsResponse;
import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

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
    public final int establishedTcp443;
    public final long nicRxBytes;
    public final long nicTxBytes;

    private ProcessSnapshot(
        long wallNanoTime,
        double jvmCpuSecs,
        long jvmVmHwmBytes,
        double rustCpuSecs,
        long rustVmHwmBytes,
        int establishedTcp443,
        long nicRxBytes,
        long nicTxBytes) {
      this.wallNanoTime = wallNanoTime;
      this.jvmCpuSecs = jvmCpuSecs;
      this.jvmVmHwmBytes = jvmVmHwmBytes;
      this.rustCpuSecs = rustCpuSecs;
      this.rustVmHwmBytes = rustVmHwmBytes;
      this.establishedTcp443 = establishedTcp443;
      this.nicRxBytes = nicRxBytes;
      this.nicTxBytes = nicTxBytes;
    }

    public static ProcessSnapshot capture(Optional<UdsStorage> udsClient, boolean resetRustPeak) {
      long wallNs = System.nanoTime();
      double jvmCpu = readJvmProcessCpuSecs();
      long jvmHwm = readProcSelfVmHwmBytes();
      int tcp443 = countEstablishedTcp443(udsClient);
      long[] nic = readNicBytes();
      double rustCpu = 0.0;
      long rustHwm = 0L;
      if (udsClient != null && udsClient.isPresent()) {
        try {
          UdsStorage uds = udsClient.get();
          StatsResponse st = uds.getCoreStats(resetRustPeak);
          // In INPROC mode, Tokio shares the JVM OS process so getProcessCpuTime() already covers
          // both JVM and Rust threads; only add separate child-process CPU in SIDECAR mode.
          if (uds.getDriverMode() == DriverMode.SIDECAR) {
            rustCpu = st.getCpuUserSecs() + st.getCpuSysSecs();
          }
          rustHwm = st.getPeakRssBytes();
        } catch (RuntimeException ignored) {
          // Supplementary diagnostic only.
        }
      }
      return new ProcessSnapshot(
          wallNs, jvmCpu, jvmHwm, rustCpu, rustHwm, tcp443, nic[0], nic[1]);
    }

    /**
     * Counts established TCP connections to remote port {@code 443} ({@code :01BB}) owned by the
     * active upstream GCS transport process ({@code /proc/<serverPid>/fd} for {@code uds-sidecar},
     * or {@code /proc/self/fd} for {@code native} and {@code uds-inproc}).
     */
    public static int countEstablishedTcp443(Optional<UdsStorage> udsClient) {
      Path fdDir = Paths.get("/proc/self/fd");
      if (udsClient != null && udsClient.isPresent()) {
        UdsStorage uds = udsClient.get();
        if (uds.getDriverMode() == DriverMode.SIDECAR) {
          OptionalLong pid = uds.getServerPid();
          if (pid.isPresent()) {
            fdDir = Paths.get("/proc/" + pid.getAsLong() + "/fd");
          }
        }
      }
      Set<String> inodes = new HashSet<String>();
      try (DirectoryStream<Path> fds = Files.newDirectoryStream(fdDir)) {
        for (Path fd : fds) {
          try {
            String target = Files.readSymbolicLink(fd).toString();
            if (target.startsWith("socket:[")) {
              inodes.add(target.substring(8, target.length() - 1));
            }
          } catch (IOException ignored) {
            // Socket closed while listing.
          }
        }
      } catch (IOException e) {
        return -1;
      }
      int count = 0;
      String[] netFiles = new String[] {"/proc/net/tcp", "/proc/net/tcp6"};
      for (int i = 0; i < netFiles.length; i++) {
        try {
          List<String> lines = Files.readAllLines(Paths.get(netFiles[i]), StandardCharsets.UTF_8);
          for (int j = 0; j < lines.size(); j++) {
            String[] cols = lines.get(j).trim().split("\\s+");
            if (cols.length > 9
                && cols[2].endsWith(":01BB")
                && "01".equals(cols[3])
                && inodes.contains(cols[9])) {
              count++;
            }
          }
        } catch (IOException e) {
          return -1;
        }
      }
      return count;
    }

    private static long[] readNicBytes() {
      long rx = 0L;
      long tx = 0L;
      try {
        List<String> lines =
            Files.readAllLines(Paths.get("/proc/net/dev"), StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
          String line = lines.get(i);
          int colon = line.indexOf(':');
          if (colon < 0 || "lo".equals(line.substring(0, colon).trim())) {
            continue;
          }
          String[] cols = line.substring(colon + 1).trim().split("\\s+");
          if (cols.length > 8) {
            rx += Long.parseLong(cols[0]);
            tx += Long.parseLong(cols[8]);
          }
        }
      } catch (IOException | RuntimeException ignored) {
        return new long[] {0L, 0L};
      }
      return new long[] {rx, tx};
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

  /** Supplementary per-workload process CPU-core equivalents, peak RSS, TCP:443, and NIC Gbps. */
  public static final class DiagnosticDelta {
    public final double jvmCpuCoresUsed;
    public final double rustCpuCoresUsed;
    public final long jvmPeakRssBytes;
    public final long rustPeakRssBytes;
    public final int establishedTcp443;
    public final double nicRxGbps;
    public final double nicTxGbps;

    public DiagnosticDelta(
        double jvmCpuCoresUsed,
        double rustCpuCoresUsed,
        long jvmPeakRssBytes,
        long rustPeakRssBytes) {
      this(jvmCpuCoresUsed, rustCpuCoresUsed, jvmPeakRssBytes, rustPeakRssBytes, -1, 0.0, 0.0);
    }

    public DiagnosticDelta(
        double jvmCpuCoresUsed,
        double rustCpuCoresUsed,
        long jvmPeakRssBytes,
        long rustPeakRssBytes,
        int establishedTcp443,
        double nicRxGbps,
        double nicTxGbps) {
      this.jvmCpuCoresUsed = jvmCpuCoresUsed;
      this.rustCpuCoresUsed = rustCpuCoresUsed;
      this.jvmPeakRssBytes = jvmPeakRssBytes;
      this.rustPeakRssBytes = rustPeakRssBytes;
      this.establishedTcp443 = establishedTcp443;
      this.nicRxGbps = nicRxGbps;
      this.nicTxGbps = nicTxGbps;
    }

    public static DiagnosticDelta between(ProcessSnapshot before, ProcessSnapshot after) {
      return between(before, after, -1);
    }

    public static DiagnosticDelta between(
        ProcessSnapshot before, ProcessSnapshot after, int midTcp443) {
      double elapsedSec =
          Math.max(1e-9, (after.wallNanoTime - before.wallNanoTime) / NANOS_PER_SEC);
      double jvmCores = Math.max(0.0, (after.jvmCpuSecs - before.jvmCpuSecs) / elapsedSec);
      double rustCores = Math.max(0.0, (after.rustCpuSecs - before.rustCpuSecs) / elapsedSec);
      int tcp443 =
          Math.max(midTcp443, Math.max(before.establishedTcp443, after.establishedTcp443));
      double rxGbps =
          Math.max(0.0, (after.nicRxBytes - before.nicRxBytes) * 8.0 / elapsedSec / 1e9);
      double txGbps =
          Math.max(0.0, (after.nicTxBytes - before.nicTxBytes) * 8.0 / elapsedSec / 1e9);
      return new DiagnosticDelta(
          jvmCores,
          rustCores,
          after.jvmVmHwmBytes,
          after.rustVmHwmBytes,
          tcp443,
          rxGbps,
          txGbps);
    }
  }
}
