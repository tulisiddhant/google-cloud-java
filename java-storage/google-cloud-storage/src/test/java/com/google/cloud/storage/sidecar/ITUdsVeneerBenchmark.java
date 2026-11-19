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

package com.google.cloud.storage.sidecar;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.gax.paging.Page;
import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.UdsStorage.DriverMode;
import gcscore.v1.StatsResponse;
import gcscore.v1.Transport;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * Remediation-compliant Veneer Integration Benchmark for GCS SDK Consolidation (Java over UDS).
 *
 * <p>Evaluates {@code Native Java}, {@code uds-sidecar} ({@code DRIVER=uds}), and {@code
 * uds-inproc} ({@code DRIVER=uds-inproc}) across both {@code gRPC} and {@code JSON} transports with
 * strict 1:1 Veneer API symmetry, steady-state warmup, dual {@code MB/s} + {@code MiB/s} reporting,
 * {@code >= 120s} Cloud Monitoring sampling windows, and per-workload JVM + Rust {@code Stats}
 * CPU/{@code VmHWM} deltas.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITUdsVeneerBenchmark {

  private static final String TEST_BUCKET =
      System.getProperty("gcs.test.bucket", "sidecar-experiment-bucket");
  private static final String DRIVER =
      System.getProperty("gcs.benchmark.driver", System.getProperty("gcs.driver", "uds"));
  private static final boolean IS_LIVE =
      Boolean.parseBoolean(
          System.getProperty("gcs.live.benchmark", System.getProperty("gcs.bench.live", "false")));
  private static final int MIN_WINDOW_SEC =
      Integer.getInteger("gcs.bench.min.window.sec", IS_LIVE ? 120 : 0);
  private static final boolean ENABLE_WARMUP =
      Boolean.parseBoolean(System.getProperty("gcs.bench.warmup", "true"));

  private static MockStorageGrpcServer mockServer;
  private static Storage storage;
  private static final List<Storage> nativeClientPool = new ArrayList<Storage>();
  private static UdsStorage udsStorageHandler;
  private static String activeTransport = "grpc";

  private static final List<BenchmarkMetric> recordedMetrics = new ArrayList<BenchmarkMetric>();
  private static final List<String> TIMELINE_RECORDS =
      Collections.synchronizedList(new ArrayList<String>());
  private static StatsResponse initialStats;
  private static StatsResponse finalStats;

  private static void recordTimeline(String workload, Instant start, Instant end) {
    TIMELINE_RECORDS.add(
        String.format(
            "{\"workload\":\"%s\",\"driver\":\"%s\",\"transport\":\"%s\",\"start\":\"%s\",\"end\":\"%s\"}",
            workload, DRIVER, activeTransport, start.toString(), end.toString()));
  }

  private static String repeat(String s, int count) {
    StringBuilder sb = new StringBuilder(s.length() * count);
    for (int i = 0; i < count; i++) {
      sb.append(s);
    }
    return sb.toString();
  }

  private static void banner(String title) {
    System.out.println("\n" + repeat("=", 88));
    System.out.println("  " + title);
    System.out.println(repeat("=", 88));
  }

  private static void log(String msg) {
    System.out.println("[UdsBenchmark] " + msg);
  }

  private static String getTransportPrefix() {
    String tag = activeTransport.toUpperCase();
    if ("native".equalsIgnoreCase(DRIVER)) {
      return "[Native-" + tag + "] ";
    } else if ("uds-inproc".equalsIgnoreCase(DRIVER)) {
      return "[UDS-INPROC-" + tag + "] ";
    } else {
      return "[UDS-SIDECAR-" + tag + "] ";
    }
  }

  private static boolean isGrpcTransport() {
    return "grpc".equalsIgnoreCase(activeTransport);
  }

  private static Storage pickClient(int workerIndex) {
    if ("native".equalsIgnoreCase(DRIVER) && !nativeClientPool.isEmpty()) {
      return nativeClientPool.get(Math.abs(workerIndex) % nativeClientPool.size());
    }
    return storage;
  }

  @BeforeClass
  public static void setUp() throws Exception {
    String requestedTransport;
    if ("native".equalsIgnoreCase(DRIVER)) {
      requestedTransport = System.getProperty("gcs.native.transport", "grpc");
    } else {
      requestedTransport =
          System.getProperty(
              "gcs.sidecar.transport",
              System.getProperty("gcs.sidecar.default.transport", "grpc"));
    }
    activeTransport = "json".equalsIgnoreCase(requestedTransport) ? "json" : "grpc";

    banner(
        String.format(
            "GCS BENCHMARK SETUP: DRIVER=%s | TRANSPORT=%s | LIVE=%b | BUCKET=%s | MIN_WIN=%ds",
            DRIVER.toUpperCase(),
            activeTransport.toUpperCase(),
            IS_LIVE,
            TEST_BUCKET,
            MIN_WINDOW_SEC));

    if (!IS_LIVE) {
      String customEndpoint = System.getProperty("gcs.sidecar.endpoint");
      if (customEndpoint == null || customEndpoint.isEmpty()) {
        mockServer = new MockStorageGrpcServer();
        mockServer.start();
        seedMockFixtures(mockServer);

        if (isGrpcTransport()) {
          System.setProperty("gcs.sidecar.endpoint", mockServer.getGrpcEndpoint());
          System.setProperty("gcs.sidecar.control.endpoint", mockServer.getGrpcEndpoint());
          System.setProperty("gcs.sidecar.default.transport", "grpc");
          System.setProperty("gcs.sidecar.transport", "grpc");
        } else {
          System.setProperty("gcs.sidecar.endpoint", mockServer.getHttpEndpoint());
          System.setProperty("gcs.sidecar.control.endpoint", mockServer.getGrpcEndpoint());
          System.setProperty("gcs.sidecar.default.transport", "json");
          System.setProperty("gcs.sidecar.transport", "json");
        }
        log(
            String.format(
                "Started dual-protocol MockStorageGrpcServer (gRPC=%s, HTTP=%s)",
                mockServer.getGrpcEndpoint(), mockServer.getHttpEndpoint()));
      }
    }

    StorageOptions baseOptions = buildStorageOptions();
    if ("native".equalsIgnoreCase(DRIVER)) {
      int poolSize = Math.max(1, Integer.getInteger("gcs.native.channel.pool.size", 1));
      storage = baseOptions.getService();
      nativeClientPool.add(storage);
      for (int i = 1; i < poolSize; i++) {
        nativeClientPool.add(buildStorageOptions().getService());
      }
      udsStorageHandler = null;
      log(
          String.format(
              "Initialized Native Storage client (%s, clientPoolSize=%d).",
              activeTransport.toUpperCase(), nativeClientPool.size()));
    } else {
      DriverMode mode =
          "uds-inproc".equalsIgnoreCase(DRIVER) ? DriverMode.INPROC : DriverMode.SIDECAR;
      Transport protoTransport = isGrpcTransport() ? Transport.GRPC : Transport.JSON;
      storage =
          UdsStorage.newBuilder(baseOptions)
              .setDriverMode(mode)
              .setTransport(protoTransport)
              .build();
      udsStorageHandler = (UdsStorage) Proxy.getInvocationHandler(storage);
      try {
        initialStats = udsStorageHandler.getStats(true);
        log(
            String.format(
                "Baseline %s Telemetry: PID=%d, Threads=%d, RSS=%,d KiB",
                mode == DriverMode.INPROC ? "Unified Process (JVM+Rust)" : "Sidecar Process",
                initialStats.getPid(),
                initialStats.getThreads(),
                initialStats.getRssBytes() / 1024));
      } catch (Exception e) {
        log("Note: Could not capture initial stats: " + e.getMessage());
      }
    }
  }

  private static StorageOptions buildStorageOptions() {
    StorageOptions.Builder builder =
        isGrpcTransport() ? StorageOptions.grpc() : StorageOptions.http();
    if (builder instanceof GrpcStorageOptions.Builder) {
      boolean directPath = Boolean.parseBoolean(System.getProperty("gcs.grpc.directpath", "false"));
      ((GrpcStorageOptions.Builder) builder).setAttemptDirectPath(directPath);
    }
    String project =
        System.getProperty(
            "gcs.project.id",
            System.getenv("GOOGLE_CLOUD_PROJECT") != null
                ? System.getenv("GOOGLE_CLOUD_PROJECT")
                : "vaibhavpratap-sdk-test");
    builder.setProjectId(project);
    if (!IS_LIVE) {
      builder.setCredentials(NoCredentials.getInstance());
      if (mockServer != null) {
        builder.setHost(
            isGrpcTransport() ? mockServer.getGrpcEndpoint() : mockServer.getHttpEndpoint());
      }
    }
    return builder.build();
  }

  private static void seedMockFixtures(MockStorageGrpcServer srv) {
    srv.addBucket(TEST_BUCKET);
    for (int i = 0; i < 99; i++) {
      srv.addBucket(String.format("bench-bucket-%03d", i));
    }
    srv.putVirtualObject(TEST_BUCKET, "bench-open-200mb.bin", 200L * 1024 * 1024);
    srv.putVirtualObject(TEST_BUCKET, "bench-random-100mb.bin", 100L * 1024 * 1024);
    byte[] smoke = new byte[16 * 1024 * 1024];
    new Random(7).nextBytes(smoke);
    srv.putBytes(TEST_BUCKET, "smoke-test.bin", smoke, "application/octet-stream");
    for (int i = 0; i < 48; i++) {
      srv.putVirtualObject(TEST_BUCKET, "bench-200mb-" + i + ".bin", 200L * 1024 * 1024);
    }
    byte[] smallPayload = "list-fixture-item".getBytes(StandardCharsets.UTF_8);
    for (int i = 0; i < 100; i++) {
      srv.putBytes(
          TEST_BUCKET,
          String.format("list-bench-100/item-%03d.bin", i),
          smallPayload,
          "application/octet-stream");
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    int postWaitSec = Integer.getInteger("gcs.bench.post.wait.sec", 0);
    if (postWaitSec > 0) {
      log(
          String.format(
              "Waiting %d seconds post-suite for trailing Cloud Monitoring points to flush...",
              postWaitSec));
      Thread.sleep(postWaitSec * 1000L);
    }

    banner("BENCHMARK TEARDOWN & FINAL METRICS REPORT");
    if (udsStorageHandler != null) {
      try {
        finalStats = udsStorageHandler.getStats(false);
      } catch (Exception e) {
        log("Warning: Failed to capture final stats: " + e.getMessage());
      }
    }

    printSummaryReport();

    if (!TIMELINE_RECORDS.isEmpty()) {
      try {
        String timelineFile =
            System.getProperty("gcs.bench.timeline.file", "/tmp/benchmark_timeline.json");
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < TIMELINE_RECORDS.size(); i++) {
          sb.append("  ").append(TIMELINE_RECORDS.get(i));
          if (i < TIMELINE_RECORDS.size() - 1) {
            sb.append(",");
          }
          sb.append("\n");
        }
        sb.append("]\n");
        Files.write(Paths.get(timelineFile), sb.toString().getBytes(StandardCharsets.UTF_8));
        log("Saved benchmark timeline to " + timelineFile);
      } catch (Exception e) {
        log("Warning: Failed to write timeline: " + e.getMessage());
      }
    }

    for (Storage pooled : nativeClientPool) {
      if (pooled != storage) {
        try {
          pooled.close();
        } catch (Throwable ignored) {
        }
      }
    }
    nativeClientPool.clear();

    if (storage != null) {
      try {
        storage.close();
      } catch (Throwable ignored) {
      }
      storage = null;
    }
    if (mockServer != null) {
      mockServer.close();
      mockServer = null;
      System.clearProperty("gcs.sidecar.endpoint");
      System.clearProperty("gcs.sidecar.control.endpoint");
      System.clearProperty("gcs.sidecar.default.transport");
      System.clearProperty("gcs.sidecar.transport");
    }
    log("Cleanup complete.");
  }

  @After
  public void tearDownTestMethod() throws Exception {
    int pauseSec = Integer.getInteger("gcs.bench.pause.sec", IS_LIVE ? 75 : 0);
    if (pauseSec > 0) {
      log(
          String.format(
              "Quiescing for %d seconds to isolate 60s Cloud Monitoring buckets...", pauseSec));
      Thread.sleep(pauseSec * 1000L);
    }
  }

  // =========================================================================
  // TELEMETRY & PERCENTILE HELPERS
  // =========================================================================

  /** Exact linear-interpolated percentile over a sorted non-empty sample list (`p` in `[0, 1]`). */
  static double percentile(List<Double> sorted, double p) {
    if (sorted == null || sorted.isEmpty()) {
      return 0.0;
    }
    if (sorted.size() == 1) {
      return sorted.get(0);
    }
    double rank = p * (sorted.size() - 1);
    int low = (int) Math.floor(rank);
    int high = (int) Math.ceil(rank);
    if (low == high) {
      return sorted.get(low);
    }
    double weight = rank - low;
    return sorted.get(low) * (1.0 - weight) + sorted.get(high) * weight;
  }

  private static double toDecimalMbPerSec(long totalBytes, double elapsedSec) {
    return elapsedSec > 0 ? (totalBytes / 1_000_000.0) / elapsedSec : 0.0;
  }

  private static double toBinaryMibPerSec(long totalBytes, double elapsedSec) {
    return elapsedSec > 0 ? (totalBytes / 1_048_576.0) / elapsedSec : 0.0;
  }

  private static long readJvmProcessCpuNanos() {
    java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
    try {
      Class<?> sunOsBean = Class.forName("com.sun.management.OperatingSystemMXBean");
      if (sunOsBean.isInstance(bean)) {
        Method m = sunOsBean.getMethod("getProcessCpuTime");
        return (Long) m.invoke(bean);
      }
    } catch (Throwable ignored) {
    }
    return 0L;
  }

  private static void resetJvmPeakRss() {
    try {
      Files.write(Paths.get("/proc/self/clear_refs"), "5".getBytes(StandardCharsets.US_ASCII));
    } catch (Throwable ignored) {
    }
  }

  private static double readJvmPeakRssMib() {
    try {
      List<String> lines = Files.readAllLines(Paths.get("/proc/self/status"), StandardCharsets.UTF_8);
      for (String line : lines) {
        if (line.startsWith("VmHWM:")) {
          String[] parts = line.trim().split("\\s+");
          if (parts.length >= 2) {
            return Long.parseLong(parts[1]) / 1024.0;
          }
        }
      }
    } catch (Throwable ignored) {
    }
    return 0.0;
  }

  private static final class WorkloadTelemetrySnapshot {
    final long jvmCpuStartNs;
    final StatsResponse rustStart;

    WorkloadTelemetrySnapshot(long jvmCpuStartNs, StatsResponse rustStart) {
      this.jvmCpuStartNs = jvmCpuStartNs;
      this.rustStart = rustStart;
    }
  }

  private static final class WorkloadTelemetryDelta {
    final double jvmCpuSec;
    final double jvmPeakRssMib;
    final double rustCpuSec;
    final double rustPeakRssMib;

    WorkloadTelemetryDelta(
        double jvmCpuSec, double jvmPeakRssMib, double rustCpuSec, double rustPeakRssMib) {
      this.jvmCpuSec = jvmCpuSec;
      this.jvmPeakRssMib = jvmPeakRssMib;
      this.rustCpuSec = rustCpuSec;
      this.rustPeakRssMib = rustPeakRssMib;
    }
  }

  private static WorkloadTelemetrySnapshot beginWorkloadTelemetry() {
    resetJvmPeakRss();
    StatsResponse rs = null;
    if (udsStorageHandler != null) {
      try {
        rs = udsStorageHandler.getStats(/* resetPeakRss= */ true);
      } catch (Throwable ignored) {
      }
    }
    return new WorkloadTelemetrySnapshot(readJvmProcessCpuNanos(), rs);
  }

  private static WorkloadTelemetryDelta endWorkloadTelemetry(WorkloadTelemetrySnapshot start) {
    long jvmEndNs = readJvmProcessCpuNanos();
    double jvmCpuSec = Math.max(0.0, (jvmEndNs - start.jvmCpuStartNs) / 1_000_000_000.0);
    double jvmHwmMib = readJvmPeakRssMib();
    double rustCpuSec = 0.0;
    double rustHwmMib = 0.0;
    if (udsStorageHandler != null && start.rustStart != null) {
      try {
        StatsResponse end = udsStorageHandler.getStats(/* resetPeakRss= */ false);
        double startTot = start.rustStart.getCpuUserSecs() + start.rustStart.getCpuSysSecs();
        double endTot = end.getCpuUserSecs() + end.getCpuSysSecs();
        rustCpuSec = Math.max(0.0, endTot - startTot);
        rustHwmMib = end.getPeakRssBytes() / 1_048_576.0;
      } catch (Throwable ignored) {
      }
    }
    return new WorkloadTelemetryDelta(jvmCpuSec, jvmHwmMib, rustCpuSec, rustHwmMib);
  }

  private static String resolveExistingObject(String preferred, String fallback) {
    try {
      Blob b = storage.get(BlobId.of(TEST_BUCKET, preferred));
      if (b != null) {
        return preferred;
      }
    } catch (Throwable ignored) {
    }
    return fallback;
  }

  // =========================================================================
  // SECTION 0: PURE UDS gRPC IPC PING MICROBENCHMARK
  // =========================================================================

  @Test
  public void test00_PingLatencyMicrobenchmark() throws Exception {
    banner("TEST 00: Pure UDS gRPC IPC Ping Round-Trip Microbenchmark");
    if (udsStorageHandler == null) {
      log("Skipping UDS Ping microbenchmark for Native driver.");
      return;
    }
    int[] sizes = new int[] {1, 1024, 64 * 1024, 1024 * 1024};
    String[] labels = new String[] {"1 Byte", "1 KiB", "64 KiB", "1 MiB"};

    for (int i = 0; i < 20; i++) {
      udsStorageHandler.ping(new byte[16]);
    }

    for (int s = 0; s < sizes.length; s++) {
      int size = sizes[s];
      String label = labels[s];
      byte[] payload = new byte[size];
      new Random(100 + s).nextBytes(payload);

      int iterations = (size > 64 * 1024) ? 50 : 100;
      List<Double> latencies = new ArrayList<Double>(iterations);
      long startTotalNs = System.nanoTime();

      for (int i = 0; i < iterations; i++) {
        long callStartNs = System.nanoTime();
        byte[] echo = udsStorageHandler.ping(payload);
        long elapsedNs = System.nanoTime() - callStartNs;
        latencies.add(elapsedNs / 1_000_000.0);
        Assert.assertEquals(payload.length, echo.length);
      }

      double totalSec = (System.nanoTime() - startTotalNs) / 1_000_000_000.0;
      Collections.sort(latencies);
      double p50 = percentile(latencies, 0.50);
      double p90 = percentile(latencies, 0.90);
      double p99 = percentile(latencies, 0.99);
      long totalBytes = (long) size * iterations;
      double mbSec = toDecimalMbPerSec(totalBytes, totalSec);
      double mibSec = toBinaryMibPerSec(totalBytes, totalSec);

      log(
          String.format(
              "UDS Ping %-8s : P50=%.3f ms | P90=%.3f ms | P99=%.3f ms | %.2f MB/s (%.2f MiB/s)",
              label, p50, p90, p99, mbSec, mibSec));

      recordedMetrics.add(
          new BenchmarkMetric(
              "Ping " + label,
              size,
              1,
              iterations,
              0.0,
              0.0,
              0.0,
              0.0,
              0.0,
              p50,
              p90,
              p99,
              mbSec,
              mibSec,
              getUsedHeapMb(),
              new WorkloadTelemetryDelta(0, 0, 0, 0)));
    }
  }

  // =========================================================================
  // SECTION 0A: PRE-FLIGHT SMOKE TEST
  // =========================================================================

  @Test
  public void test00a_SmokeTestBidiRead() throws Exception {
    banner("TEST 00A: Pre-flight Smoke Test (16 MiB Download)");
    String smokeObject = "smoke-test.bin";
    long startNs = System.nanoTime();
    byte[] data = storage.readAllBytes(BlobId.of(TEST_BUCKET, smokeObject));
    long elapsedNs = System.nanoTime() - startNs;

    Assert.assertNotNull(data);
    Assert.assertEquals(16 * 1024 * 1024, data.length);
    double sec = elapsedNs / 1_000_000_000.0;
    log(
        String.format(
            "PASSED: Read %,d bytes in %.3f s (%.2f MB/s / %.2f MiB/s).",
            data.length, sec, toDecimalMbPerSec(data.length, sec), toBinaryMibPerSec(data.length, sec)));
  }

  // =========================================================================
  // WORKLOAD 1 — OPEN (200 MB BidiRead Streaming Handshake ONLY)
  // =========================================================================

  @Test
  public void test00b_Open_200MB() throws Exception {
    banner("WORKLOAD 1: Open (Start BidiReadSession handshake on 200 MB object)");
    if (!isGrpcTransport()) {
      log("Skipping BidiRead Open workload under JSON transport (BidiReadObject is gRPC-only).");
      return;
    }

    String targetObject =
        resolveExistingObject(
            System.getProperty("gcs.bench.open.object", "bench-open-200mb.bin"),
            "bench-200mb-0.bin");
    BlobId blobId = BlobId.of(TEST_BUCKET, targetObject);
    int minIterations = Integer.getInteger("gcs.bench.open.iterations", IS_LIVE ? 50 : 5);

    if (ENABLE_WARMUP) {
      int warmupOps = IS_LIVE ? 3 : 1;
      for (int w = 0; w < warmupOps; w++) {
        try (BlobReadSession s = storage.blobReadSession(blobId).get(120, TimeUnit.SECONDS)) {
          Assert.assertNotNull(s.getBlobInfo());
        }
      }
    }

    WorkloadTelemetrySnapshot snap = beginWorkloadTelemetry();
    Instant wlStart = Instant.now();
    long loopStartNs = System.nanoTime();
    long minDeadlineMs =
        MIN_WINDOW_SEC > 0 ? (System.currentTimeMillis() + MIN_WINDOW_SEC * 1000L) : 0L;
    List<Double> openLatencies = new ArrayList<Double>(minIterations);

    while (openLatencies.size() < minIterations || System.currentTimeMillis() < minDeadlineMs) {
      BlobReadSession session = null;
      long startNs = System.nanoTime();
      try {
        session = storage.blobReadSession(blobId).get(120, TimeUnit.SECONDS);
        long elapsedNs = System.nanoTime() - startNs;
        Assert.assertNotNull(session.getBlobInfo());
        if (session.getBlobInfo().getSize() != null) {
          Assert.assertEquals(200L * 1024 * 1024, session.getBlobInfo().getSize().longValue());
        }
        openLatencies.add(elapsedNs / 1_000_000.0);
      } finally {
        // Close outside [startNs, elapsedNs] so Open measures strictly the BidiReadObject handshake.
        if (session != null) {
          session.close();
        }
      }
    }

    double activeWallSec = (System.nanoTime() - loopStartNs) / 1_000_000_000.0;
    Instant wlEnd = Instant.now();
    WorkloadTelemetryDelta delta = endWorkloadTelemetry(snap);
    recordTimeline("WL1_Open_200MB", wlStart, wlEnd);

    Collections.sort(openLatencies);
    double p50 = percentile(openLatencies, 0.50);
    double p90 = percentile(openLatencies, 0.90);
    double p99 = percentile(openLatencies, 0.99);
    double opsSec = activeWallSec > 0 ? (openLatencies.size() / activeWallSec) : 0.0;

    log(
        String.format(
            "Open (200MB) Results: %d opens in %.2fs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.1f ops/sec",
            openLatencies.size(), activeWallSec, p50, p90, p99, opsSec));

    recordedMetrics.add(
        new BenchmarkMetric(
            getTransportPrefix() + "Open (200MB)",
            200 * 1024 * 1024,
            1,
            openLatencies.size(),
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            p50,
            p90,
            p99,
            opsSec,
            opsSec,
            getUsedHeapMb(),
            delta));
  }

  // =========================================================================
  // WORKLOAD 2 — Read50x2x4 Serial (1:1 API Symmetry Across Native & UDS)
  // =========================================================================

  @Test
  public void test00c_Read50x2x4_Serial() throws Exception {
    banner("WORKLOAD 2: Read50x2x4 Serial (50 x 2 MiB ranges at 4 MiB stride)");
    String targetObject =
        resolveExistingObject(
            System.getProperty("gcs.bench.read50.object", "bench-open-200mb.bin"),
            "bench-200mb-0.bin");
    BlobId blobId = BlobId.of(TEST_BUCKET, targetObject);
    int minIterations = Integer.getInteger("gcs.bench.read50.iterations", IS_LIVE ? 20 : 2);
    boolean useBidi = isGrpcTransport();

    if (ENABLE_WARMUP) {
      runSingleSerial50x2x4Pass(blobId, useBidi);
    }

    WorkloadTelemetrySnapshot snap = beginWorkloadTelemetry();
    Instant wlStart = Instant.now();
    long activeTransferNs = 0L;
    long minDeadlineMs =
        MIN_WINDOW_SEC > 0 ? (System.currentTimeMillis() + MIN_WINDOW_SEC * 1000L) : 0L;
    List<Double> iterationLatencies = new ArrayList<Double>(minIterations);
    long totalBytesAllRuns = 0L;

    while (iterationLatencies.size() < minIterations
        || System.currentTimeMillis() < minDeadlineMs) {
      long elapsedNs = runSingleSerial50x2x4Pass(blobId, useBidi);
      activeTransferNs += elapsedNs;
      totalBytesAllRuns += 100L * 1024 * 1024;
      iterationLatencies.add(elapsedNs / 1_000_000.0);
    }

    Instant wlEnd = Instant.now();
    WorkloadTelemetryDelta delta = endWorkloadTelemetry(snap);
    recordTimeline("WL2_Read50x2x4_Serial", wlStart, wlEnd);

    Collections.sort(iterationLatencies);
    double p50 = percentile(iterationLatencies, 0.50);
    double p90 = percentile(iterationLatencies, 0.90);
    double p99 = percentile(iterationLatencies, 0.99);
    double activeSec = activeTransferNs / 1_000_000_000.0;
    double mbSec = toDecimalMbPerSec(totalBytesAllRuns, activeSec);
    double mibSec = toBinaryMibPerSec(totalBytesAllRuns, activeSec);

    log(
        String.format(
            "Read50x2x4 Serial Results: %d runs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.2f MB/s (%.2f MiB/s)",
            iterationLatencies.size(), p50, p90, p99, mbSec, mibSec));

    recordedMetrics.add(
        new BenchmarkMetric(
            getTransportPrefix() + "Read50x2x4 Serial",
            100 * 1024 * 1024,
            1,
            iterationLatencies.size(),
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            p50,
            p90,
            p99,
            mbSec,
            mibSec,
            getUsedHeapMb(),
            delta));
  }

  private static long runSingleSerial50x2x4Pass(BlobId blobId, boolean useBidi) throws Exception {
    if (useBidi) {
      try (BlobReadSession session = storage.blobReadSession(blobId).get(120, TimeUnit.SECONDS)) {
        long startNs = System.nanoTime();
        long bytes = 0;
        for (int r = 0; r < 50; r++) {
          long offset = r * 4L * 1024 * 1024;
          byte[] b =
              session
                  .readAs(
                      ReadProjectionConfigs.asFutureBytes()
                          .withRangeSpec(RangeSpec.of(offset, 2 * 1024 * 1024)))
                  .get(120, TimeUnit.SECONDS);
          bytes += b.length;
        }
        long elapsed = System.nanoTime() - startNs;
        Assert.assertEquals(100L * 1024 * 1024, bytes);
        return elapsed;
      }
    } else {
      ByteBuffer bb = ByteBuffer.allocate(2 * 1024 * 1024);
      long startNs = System.nanoTime();
      long bytes = 0;
      for (int r = 0; r < 50; r++) {
        long offset = r * 4L * 1024 * 1024;
        bb.clear();
        try (ReadChannel reader = storage.reader(blobId)) {
          reader.seek(offset);
          reader.limit(offset + 2L * 1024 * 1024);
          while (bb.hasRemaining()) {
            if (reader.read(bb) == -1) {
              break;
            }
          }
        }
        bytes += bb.position();
      }
      long elapsed = System.nanoTime() - startNs;
      Assert.assertEquals(100L * 1024 * 1024, bytes);
      return elapsed;
    }
  }

  // =========================================================================
  // WORKLOAD 3 — Read50x2x4 Concurrent (Strict 1:1 Symmetry on Native & UDS)
  // =========================================================================

  @Test
  public void test00d_Read50x2x4_Concurrent() throws Exception {
    banner("WORKLOAD 3: Read50x2x4 Concurrent (50 x 2 MiB ranges concurrently, 1:1 API symmetry)");
    String targetObject =
        resolveExistingObject(
            System.getProperty("gcs.bench.read50.object", "bench-open-200mb.bin"),
            "bench-200mb-0.bin");
    final BlobId blobId = BlobId.of(TEST_BUCKET, targetObject);
    int minIterations = Integer.getInteger("gcs.bench.read50.iterations", IS_LIVE ? 20 : 2);
    int concurrency = Integer.getInteger("gcs.bench.wl3.concurrency", 16);
    String defaultMode = isGrpcTransport() ? "single" : "reader";
    String wl3Mode = System.getProperty("gcs.bench.wl3.mode", defaultMode).trim().toLowerCase();
    int sessionPoolSize =
        "pooled".equals(wl3Mode)
            ? Math.max(1, Math.min(50, Integer.getInteger("gcs.bench.wl3.sessions", concurrency)))
            : 1;

    log(
        String.format(
            "Executing Read50x2x4 Concurrent (driver=%s, mode=%s, sessions=%d, threads=%d)...",
            DRIVER, wl3Mode, sessionPoolSize, concurrency));

    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    try {
      if (ENABLE_WARMUP) {
        runSingleConcurrent50x2x4Pass(blobId, wl3Mode, sessionPoolSize, executor);
      }

      WorkloadTelemetrySnapshot snap = beginWorkloadTelemetry();
      Instant wlStart = Instant.now();
      long activeTransferNs = 0L;
      long minDeadlineMs =
          MIN_WINDOW_SEC > 0 ? (System.currentTimeMillis() + MIN_WINDOW_SEC * 1000L) : 0L;
      List<Double> iterationLatencies = new ArrayList<Double>(minIterations);
      long totalBytesAllRuns = 0L;

      while (iterationLatencies.size() < minIterations
          || System.currentTimeMillis() < minDeadlineMs) {
        long elapsedNs =
            runSingleConcurrent50x2x4Pass(blobId, wl3Mode, sessionPoolSize, executor);
        activeTransferNs += elapsedNs;
        totalBytesAllRuns += 100L * 1024 * 1024;
        iterationLatencies.add(elapsedNs / 1_000_000.0);
      }

      Instant wlEnd = Instant.now();
      WorkloadTelemetryDelta delta = endWorkloadTelemetry(snap);
      recordTimeline("WL3_Read50x2x4_Concurrent_" + wl3Mode, wlStart, wlEnd);

      Collections.sort(iterationLatencies);
      double p50 = percentile(iterationLatencies, 0.50);
      double p90 = percentile(iterationLatencies, 0.90);
      double p99 = percentile(iterationLatencies, 0.99);
      double activeSec = activeTransferNs / 1_000_000_000.0;
      double mbSec = toDecimalMbPerSec(totalBytesAllRuns, activeSec);
      double mibSec = toBinaryMibPerSec(totalBytesAllRuns, activeSec);

      log(
          String.format(
              "Read50x2x4 Concurrent [%s] Results: %d runs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.2f MB/s (%.2f MiB/s)",
              wl3Mode, iterationLatencies.size(), p50, p90, p99, mbSec, mibSec));

      recordedMetrics.add(
          new BenchmarkMetric(
              getTransportPrefix() + "Read50x2x4 Conc (" + wl3Mode + ")",
              100 * 1024 * 1024,
              concurrency,
              iterationLatencies.size(),
              0.0,
              0.0,
              0.0,
              0.0,
              0.0,
              p50,
              p90,
              p99,
              mbSec,
              mibSec,
              getUsedHeapMb(),
              delta));
    } finally {
      executor.shutdown();
      executor.awaitTermination(30, TimeUnit.SECONDS);
    }
  }

  private static long runSingleConcurrent50x2x4Pass(
      final BlobId blobId, String mode, int sessionPoolSize, ExecutorService executor)
      throws Exception {
    // Identical execution path for Native Java, uds-sidecar, and uds-inproc!
    if (!"reader".equals(mode) && isGrpcTransport()) {
      final List<BlobReadSession> sessions = new ArrayList<BlobReadSession>(sessionPoolSize);
      try {
        for (int s = 0; s < sessionPoolSize; s++) {
          sessions.add(pickClient(s).blobReadSession(blobId).get(120, TimeUnit.SECONDS));
        }
        long startNs = System.nanoTime();
        List<ApiFuture<byte[]>> rangeFutures = new ArrayList<ApiFuture<byte[]>>(50);
        for (int r = 0; r < 50; r++) {
          long offset = r * 4L * 1024 * 1024;
          BlobReadSession sess = sessions.get(r % sessions.size());
          rangeFutures.add(
              sess.readAs(
                  ReadProjectionConfigs.asFutureBytes()
                      .withRangeSpec(RangeSpec.of(offset, 2 * 1024 * 1024))));
        }
        List<byte[]> results = ApiFutures.allAsList(rangeFutures).get(120, TimeUnit.SECONDS);
        long elapsedNs = System.nanoTime() - startNs;
        long total = 0;
        for (byte[] chunk : results) {
          total += chunk.length;
        }
        Assert.assertEquals(100L * 1024 * 1024, total);
        return elapsedNs;
      } finally {
        for (BlobReadSession s : sessions) {
          try {
            s.close();
          } catch (IOException ignored) {
          }
        }
      }
    } else {
      long startNs = System.nanoTime();
      List<Future<Integer>> futures = new ArrayList<Future<Integer>>(50);
      for (int r = 0; r < 50; r++) {
        final int idx = r;
        final long offset = r * 4L * 1024 * 1024;
        futures.add(
            executor.submit(
                new Callable<Integer>() {
                  @Override
                  public Integer call() throws Exception {
                    ByteBuffer bb = ByteBuffer.allocate(2 * 1024 * 1024);
                    try (ReadChannel reader = pickClient(idx).reader(blobId)) {
                      reader.seek(offset);
                      reader.limit(offset + 2L * 1024 * 1024);
                      while (bb.hasRemaining()) {
                        if (reader.read(bb) == -1) {
                          break;
                        }
                      }
                    }
                    return bb.position();
                  }
                }));
      }
      long total = 0;
      for (Future<Integer> f : futures) {
        total += f.get(120, TimeUnit.SECONDS);
      }
      long elapsedNs = System.nanoTime() - startNs;
      Assert.assertEquals(100L * 1024 * 1024, total);
      return elapsedNs;
    }
  }

  // =========================================================================
  // WORKLOAD 4 — Random8K (2,000 Random 8 KiB Range Seeks + Dynamic Label)
  // =========================================================================

  @Test
  public void test00e_Random8K() throws Exception {
    banner("WORKLOAD 4: Random8K (2,000 random 8 KiB range reads from 100 MB object)");
    String targetObject =
        resolveExistingObject(
            System.getProperty("gcs.bench.random.object", "bench-random-100mb.bin"),
            "bench-open-200mb.bin");
    BlobId blobId = BlobId.of(TEST_BUCKET, targetObject);
    int minSeekCount = Integer.getInteger("gcs.bench.random8k.count", IS_LIVE ? 2000 : 40);

    Random rng = new Random(42);
    long maxOffset = (100L * 1024 * 1024) - 8192L;
    long[] precomputedOffsets = new long[Math.max(minSeekCount, 2000)];
    for (int i = 0; i < precomputedOffsets.length; i++) {
      precomputedOffsets[i] = Math.abs(rng.nextLong()) % maxOffset;
    }

    WorkloadTelemetrySnapshot snap;
    Instant wlStart;
    long totalStartNs;
    List<Double> seekLatencies = new ArrayList<Double>(minSeekCount);

    if (isGrpcTransport()) {
      try (BlobReadSession session = storage.blobReadSession(blobId).get(120, TimeUnit.SECONDS)) {
        if (ENABLE_WARMUP) {
          int warmupSeeks = IS_LIVE ? 50 : 5;
          for (int w = 0; w < warmupSeeks; w++) {
            session
                .readAs(
                    ReadProjectionConfigs.asFutureBytes()
                        .withRangeSpec(
                            RangeSpec.of(precomputedOffsets[w % precomputedOffsets.length], 8192)))
                .get(120, TimeUnit.SECONDS);
          }
        }

        snap = beginWorkloadTelemetry();
        wlStart = Instant.now();
        totalStartNs = System.nanoTime();
        long minDeadlineMs =
            MIN_WINDOW_SEC > 0 ? (System.currentTimeMillis() + MIN_WINDOW_SEC * 1000L) : 0L;

        int i = 0;
        while (seekLatencies.size() < minSeekCount
            || System.currentTimeMillis() < minDeadlineMs) {
          long off = precomputedOffsets[i % precomputedOffsets.length];
          i++;
          long opStartNs = System.nanoTime();
          byte[] data =
              session
                  .readAs(
                      ReadProjectionConfigs.asFutureBytes().withRangeSpec(RangeSpec.of(off, 8192)))
                  .get(120, TimeUnit.SECONDS);
          long opElapsedNs = System.nanoTime() - opStartNs;
          Assert.assertEquals(8192, data.length);
          seekLatencies.add(opElapsedNs / 1_000_000.0);
        }
      }
    } else {
      ByteBuffer bb = ByteBuffer.allocate(8192);
      if (ENABLE_WARMUP) {
        bb.clear();
        try (ReadChannel r = storage.reader(blobId)) {
          r.seek(precomputedOffsets[0]);
          r.limit(precomputedOffsets[0] + 8192);
          while (bb.hasRemaining() && r.read(bb) != -1) {}
        }
      }
      snap = beginWorkloadTelemetry();
      wlStart = Instant.now();
      totalStartNs = System.nanoTime();
      long minDeadlineMs =
          MIN_WINDOW_SEC > 0 ? (System.currentTimeMillis() + MIN_WINDOW_SEC * 1000L) : 0L;

      int i = 0;
      while (seekLatencies.size() < minSeekCount || System.currentTimeMillis() < minDeadlineMs) {
        long off = precomputedOffsets[i % precomputedOffsets.length];
        i++;
        bb.clear();
        long opStartNs = System.nanoTime();
        try (ReadChannel r = storage.reader(blobId)) {
          r.seek(off);
          r.limit(off + 8192);
          while (bb.hasRemaining()) {
            if (r.read(bb) == -1) {
              break;
            }
          }
        }
        long opElapsedNs = System.nanoTime() - opStartNs;
        Assert.assertEquals(8192, bb.position());
        seekLatencies.add(opElapsedNs / 1_000_000.0);
      }
    }

    double totalSec = (System.nanoTime() - totalStartNs) / 1_000_000_000.0;
    Instant wlEnd = Instant.now();
    WorkloadTelemetryDelta delta = endWorkloadTelemetry(snap);
    recordTimeline("WL4_Random8K", wlStart, wlEnd);

    Collections.sort(seekLatencies);
    double p50 = percentile(seekLatencies, 0.50);
    double p90 = percentile(seekLatencies, 0.90);
    double p99 = percentile(seekLatencies, 0.99);
    double opsSec = totalSec > 0 ? (seekLatencies.size() / totalSec) : 0.0;
    long totalBytes = (long) seekLatencies.size() * 8192L;

    log(
        String.format(
            "Random8K Results: %d seeks in %.2fs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.1f ops/sec (%.2f MB/s / %.2f MiB/s)",
            seekLatencies.size(),
            totalSec,
            p50,
            p90,
            p99,
            opsSec,
            toDecimalMbPerSec(totalBytes, totalSec),
            toBinaryMibPerSec(totalBytes, totalSec)));

    recordedMetrics.add(
        new BenchmarkMetric(
            getTransportPrefix() + String.format("Random8K (%d seeks)", seekLatencies.size()),
            8192,
            1,
            seekLatencies.size(),
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            p50,
            p90,
            p99,
            opsSec,
            opsSec,
            getUsedHeapMb(),
            delta));
  }

  // =========================================================================
  // SECTION 1: FUNCTIONAL VERIFICATION OF VENEER METHODS
  // =========================================================================

  @Test
  public void test01_VeneerCreateAndReadBytes() throws Exception {
    banner("TEST 01: Veneer create() and readAllBytes() round-trip");
    String objectName = "veneer-byte-test-" + System.nanoTime() + ".bin";
    byte[] originalData = new byte[64 * 1024];
    new Random(42).nextBytes(originalData);

    BlobInfo info = BlobInfo.newBuilder(TEST_BUCKET, objectName).build();
    Blob created = storage.create(info, originalData);
    Assert.assertNotNull(created);

    byte[] downloaded = storage.readAllBytes(created.getBlobId());
    Assert.assertArrayEquals(originalData, downloaded);

    Assert.assertTrue(storage.delete(created.getBlobId()));
    Assert.assertNull("Deleted blob must return null on get()", storage.get(created.getBlobId()));
  }

  @Test
  public void test02_VeneerReadAndWriteChannels() throws Exception {
    banner("TEST 02: Veneer ReadChannel & WriteChannel streaming");
    String objectName = "veneer-channel-test-" + System.nanoTime() + ".bin";
    byte[] originalData = new byte[256 * 1024];
    new Random(99).nextBytes(originalData);

    BlobInfo info = BlobInfo.newBuilder(TEST_BUCKET, objectName).build();
    try (WriteChannel writer = storage.writer(info)) {
      ByteBuffer buf = ByteBuffer.wrap(originalData);
      while (buf.hasRemaining()) {
        writer.write(buf);
      }
    }

    byte[] readBack = new byte[originalData.length];
    ByteBuffer readBuf = ByteBuffer.wrap(readBack);
    try (ReadChannel reader = storage.reader(info.getBlobId())) {
      while (readBuf.hasRemaining()) {
        if (reader.read(readBuf) == -1) {
          break;
        }
      }
    }
    Assert.assertArrayEquals(originalData, readBack);
    Assert.assertTrue(storage.delete(info.getBlobId()));
  }

  @Test
  public void test03_VeneerCreateFromAndDownloadToPath() throws Exception {
    banner("TEST 03: Veneer createFrom(Path) and downloadTo(Path)");
    Path srcFile = Files.createTempFile("uds_veneer_upload_", ".dat");
    Path dstFile = Files.createTempFile("uds_veneer_download_", ".dat");
    try {
      byte[] payload = new byte[512 * 1024];
      new Random(1234).nextBytes(payload);
      Files.write(srcFile, payload);

      String objectName = "veneer-file-test-" + System.nanoTime() + ".dat";
      Blob created =
          storage.createFrom(BlobInfo.newBuilder(TEST_BUCKET, objectName).build(), srcFile);
      Assert.assertNotNull(created);

      storage.downloadTo(created.getBlobId(), dstFile);
      Assert.assertArrayEquals(payload, Files.readAllBytes(dstFile));
      Assert.assertTrue(storage.delete(created.getBlobId()));
    } finally {
      Files.deleteIfExists(srcFile);
      Files.deleteIfExists(dstFile);
    }
  }

  @Test
  public void test04_VeneerListAndGetOperations() throws Exception {
    banner("TEST 04: Veneer list(prefix) and get() operations");
    String prefix = "list-test-" + System.nanoTime() + "/";
    for (int i = 0; i < 5; i++) {
      BlobInfo info = BlobInfo.newBuilder(TEST_BUCKET, prefix + "item-" + i).build();
      storage.create(info, ("data-" + i).getBytes(StandardCharsets.UTF_8));
    }

    Page<Blob> page =
        storage.list(TEST_BUCKET, BlobListOption.prefix(prefix), BlobListOption.pageSize(10));
    int count = 0;
    for (Blob b : page.getValues()) {
      Assert.assertTrue(b.getName().startsWith(prefix));
      count++;
    }
    Assert.assertEquals(5, count);

    Blob fetched = storage.get(BlobId.of(TEST_BUCKET, prefix + "item-0"));
    Assert.assertNotNull(fetched);
    Assert.assertEquals(6L, fetched.getSize().longValue());

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(storage.delete(BlobId.of(TEST_BUCKET, prefix + "item-" + i)));
    }
  }

  // =========================================================================
  // WORKLOAD 7a & 7b — ListObjects (100 Items) & ListBuckets (100 Items)
  // =========================================================================

  @Test
  public void test09a_ListObjects100Benchmark() throws Exception {
    banner("WORKLOAD 7a: ListObjects (100 Items under isolated prefix)");
    String listPrefix = System.getProperty("gcs.bench.list.prefix", "list-bench-100/");
    int minIters = Integer.getInteger("gcs.bench.list.iterations", IS_LIVE ? 50 : 5);

    if (ENABLE_WARMUP) {
      int warmup = IS_LIVE ? 5 : 1;
      for (int w = 0; w < warmup; w++) {
        Page<Blob> p =
            storage.list(
                TEST_BUCKET, BlobListOption.prefix(listPrefix), BlobListOption.pageSize(100));
        for (Blob ignored : p.getValues()) {}
      }
    }

    WorkloadTelemetrySnapshot snap = beginWorkloadTelemetry();
    Instant objStart = Instant.now();
    long startTotalNs = System.nanoTime();
    long minDeadlineMs =
        MIN_WINDOW_SEC > 0 ? (System.currentTimeMillis() + MIN_WINDOW_SEC * 1000L) : 0L;
    List<Double> listLatencies = new ArrayList<Double>(minIters);
    int lastCount = 0;

    while (listLatencies.size() < minIters || System.currentTimeMillis() < minDeadlineMs) {
      long start = System.nanoTime();
      Page<Blob> page =
          storage.list(
              TEST_BUCKET, BlobListOption.prefix(listPrefix), BlobListOption.pageSize(100));
      int count = 0;
      for (Blob ignored : page.getValues()) {
        count++;
      }
      long elapsedNs = System.nanoTime() - start;
      lastCount = count;
      listLatencies.add(elapsedNs / 1_000_000.0);
    }

    double totalSec = (System.nanoTime() - startTotalNs) / 1_000_000_000.0;
    Instant objEnd = Instant.now();
    WorkloadTelemetryDelta delta = endWorkloadTelemetry(snap);
    recordTimeline("WL7a_ListObjects_100", objStart, objEnd);

    if (!IS_LIVE || Boolean.parseBoolean(System.getProperty("gcs.bench.strict.list100", "false"))) {
      Assert.assertEquals("Expected 100 objects under " + listPrefix, 100, lastCount);
    }

    Collections.sort(listLatencies);
    double p50 = percentile(listLatencies, 0.50);
    double p90 = percentile(listLatencies, 0.90);
    double p99 = percentile(listLatencies, 0.99);
    double opsSec = totalSec > 0 ? (listLatencies.size() / totalSec) : 0.0;

    log(
        String.format(
            "ListObjects (%d items) across %d runs: P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.1f ops/sec",
            lastCount, listLatencies.size(), p50, p90, p99, opsSec));

    recordedMetrics.add(
        new BenchmarkMetric(
            getTransportPrefix() + "ListObjects (" + lastCount + " items)",
            lastCount,
            1,
            listLatencies.size(),
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            p50,
            p90,
            p99,
            opsSec,
            opsSec,
            getUsedHeapMb(),
            delta));
  }

  @Test
  public void test09b_ListBucketsBenchmark() throws Exception {
    banner("WORKLOAD 7b: ListBuckets (pageSize=100)");
    int minIters = Integer.getInteger("gcs.bench.list.iterations", IS_LIVE ? 50 : 5);

    if (ENABLE_WARMUP) {
      int warmup = IS_LIVE ? 5 : 1;
      for (int w = 0; w < warmup; w++) {
        Page<Bucket> p = storage.list(BucketListOption.pageSize(100));
        for (Bucket ignored : p.getValues()) {}
      }
    }

    WorkloadTelemetrySnapshot snap = beginWorkloadTelemetry();
    Instant bktStart = Instant.now();
    long startTotalNs = System.nanoTime();
    long minDeadlineMs =
        MIN_WINDOW_SEC > 0 ? (System.currentTimeMillis() + MIN_WINDOW_SEC * 1000L) : 0L;
    List<Double> bucketLatencies = new ArrayList<Double>(minIters);
    int lastCount = 0;

    while (bucketLatencies.size() < minIters || System.currentTimeMillis() < minDeadlineMs) {
      long start = System.nanoTime();
      Page<Bucket> page = storage.list(BucketListOption.pageSize(100));
      int count = 0;
      for (Bucket ignored : page.getValues()) {
        count++;
      }
      long elapsedNs = System.nanoTime() - start;
      lastCount = count;
      bucketLatencies.add(elapsedNs / 1_000_000.0);
    }

    double totalSec = (System.nanoTime() - startTotalNs) / 1_000_000_000.0;
    Instant bktEnd = Instant.now();
    WorkloadTelemetryDelta delta = endWorkloadTelemetry(snap);
    recordTimeline("WL7b_ListBuckets", bktStart, bktEnd);

    if (!IS_LIVE) {
      Assert.assertEquals(100, lastCount);
    }

    Collections.sort(bucketLatencies);
    double p50 = percentile(bucketLatencies, 0.50);
    double p90 = percentile(bucketLatencies, 0.90);
    double p99 = percentile(bucketLatencies, 0.99);
    double opsSec = totalSec > 0 ? (bucketLatencies.size() / totalSec) : 0.0;

    log(
        String.format(
            "ListBuckets (%d buckets) across %d runs: P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.1f ops/sec",
            lastCount, bucketLatencies.size(), p50, p90, p99, opsSec));

    recordedMetrics.add(
        new BenchmarkMetric(
            getTransportPrefix() + "ListBuckets (" + lastCount + " items)",
            lastCount,
            1,
            bucketLatencies.size(),
            0.0,
            0.0,
            0.0,
            0.0,
            0.0,
            p50,
            p90,
            p99,
            opsSec,
            opsSec,
            getUsedHeapMb(),
            delta));
  }

  // =========================================================================
  // WORKLOAD 5 — Read 16 MiB Chunks of 200 MiB (48 Workers, 5 Mins Live)
  // =========================================================================

  @Test
  public void test10_CloudpathWorkload4_Read16MbChunkOf200Mb() throws Exception {
    int concurrency = Integer.getInteger("gcs.bench.workload4.concurrency", IS_LIVE ? 48 : 2);
    int durationSec = Integer.getInteger("gcs.bench.duration.sec", IS_LIVE ? 300 : 0);
    int iterations = Integer.getInteger("gcs.bench.workload4.iterations", IS_LIVE ? 4 : 2);
    runRead16MbChunksWorkload(concurrency, durationSec, iterations);
  }

  private void runRead16MbChunksWorkload(
      final int concurrency, final int durationSec, final int iterations) throws Exception {
    final String apiMode =
        System.getProperty("gcs.bench.wl5.api", isGrpcTransport() ? "bidi" : "reader")
            .trim()
            .toLowerCase();
    banner(
        String.format(
            "WORKLOAD 5: Read 16 MiB chunks of 200 MiB (%d workers, duration=%ds, api=%s)",
            concurrency, durationSec, apiMode));

    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    try {
      // Optional steady-state warmup before measurement window
      if (ENABLE_WARMUP) {
        int warmupSec = Integer.getInteger("gcs.bench.wl5.warmup.sec", IS_LIVE ? 15 : 0);
        if (warmupSec > 0) {
          log("Running " + warmupSec + "s unrecorded warmup on " + concurrency + " workers...");
          executeRead16MbWorkers(executor, concurrency, warmupSec, 1, apiMode, null, null, null);
        } else {
          executeRead16MbWorkers(executor, Math.min(2, concurrency), 0, 1, apiMode, null, null, null);
        }
      }

      WorkloadTelemetrySnapshot snap = beginWorkloadTelemetry();
      Instant wlStart = Instant.now();
      List<Double> readLatencies = Collections.synchronizedList(new ArrayList<Double>());
      AtomicLong totalBytes = new AtomicLong(0L);
      AtomicInteger totalOps = new AtomicInteger(0);

      long startNs = System.nanoTime();
      executeRead16MbWorkers(
          executor,
          concurrency,
          durationSec,
          iterations,
          apiMode,
          readLatencies,
          totalBytes,
          totalOps);
      double totalSec = (System.nanoTime() - startNs) / 1_000_000_000.0;

      Instant wlEnd = Instant.now();
      WorkloadTelemetryDelta delta = endWorkloadTelemetry(snap);
      recordTimeline("WL5_Read16Mb_Workers48_" + apiMode, wlStart, wlEnd);

      Assert.assertFalse("Expected completed 16 MiB chunk reads in WL5", readLatencies.isEmpty());
      Collections.sort(readLatencies);
      double p50 = percentile(readLatencies, 0.50);
      double p90 = percentile(readLatencies, 0.90);
      double p99 = percentile(readLatencies, 0.99);
      double mbSec = toDecimalMbPerSec(totalBytes.get(), totalSec);
      double mibSec = toBinaryMibPerSec(totalBytes.get(), totalSec);

      log(
          String.format(
              "WL5 (Read 16MB/%s) Results: %d ops in %.2fs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.2f MB/s (%.2f MiB/s)",
              apiMode, totalOps.get(), totalSec, p50, p90, p99, mbSec, mibSec));

      recordedMetrics.add(
          new BenchmarkMetric(
              getTransportPrefix() + "WL5 Read 16MB (" + apiMode + ")",
              16 * 1024 * 1024,
              concurrency,
              totalOps.get(),
              0.0,
              0.0,
              0.0,
              0.0,
              0.0,
              p50,
              p90,
              p99,
              mbSec,
              mibSec,
              getUsedHeapMb(),
              delta));
    } finally {
      executor.shutdown();
      executor.awaitTermination(60, TimeUnit.SECONDS);
    }
  }

  private static void executeRead16MbWorkers(
      ExecutorService executor,
      int concurrency,
      final int durationSec,
      final int iterations,
      final String apiMode,
      final List<Double> latenciesOut,
      final AtomicLong bytesOut,
      final AtomicInteger opsOut)
      throws Exception {
    final long chunkLen = 16L * 1024 * 1024;
    final int chunksPerObject = 12; // 12 * 16 MiB = 192 MiB <= 200 MiB
    final long deadlineMs =
        durationSec > 0 ? (System.currentTimeMillis() + durationSec * 1000L) : Long.MAX_VALUE;

    List<Future<?>> futures = new ArrayList<Future<?>>(concurrency);
    for (int w = 0; w < concurrency; w++) {
      final int workerId = w;
      futures.add(
          executor.submit(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  Storage client = pickClient(workerId);
                  BlobId blobId =
                      BlobId.of(TEST_BUCKET, String.format("bench-200mb-%d.bin", workerId % 48));
                  if ("bidi".equals(apiMode) && isGrpcTransport()) {
                    try (BlobReadSession session =
                        client.blobReadSession(blobId).get(120, TimeUnit.SECONDS)) {
                      int count = 0;
                      while ((durationSec > 0 && System.currentTimeMillis() < deadlineMs)
                          || (durationSec == 0 && count < iterations)) {
                        long offset = (count % chunksPerObject) * chunkLen;
                        count++;
                        long opStart = System.nanoTime();
                        byte[] bytes =
                            session
                                .readAs(
                                    ReadProjectionConfigs.asFutureBytes()
                                        .withRangeSpec(RangeSpec.of(offset, chunkLen)))
                                .get(120, TimeUnit.SECONDS);
                        long opNs = System.nanoTime() - opStart;
                        if (latenciesOut != null && bytes.length > 0) {
                          latenciesOut.add(opNs / 1_000_000.0);
                          bytesOut.addAndGet(bytes.length);
                          opsOut.incrementAndGet();
                        }
                      }
                    }
                  } else {
                    ByteBuffer buf = ByteBuffer.allocate((int) chunkLen);
                    int count = 0;
                    while ((durationSec > 0 && System.currentTimeMillis() < deadlineMs)
                        || (durationSec == 0 && count < iterations)) {
                      long offset = (count % chunksPerObject) * chunkLen;
                      count++;
                      buf.clear();
                      long opStart = System.nanoTime();
                      try (ReadChannel reader = client.reader(blobId)) {
                        reader.seek(offset);
                        // Explicit range limit prevents unbounded 200 MiB full-object streaming!
                        reader.limit(offset + chunkLen);
                        while (buf.hasRemaining()) {
                          if (reader.read(buf) == -1) {
                            break;
                          }
                        }
                      }
                      long opNs = System.nanoTime() - opStart;
                      if (latenciesOut != null && buf.position() > 0) {
                        latenciesOut.add(opNs / 1_000_000.0);
                        bytesOut.addAndGet(buf.position());
                        opsOut.incrementAndGet();
                      }
                    }
                  }
                  return null;
                }
              }));
    }
    for (Future<?> f : futures) {
      f.get();
    }
  }

  // =========================================================================
  // WORKLOAD 6 — Write 200 MiB Objects (48 Workers, 5 Mins Live, Zonal Enabled)
  // =========================================================================

  @Test
  public void test11_CloudpathWorkload5_Write200Mb() throws Exception {
    int concurrency = Integer.getInteger("gcs.bench.workload5.concurrency", IS_LIVE ? 48 : 2);
    int durationSec = Integer.getInteger("gcs.bench.duration.sec", IS_LIVE ? 300 : 0);
    int iterations = Integer.getInteger("gcs.bench.workload5.iterations", IS_LIVE ? 2 : 1);
    runWrite200MbWorkload(concurrency, durationSec, iterations);
  }

  private void runWrite200MbWorkload(
      final int concurrency, final int durationSec, final int iterations) throws Exception {
    // Use BidiWriteObject (blobAppendableUpload) by default on gRPC/Zonal, or writer() on JSON/Regional.
    String defaultWriteApi =
        (isGrpcTransport() && (!IS_LIVE || TEST_BUCKET.contains("zonal")))
            ? "appendable"
            : (isGrpcTransport() ? "writer" : "writer");
    final String writeApi =
        System.getProperty("gcs.bench.wl6.api", defaultWriteApi).trim().toLowerCase();
    final long targetSize = IS_LIVE ? (200L * 1024 * 1024) : (4L * 1024 * 1024);

    banner(
        String.format(
            "WORKLOAD 6: Write %,d MiB objects (%d workers, duration=%ds, api=%s)",
            targetSize / (1024 * 1024), concurrency, durationSec, writeApi));

    final byte[] reusableChunk = new byte[4 * 1024 * 1024];
    new Random(42).nextBytes(reusableChunk);
    final Queue<BlobId> objectsToCleanup = new ConcurrentLinkedQueue<BlobId>();

    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    try {
      if (ENABLE_WARMUP) {
        int warmupSec = Integer.getInteger("gcs.bench.wl6.warmup.sec", IS_LIVE ? 15 : 0);
        if (warmupSec > 0) {
          log("Running " + warmupSec + "s unrecorded write warmup...");
          executeWriteWorkers(
              executor,
              concurrency,
              warmupSec,
              1,
              targetSize,
              writeApi,
              reusableChunk,
              objectsToCleanup,
              null,
              null,
              null);
        } else {
          executeWriteWorkers(
              executor,
              Math.min(2, concurrency),
              0,
              1,
              targetSize,
              writeApi,
              reusableChunk,
              objectsToCleanup,
              null,
              null,
              null);
        }
      }

      WorkloadTelemetrySnapshot snap = beginWorkloadTelemetry();
      Instant wlStart = Instant.now();
      List<Double> writeLatencies = Collections.synchronizedList(new ArrayList<Double>());
      AtomicLong totalBytes = new AtomicLong(0L);
      AtomicInteger totalOps = new AtomicInteger(0);

      long startNs = System.nanoTime();
      double totalSec;
      Instant wlEnd;
      WorkloadTelemetryDelta delta;
      try {
        executeWriteWorkers(
            executor,
            concurrency,
            durationSec,
            iterations,
            targetSize,
            writeApi,
            reusableChunk,
            objectsToCleanup,
            writeLatencies,
            totalBytes,
            totalOps);
      } finally {
        totalSec = (System.nanoTime() - startNs) / 1_000_000_000.0;
        wlEnd = Instant.now();
        delta = endWorkloadTelemetry(snap);
        // Post-workload cleanup strictly OUTSIDE [wlStart, wlEnd] and [startNs, totalSec],
        // and guaranteed to run even if an exception occurs mid-stream!
        cleanupCreatedObjects(executor, objectsToCleanup);
      }
      recordTimeline("WL6_Write200Mb_Workers48_" + writeApi, wlStart, wlEnd);

      Assert.assertFalse("Expected completed writes in WL6", writeLatencies.isEmpty());
      Collections.sort(writeLatencies);
      double p50 = percentile(writeLatencies, 0.50);
      double p90 = percentile(writeLatencies, 0.90);
      double p99 = percentile(writeLatencies, 0.99);
      double mbSec = toDecimalMbPerSec(totalBytes.get(), totalSec);
      double mibSec = toBinaryMibPerSec(totalBytes.get(), totalSec);

      log(
          String.format(
              "WL6 (Write/%s) Results: %d ops in %.2fs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.2f MB/s (%.2f MiB/s)",
              writeApi, totalOps.get(), totalSec, p50, p90, p99, mbSec, mibSec));

      recordedMetrics.add(
          new BenchmarkMetric(
              getTransportPrefix() + "WL6 Write 200MB (" + writeApi + ")",
              (int) targetSize,
              concurrency,
              totalOps.get(),
              p50,
              p90,
              p99,
              mbSec,
              mibSec,
              0.0,
              0.0,
              0.0,
              0.0,
              0.0,
              getUsedHeapMb(),
              delta));
    } finally {
      executor.shutdown();
      executor.awaitTermination(60, TimeUnit.SECONDS);
    }
  }

  private static void executeWriteWorkers(
      ExecutorService executor,
      int concurrency,
      final int durationSec,
      final int iterations,
      final long targetSize,
      final String writeApi,
      final byte[] chunk,
      final Queue<BlobId> cleanupQueue,
      final List<Double> latenciesOut,
      final AtomicLong bytesOut,
      final AtomicInteger opsOut)
      throws Exception {
    final long deadlineMs =
        durationSec > 0 ? (System.currentTimeMillis() + durationSec * 1000L) : Long.MAX_VALUE;
    final int slotCount = Math.max(1, Integer.getInteger("gcs.bench.wl6.slots", 2));
    final Set<BlobId> queuedSet = ConcurrentHashMap.newKeySet();
    final BlobAppendableUploadConfig appendConfig =
        BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING);

    List<Future<?>> futures = new ArrayList<Future<?>>(concurrency);
    for (int w = 0; w < concurrency; w++) {
      final int workerId = w;
      futures.add(
          executor.submit(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  Storage client = pickClient(workerId);
                  int count = 0;
                  while ((durationSec > 0 && System.currentTimeMillis() < deadlineMs)
                      || (durationSec == 0 && count < iterations)) {
                    String objName =
                        String.format(
                            "bench-write-w%d-slot%d.bin", workerId, count % slotCount);
                    count++;
                    BlobId slotKey = BlobId.of(TEST_BUCKET, objName);
                    if (queuedSet.add(slotKey)) {
                      cleanupQueue.offer(slotKey);
                    }
                    BlobInfo info = BlobInfo.newBuilder(slotKey).build();

                    long opStart = System.nanoTime();
                    if ("appendable".equals(writeApi)) {
                      BlobAppendableUpload upload =
                          client.blobAppendableUpload(info, appendConfig);
                      try (AppendableUploadWriteableByteChannel ch = upload.open()) {
                        long remaining = targetSize;
                        while (remaining > 0) {
                          int toWrite = (int) Math.min(chunk.length, remaining);
                          ByteBuffer buf = ByteBuffer.wrap(chunk, 0, toWrite);
                          while (buf.hasRemaining()) {
                            int n = ch.write(buf);
                            if (n == 0) {
                              Thread.yield();
                            }
                          }
                          remaining -= toWrite;
                        }
                        ch.finalizeAndClose();
                      }
                      upload.getResult().get(300, TimeUnit.SECONDS);
                    } else {
                      try (WriteChannel writer = client.writer(info)) {
                        long remaining = targetSize;
                        while (remaining > 0) {
                          int toWrite = (int) Math.min(chunk.length, remaining);
                          ByteBuffer buf = ByteBuffer.wrap(chunk, 0, toWrite);
                          while (buf.hasRemaining()) {
                            writer.write(buf);
                          }
                          remaining -= toWrite;
                        }
                      }
                    }
                    long opNs = System.nanoTime() - opStart;
                    if (latenciesOut != null) {
                      latenciesOut.add(opNs / 1_000_000.0);
                      bytesOut.addAndGet(targetSize);
                      opsOut.incrementAndGet();
                    }
                  }
                  return null;
                }
              }));
    }
    for (Future<?> f : futures) {
      f.get();
    }
  }

  private static void cleanupCreatedObjects(
      ExecutorService executor, final Queue<BlobId> cleanupQueue) throws Exception {
    int count = cleanupQueue.size();
    if (count == 0) {
      return;
    }
    log("Deleting " + count + " temporary benchmark objects outside timed window...");
    int workers = Math.min(16, Math.max(1, count));
    List<Future<?>> delFutures = new ArrayList<Future<?>>(workers);
    for (int i = 0; i < workers; i++) {
      final int idx = i;
      delFutures.add(
          executor.submit(
              new Callable<Void>() {
                @Override
                public Void call() {
                  Storage client = pickClient(idx);
                  BlobId id;
                  while ((id = cleanupQueue.poll()) != null) {
                    try {
                      client.delete(BlobId.of(id.getBucket(), id.getName()));
                    } catch (Throwable ignored) {
                    }
                  }
                  return null;
                }
              }));
    }
    for (Future<?> f : delFutures) {
      f.get(120, TimeUnit.SECONDS);
    }
  }

  private static double getUsedHeapMb() {
    Runtime rt = Runtime.getRuntime();
    return (rt.totalMemory() - rt.freeMemory()) / 1_048_576.0;
  }

  private static void printSummaryReport() {
    boolean isInproc = "uds-inproc".equalsIgnoreCase(DRIVER);
    System.out.println("\n" + repeat("#", 132));
    System.out.printf(
        "  GCS SDK CONSOLIDATION: JAVA %s BENCHMARK REPORT (Transport=%s, Live=%b, Bucket=%s)\n",
        DRIVER.toUpperCase(), activeTransport.toUpperCase(), IS_LIVE, TEST_BUCKET);
    System.out.println(repeat("#", 132));
    System.out.printf(
        "| %-32s | %-5s | %-15s | %-21s | %-15s | %-21s | %-10s |\n",
        "Workload",
        "Thrds",
        "Write P50/P99",
        "Write MB/s (MiB/s)",
        "Read P50/P99",
        "Read MB/s (MiB/s)",
        "CPU (s)");
    System.out.println(repeat("-", 132));

    for (BenchmarkMetric m : recordedMetrics) {
      boolean isOpsMetric =
          m.payloadLabel.contains("Open")
              || m.payloadLabel.contains("Random8K")
              || m.payloadLabel.contains("ListObjects")
              || m.payloadLabel.contains("ListBuckets");
      String wLat =
          m.writeP99 > 0 ? String.format("%.1f/%.1f ms", m.writeP50, m.writeP99) : "N/A";
      String wBw =
          m.writeMbSec > 0
              ? String.format("%.1f (%.1f)", m.writeMbSec, m.writeMibSec)
              : "N/A";
      String rLat =
          m.readP99 > 0 ? String.format("%.2f/%.2f ms", m.readP50, m.readP99) : "N/A";
      String rBw =
          m.readMbSec > 0
              ? (isOpsMetric
                  ? String.format("%.1f ops/s", m.readMbSec)
                  : String.format("%.1f (%.1f)", m.readMbSec, m.readMibSec))
              : "N/A";
      double totalProcCpu =
          isInproc
              ? Math.max(m.telemetry.jvmCpuSec, m.telemetry.rustCpuSec)
              : (m.telemetry.jvmCpuSec + m.telemetry.rustCpuSec);

      System.out.printf(
          "| %-32s | %-5d | %-15s | %-21s | %-15s | %-21s | %-10.2f |\n",
          m.payloadLabel, m.workers, wLat, wBw, rLat, rBw, totalProcCpu);
    }
    System.out.println(repeat("#", 132));

    if (finalStats != null) {
      System.out.println("\n" + repeat("-", 72));
      if (isInproc) {
        System.out.println(
            "  UNIFIED PROCESS TELEMETRY (JVM + In-Process Rust Core — Same OS PID)");
      } else {
        System.out.println("  OUT-OF-PROCESS RUST SIDECAR TELEMETRY (Stats RPC)");
      }
      System.out.println(repeat("-", 72));
      System.out.printf("  Process PID               : %d\n", finalStats.getPid());
      System.out.printf("  OS Thread Count           : %d\n", finalStats.getThreads());
      System.out.printf("  CPU User Time             : %.3f sec\n", finalStats.getCpuUserSecs());
      System.out.printf("  CPU System Time           : %.3f sec\n", finalStats.getCpuSysSecs());
      System.out.printf(
          "  Current RSS Memory        : %,d KiB (%.1f MiB)\n",
          finalStats.getRssBytes() / 1024, finalStats.getRssBytes() / 1_048_576.0);
      System.out.printf(
          "  Peak Resident Memory (HWM): %,d KiB (%.1f MiB)\n",
          finalStats.getPeakRssBytes() / 1024, finalStats.getPeakRssBytes() / 1_048_576.0);
      System.out.printf("  Total UDS RPCs Served     : %,d\n", finalStats.getRpcsServed());
      System.out.printf("  Uptime                    : %,d ms\n", finalStats.getUptimeMs());
      System.out.println(repeat("-", 72) + "\n");
    }
  }

  public static final class BenchmarkMetric {
    public final String payloadLabel;
    public final int objectSizeBytes;
    public final int workers;
    public final int iterations;
    public final double writeP50;
    public final double writeP90;
    public final double writeP99;
    public final double writeMbSec;
    public final double writeMibSec;
    public final double readP50;
    public final double readP90;
    public final double readP99;
    public final double readMbSec;
    public final double readMibSec;
    public final double heapUsedMb;
    final WorkloadTelemetryDelta telemetry;

    BenchmarkMetric(
        String payloadLabel,
        int objectSizeBytes,
        int workers,
        int iterations,
        double writeP50,
        double writeP90,
        double writeP99,
        double writeMbSec,
        double writeMibSec,
        double readP50,
        double readP90,
        double readP99,
        double readMbSec,
        double readMibSec,
        double heapUsedMb,
        WorkloadTelemetryDelta telemetry) {
      this.payloadLabel = payloadLabel;
      this.objectSizeBytes = objectSizeBytes;
      this.workers = workers;
      this.iterations = iterations;
      this.writeP50 = writeP50;
      this.writeP90 = writeP90;
      this.writeP99 = writeP99;
      this.writeMbSec = writeMbSec;
      this.writeMibSec = writeMibSec;
      this.readP50 = readP50;
      this.readP90 = readP90;
      this.readP99 = readP99;
      this.readMbSec = readMbSec;
      this.readMibSec = readMibSec;
      this.heapUsedMb = heapUsedMb;
      this.telemetry = telemetry;
    }
  }
}
