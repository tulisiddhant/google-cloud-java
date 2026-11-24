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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Isolated single-suite benchmark process runner.
 *
 * <p>Executes one {@code (driver × protocol × bucketType)} suite inside a dedicated fresh JVM
 * process, enforcing a mandatory {@code 75}-second zero-I/O quiescence gap between consecutive
 * workloads so Google Cloud Monitoring {@code 60s} CPU and Ops Agent memory samples never bleed
 * across workload boundaries.
 */
public final class BenchmarkSuiteRunner {

  private static final Gson GSON = new GsonBuilder().setPrettyPrinting().create();
  private static final Gson COMPACT_GSON = new Gson();

  private BenchmarkSuiteRunner() {}

  /** Canonical ordered workload suite covering {@code WL1} through {@code WL7b}. */
  public static List<Workload> defaultWorkloads() {
    return Collections.unmodifiableList(
        Arrays.<Workload>asList(
            new Wl1OpenWorkload(),
            new Wl2ReadSerialWorkload(),
            new Wl3ReadConcurrentWorkload(),
            new Wl4Random8kWorkload(),
            new Wl5ScaleReadWorkload(),
            new Wl6ScaleWriteWorkload(),
            new Wl7aListObjectsWorkload(),
            new Wl7bListBucketsWorkload()));
  }

  /** Selects workloads matching both Section 3 applicability and optional CLI filter. */
  public static List<Workload> selectApplicableWorkloads(
      List<Workload> candidates, BenchmarkConfig config) {
    requireNonNull(candidates, "candidates");
    requireNonNull(config, "config");
    List<Workload> selected = new ArrayList<Workload>(candidates.size());
    for (int i = 0; i < candidates.size(); i++) {
      Workload wl = candidates.get(i);
      if (config.shouldRunWorkload(wl.id())
          && wl.isApplicable(config.protocol(), config.bucketType())) {
        selected.add(wl);
      }
    }
    return selected;
  }

  /** CLI entry point invoked per isolated JVM suite run. */
  public static void main(String[] args) throws Exception {
    BenchmarkConfig config = BenchmarkConfig.fromArgs(args);
    System.out.printf(
        Locale.US,
        "[SUITE] Starting driver=%s protocol=%s bucketType=%s bucket=%s "
            + "nativePool=%d grpcSubchannels=%d udsChannels=%d java=%s%n",
        config.driver().cliName(),
        config.protocol().name(),
        config.bucketType().cliName(),
        config.bucketName(),
        config.nativePoolSize(),
        config.grpcSubchannels(),
        config.udsChannelCount(),
        System.getProperty("java.version"));

    try (ClientProvider clients = ClientProvider.create(config)) {
      SuiteRunRecord record = executeSuite(config, clients, defaultWorkloads());
      if (config.outputJsonPath() != null && !config.outputJsonPath().isEmpty()) {
        writeSuiteJson(Paths.get(config.outputJsonPath()), record);
        System.out.println("[SUITE] Wrote suite JSON to " + config.outputJsonPath());
      }
    }
  }

  /** Executes the selected workloads using {@code clients} with inter-workload quiescence gaps. */
  public static SuiteRunRecord executeSuite(
      BenchmarkConfig config, ClientProvider clients, List<Workload> candidateWorkloads)
      throws Exception {
    requireNonNull(config, "config");
    requireNonNull(clients, "clients");
    List<Workload> selected = selectApplicableWorkloads(candidateWorkloads, config);

    SuiteRunRecord suiteRecord = new SuiteRunRecord();
    suiteRecord.driver = config.driver().cliName();
    suiteRecord.protocol = config.protocol().name();
    suiteRecord.bucketType = config.bucketType().cliName();
    suiteRecord.projectId = config.projectId();
    suiteRecord.bucketName = config.bucketName();
    suiteRecord.nativePoolSize = config.nativePoolSize();
    suiteRecord.grpcSubchannels = config.grpcSubchannels();
    suiteRecord.udsChannelCount = config.udsChannelCount();
    suiteRecord.javaVersion = System.getProperty("java.version", "unknown");
    suiteRecord.jvmName = System.getProperty("java.vm.name", "unknown");
    suiteRecord.suiteStartUtc = Instant.now().toString();
    suiteRecord.workloads = new LinkedHashMap<String, WorkloadRecord>();

    for (int i = 0; i < selected.size(); i++) {
      if (i > 0 && config.quiescenceSec() > 0) {
        System.out.printf(
            Locale.US,
            "[QUIESCENCE] Sleeping %ds with zero GCS I/O before %s...%n",
            config.quiescenceSec(),
            selected.get(i).id());
        TimeUnit.SECONDS.sleep(config.quiescenceSec());
      }

      Workload wl = selected.get(i);
      System.out.printf(Locale.US, "[%s] Starting %s...%n", wl.id(), wl.displayName());
      WorkloadResult res = wl.execute(clients, config);
      WorkloadRecord rec = WorkloadRecord.fromResult(res);
      suiteRecord.workloads.put(wl.id(), rec);

      System.out.println("TIMELINE_JSON: " + COMPACT_GSON.toJson(rec));
      System.out.printf(
          Locale.US,
          "[%s] Done: ops=%d activeSec=%.2fs P50=%.2fms P90=%.2fms P99=%.2fms "
              + "MB/s=%.2f ops/s=%.2f tcp443=%d rxGbps=%.2f txGbps=%.2f%n",
          res.workloadId(),
          res.completedOps(),
          res.activeSec(),
          res.p50Ms(),
          res.p90Ms(),
          res.p99Ms(),
          res.decimalMbPerSec(),
          res.opsPerSec(),
          rec.establishedTcp443,
          rec.nicRxGbps,
          rec.nicTxGbps);
    }

    suiteRecord.suiteEndUtc = Instant.now().toString();
    return suiteRecord;
  }

  public static void writeSuiteJson(Path target, SuiteRunRecord record) throws IOException {
    if (target.getParent() != null) {
      Files.createDirectories(target.getParent());
    }
    byte[] bytes = GSON.toJson(record).getBytes(StandardCharsets.UTF_8);
    Files.write(target, bytes);
  }

  public static SuiteRunRecord readSuiteJson(Path source) throws IOException {
    String json = new String(Files.readAllBytes(source), StandardCharsets.UTF_8);
    return GSON.fromJson(json, SuiteRunRecord.class);
  }

  /** JSON-serializable suite summary written to {@code --output-json}. */
  public static final class SuiteRunRecord {
    public String driver;
    public String protocol;
    public String bucketType;
    public String projectId;
    public String bucketName;
    public int nativePoolSize;
    public int grpcSubchannels;
    public int udsChannelCount;
    public String javaVersion;
    public String jvmName;
    public String suiteStartUtc;
    public String suiteEndUtc;
    public Map<String, WorkloadRecord> workloads = new LinkedHashMap<String, WorkloadRecord>();
  }

  /** JSON-serializable per-workload timing, throughput, UTC boundaries, and diagnostics. */
  public static final class WorkloadRecord {
    public String workloadId;
    public String displayName;
    public String wlStartUtc;
    public String wlEndUtc;
    public long completedOps;
    public long totalBytes;
    public double activeSec;
    public long p50Ns;
    public long p90Ns;
    public long p99Ns;
    public double opsPerSec;
    public double decimalMbPerSec;
    public double binaryMibPerSec;

    public double jvmCpuCoresUsed;
    public double rustCpuCoresUsed;
    public long jvmPeakRssBytes;
    public long rustPeakRssBytes;
    public int establishedTcp443;
    public double nicRxGbps;
    public double nicTxGbps;

    public static WorkloadRecord fromResult(WorkloadResult r) {
      WorkloadRecord rec = new WorkloadRecord();
      rec.workloadId = r.workloadId();
      rec.displayName = r.displayName();
      rec.wlStartUtc = r.wlStartUtc().toString();
      rec.wlEndUtc = r.wlEndUtc().toString();
      rec.completedOps = r.completedOps();
      rec.totalBytes = r.totalBytes();
      rec.activeSec = r.activeSec();
      rec.p50Ns = r.p50Ns();
      rec.p90Ns = r.p90Ns();
      rec.p99Ns = r.p99Ns();
      rec.opsPerSec = r.opsPerSec();
      rec.decimalMbPerSec = r.decimalMbPerSec();
      rec.binaryMibPerSec = r.binaryMibPerSec();

      WorkloadResult.DiagnosticDelta d = r.diagnostics();
      if (d != null) {
        rec.jvmCpuCoresUsed = d.jvmCpuCoresUsed;
        rec.rustCpuCoresUsed = d.rustCpuCoresUsed;
        rec.jvmPeakRssBytes = d.jvmPeakRssBytes;
        rec.rustPeakRssBytes = d.rustPeakRssBytes;
        rec.establishedTcp443 = d.establishedTcp443;
        rec.nicRxGbps = d.nicRxGbps;
        rec.nicTxGbps = d.nicTxGbps;
      }
      return rec;
    }
  }
}
