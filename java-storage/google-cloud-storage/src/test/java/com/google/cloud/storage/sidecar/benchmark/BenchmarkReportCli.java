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

import com.google.api.client.http.GenericUrl;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestFactory;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.auth.http.HttpCredentialsAdapter;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.sidecar.UdsTransport;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkSuiteRunner.SuiteRunRecord;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkSuiteRunner.WorkloadRecord;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Queries Google Cloud Monitoring API v3 ({@code projects.timeSeries.list}) for steady-state VM CPU
 * utilization and Ops Agent peak memory per workload window, and renders the mandatory Section 6.2
 * Set 1 ({@code [gRPC]}, 8 tables) and Set 2 ({@code [JSON]}, 5 tables) 7-column Markdown tables.
 */
public final class BenchmarkReportCli {

  public static final String CPU_METRIC_TYPE = "compute.googleapis.com/instance/cpu/utilization";
  public static final String MEMORY_METRIC_TYPE = "agent.googleapis.com/memory/percent_used";

  public static final String NA_REGIONAL_BIDI_WRITE = "N/A (Regional rejects BidiWriteObject)";
  public static final String NA_ZONAL_JSON = "N/A (Zonal Rapid is gRPC-only)";
  public static final String PENDING_CELL = "Pending";

  private static final String TABLE_HEADER_ROW =
      "| Metric | Standard Regional (`us-west1`): Native Java "
          + "| Standard Regional (`us-west1`): `uds-sidecar` (`% Diff`) "
          + "| Standard Regional (`us-west1`): `uds-inproc` (`% Diff`) "
          + "| Zonal (`us-west1-a`): Native Java "
          + "| Zonal (`us-west1-a`): `uds-sidecar` (`% Diff`) "
          + "| Zonal (`us-west1-a`): `uds-inproc` (`% Diff`) |";

  private static final String TABLE_ALIGN_ROW =
      "| :--- | :--- | :--- | :--- | :--- | :--- | :--- |";

  private static final long ONE_SECOND_NS = 1_000_000_000L;

  private BenchmarkReportCli() {}

  /** Pluggable Cloud Monitoring window query provider (live REST v3 in production). */
  public interface TelemetryProvider {
    CloudMetrics fetchWindowMetrics(String projectId, String instanceId, Instant start, Instant end)
        throws IOException;
  }

  /** VM-level CPU (%) and Peak Memory (%) for a single workload window. */
  public static final class CloudMetrics {
    public final double cpuPercent;
    public final double peakMemoryPercent;

    public CloudMetrics(double cpuPercent, double peakMemoryPercent) {
      this.cpuPercent = cpuPercent;
      this.peakMemoryPercent = peakMemoryPercent;
    }
  }

  private enum ThroughputStyle {
    DATA_MBPS,
    OP_RATE_ONLY,
    OP_RATE_WITH_MBPS
  }

  private static final class TableSpec {
    final String workloadId;
    final String heading;
    final UdsTransport protocol;
    final ThroughputStyle throughputStyle;
    final String regionalNaReason;
    final String zonalNaReason;

    TableSpec(
        String workloadId,
        String heading,
        UdsTransport protocol,
        ThroughputStyle throughputStyle,
        String regionalNaReason,
        String zonalNaReason) {
      this.workloadId = workloadId;
      this.heading = heading;
      this.protocol = protocol;
      this.throughputStyle = throughputStyle;
      this.regionalNaReason = regionalNaReason;
      this.zonalNaReason = zonalNaReason;
    }
  }

  private static final List<TableSpec> SET_1_GRPC_TABLES =
      Collections.unmodifiableList(
          java.util.Arrays.asList(
              new TableSpec(
                  "WL1",
                  "### WL 1: Open (200 MiB BidiReadObject Session Handshake) — `[gRPC]`",
                  UdsTransport.GRPC,
                  ThroughputStyle.OP_RATE_ONLY,
                  null,
                  null),
              new TableSpec(
                  "WL2",
                  "### WL 2: Read50x2x4 Serial (50 \u00d7 2 MiB Ranges at 4 MiB Stride) — `[gRPC]`",
                  UdsTransport.GRPC,
                  ThroughputStyle.DATA_MBPS,
                  null,
                  null),
              new TableSpec(
                  "WL3",
                  "### WL 3: Read50x2x4 Concurrent (50 \u00d7 2 MiB Ranges on 1 BlobReadSession)"
                      + " — `[gRPC]`",
                  UdsTransport.GRPC,
                  ThroughputStyle.DATA_MBPS,
                  null,
                  null),
              new TableSpec(
                  "WL4",
                  "### WL 4: Random8K (2,000 Random 8 KiB Seeks on 1 Open 100 MiB Session)"
                      + " — `[gRPC]`",
                  UdsTransport.GRPC,
                  ThroughputStyle.OP_RATE_WITH_MBPS,
                  null,
                  null),
              new TableSpec(
                  "WL5",
                  "### WL 5: Scale Read (16 MiB Chunks of 200 MiB \u00d7 48 Threads, 5 Minutes)"
                      + " — `[gRPC]`",
                  UdsTransport.GRPC,
                  ThroughputStyle.DATA_MBPS,
                  null,
                  null),
              new TableSpec(
                  "WL6",
                  "### WL 6: Scale Write (200 MiB Objects via BidiWriteObject \u00d7 48 Threads,"
                      + " 5 Minutes) — `[gRPC]`",
                  UdsTransport.GRPC,
                  ThroughputStyle.DATA_MBPS,
                  NA_REGIONAL_BIDI_WRITE,
                  null),
              new TableSpec(
                  "WL7a",
                  "### WL 7a: ListObjects (100 Objects Under Prefix, Single Page) — `[gRPC]`",
                  UdsTransport.GRPC,
                  ThroughputStyle.OP_RATE_ONLY,
                  null,
                  null),
              new TableSpec(
                  "WL7b",
                  "### WL 7b: ListBuckets (Project Buckets, pageSize=100, Single Page) — `[gRPC]`",
                  UdsTransport.GRPC,
                  ThroughputStyle.OP_RATE_ONLY,
                  null,
                  null)));

  private static final List<TableSpec> SET_2_JSON_TABLES =
      Collections.unmodifiableList(
          java.util.Arrays.asList(
              new TableSpec(
                  "WL2",
                  "### WL 2: Read50x2x4 Serial (50 \u00d7 2 MiB Ranges at 4 MiB Stride via"
                      + " ReadChannel) — `[JSON]`",
                  UdsTransport.JSON,
                  ThroughputStyle.DATA_MBPS,
                  null,
                  NA_ZONAL_JSON),
              new TableSpec(
                  "WL3",
                  "### WL 3: Read50x2x4 Concurrent (50 \u00d7 2 MiB Ranges Across 16-Thread"
                      + " ReadChannel Pool) — `[JSON]`",
                  UdsTransport.JSON,
                  ThroughputStyle.DATA_MBPS,
                  null,
                  NA_ZONAL_JSON),
              new TableSpec(
                  "WL4",
                  "### WL 4: Random8K (2,000 Random 8 KiB Seeks via ReadChannel) — `[JSON]`",
                  UdsTransport.JSON,
                  ThroughputStyle.OP_RATE_WITH_MBPS,
                  null,
                  NA_ZONAL_JSON),
              new TableSpec(
                  "WL5",
                  "### WL 5: Scale Read (16 MiB Chunks of 200 MiB \u00d7 48 Threads, 5 Minutes via"
                      + " ReadChannel) — `[JSON]`",
                  UdsTransport.JSON,
                  ThroughputStyle.DATA_MBPS,
                  null,
                  NA_ZONAL_JSON),
              new TableSpec(
                  "WL6",
                  "### WL 6: Scale Write (200 MiB Objects via WriteChannel \u00d7 48 Threads,"
                      + " 5 Minutes) — `[JSON]`",
                  UdsTransport.JSON,
                  ThroughputStyle.DATA_MBPS,
                  null,
                  NA_ZONAL_JSON)));

  /** CLI entry point for generating the complete dual-protocol Markdown benchmark report. */
  public static void main(String[] args) throws Exception {
    String resultsDir = null;
    List<Path> jsonFiles = new ArrayList<Path>();
    String projectId = BenchmarkConfig.DEFAULT_PROJECT_ID;
    String instanceId = null;
    String outputMd = null;
    long waitFlushSec = 0L;

    for (String arg : args) {
      if (!arg.startsWith("--")) {
        continue;
      }
      int eq = arg.indexOf('=');
      if (eq < 0) {
        throw new IllegalArgumentException("Expected --key=value argument, got: " + arg);
      }
      String key = arg.substring(2, eq).trim().toLowerCase(Locale.US);
      String val = arg.substring(eq + 1).trim();
      switch (key) {
        case "results-dir":
          resultsDir = val;
          break;
        case "suite-json":
          for (String part : val.split(",")) {
            if (!part.trim().isEmpty()) {
              jsonFiles.add(Paths.get(part.trim()));
            }
          }
          break;
        case "project":
        case "project-id":
          projectId = val;
          break;
        case "instance-id":
          instanceId = val;
          break;
        case "output-md":
          outputMd = val;
          break;
        case "wait-flush-sec":
          waitFlushSec = Long.parseLong(val);
          break;
        default:
          throw new IllegalArgumentException("Unknown CLI flag: --" + key);
      }
    }

    if (resultsDir != null && !resultsDir.isEmpty()) {
      try (DirectoryStream<Path> ds = Files.newDirectoryStream(Paths.get(resultsDir), "*.json")) {
        for (Path p : ds) {
          jsonFiles.add(p);
        }
      }
    }
    if (jsonFiles.isEmpty()) {
      throw new IllegalArgumentException("Provide --results-dir=<dir> or --suite-json=<files>");
    }

    if (waitFlushSec > 0L) {
      System.out.printf(
          Locale.US,
          "[REPORT] Waiting %ds for Cloud Monitoring 60s metric points to flush...%n",
          waitFlushSec);
      TimeUnit.SECONDS.sleep(waitFlushSec);
    }

    if (instanceId == null || instanceId.isEmpty()) {
      instanceId = fetchGceMetadataInstanceId();
    }

    List<SuiteRunRecord> suites = new ArrayList<SuiteRunRecord>(jsonFiles.size());
    for (Path p : jsonFiles) {
      suites.add(BenchmarkSuiteRunner.readSuiteJson(p));
    }

    TelemetryProvider liveProvider = LiveCloudMonitoringProvider.createDefault();
    String markdown = renderFullReport(suites, projectId, instanceId, liveProvider);
    System.out.println(markdown);
    if (outputMd != null && !outputMd.isEmpty()) {
      Path target = Paths.get(outputMd);
      if (target.getParent() != null) {
        Files.createDirectories(target.getParent());
      }
      Files.write(target, markdown.getBytes(StandardCharsets.UTF_8));
    }
  }

  /** Renders Set 1 ({@code [gRPC]}, 8 tables) and Set 2 ({@code [JSON]}, 5 tables). */
  public static String renderFullReport(
      List<SuiteRunRecord> suites,
      String projectId,
      String instanceId,
      TelemetryProvider telemetry)
      throws IOException {
    requireNonNull(suites, "suites");
    requireNonNull(telemetry, "telemetry");

    Map<String, EnrichedCell> cellIndex = new HashMap<String, EnrichedCell>();
    for (int i = 0; i < suites.size(); i++) {
      SuiteRunRecord s = suites.get(i);
      String effectiveProject =
          (s.projectId != null && !s.projectId.isEmpty()) ? s.projectId : projectId;
      for (WorkloadRecord wl : s.workloads.values()) {
        Instant start = Instant.parse(wl.wlStartUtc);
        Instant end = Instant.parse(wl.wlEndUtc);
        CloudMetrics cm =
            telemetry.fetchWindowMetrics(effectiveProject, instanceId, start, end);
        String key = cellKey(s.protocol, s.bucketType, s.driver, wl.workloadId);
        cellIndex.put(key, new EnrichedCell(wl, cm));
      }
    }

    StringBuilder sb = new StringBuilder(16384);
    sb.append("## Set 1 — `gRPC` Protocol Tables (`[gRPC]`)\n\n");
    for (int i = 0; i < SET_1_GRPC_TABLES.size(); i++) {
      renderSingleTable(sb, SET_1_GRPC_TABLES.get(i), cellIndex);
      sb.append('\n');
    }

    sb.append("## Set 2 — `JSON` Protocol Tables (`[JSON]`)\n\n");
    for (int i = 0; i < SET_2_JSON_TABLES.size(); i++) {
      renderSingleTable(sb, SET_2_JSON_TABLES.get(i), cellIndex);
      if (i + 1 < SET_2_JSON_TABLES.size()) {
        sb.append('\n');
      }
    }
    return sb.toString();
  }

  private static void renderSingleTable(
      StringBuilder sb, TableSpec spec, Map<String, EnrichedCell> cells) {
    String proto = spec.protocol.name();
    EnrichedCell regNative = cells.get(cellKey(proto, "regional", "native", spec.workloadId));
    EnrichedCell regSidecar =
        cells.get(cellKey(proto, "regional", "uds-sidecar", spec.workloadId));
    EnrichedCell regInproc = cells.get(cellKey(proto, "regional", "uds-inproc", spec.workloadId));

    EnrichedCell zonNative = cells.get(cellKey(proto, "zonal", "native", spec.workloadId));
    EnrichedCell zonSidecar = cells.get(cellKey(proto, "zonal", "uds-sidecar", spec.workloadId));
    EnrichedCell zonInproc = cells.get(cellKey(proto, "zonal", "uds-inproc", spec.workloadId));

    boolean useSeconds =
        shouldUseSecondsUnit(regNative, regSidecar, regInproc, zonNative, zonSidecar, zonInproc);

    sb.append(spec.heading).append("\n\n");
    sb.append(TABLE_HEADER_ROW).append('\n');
    sb.append(TABLE_ALIGN_ROW).append('\n');

    // Row 1: Latency (P50 / P90 / P99)
    sb.append("| **Latency (P50 / P90 / P99)** | ")
        .append(formatNativeLatency(regNative, spec.regionalNaReason, useSeconds))
        .append(" | ")
        .append(formatExpLatency(regSidecar, regNative, spec.regionalNaReason, useSeconds))
        .append(" | ")
        .append(formatExpLatency(regInproc, regNative, spec.regionalNaReason, useSeconds))
        .append(" | ")
        .append(formatNativeLatency(zonNative, spec.zonalNaReason, useSeconds))
        .append(" | ")
        .append(formatExpLatency(zonSidecar, zonNative, spec.zonalNaReason, useSeconds))
        .append(" | ")
        .append(formatExpLatency(zonInproc, zonNative, spec.zonalNaReason, useSeconds))
        .append(" |\n");

    // Row 2: Throughput
    sb.append("| **Throughput** | ")
        .append(formatNativeThroughput(regNative, spec.regionalNaReason, spec.throughputStyle))
        .append(" | ")
        .append(
            formatExpThroughput(
                regSidecar, regNative, spec.regionalNaReason, spec.throughputStyle))
        .append(" | ")
        .append(
            formatExpThroughput(regInproc, regNative, spec.regionalNaReason, spec.throughputStyle))
        .append(" | ")
        .append(formatNativeThroughput(zonNative, spec.zonalNaReason, spec.throughputStyle))
        .append(" | ")
        .append(
            formatExpThroughput(zonSidecar, zonNative, spec.zonalNaReason, spec.throughputStyle))
        .append(" | ")
        .append(formatExpThroughput(zonInproc, zonNative, spec.zonalNaReason, spec.throughputStyle))
        .append(" |\n");

    // Row 3: CPU Utilization
    sb.append("| **CPU Utilization** | ")
        .append(formatNativePercent(regNative, spec.regionalNaReason, true))
        .append(" | ")
        .append(formatExpPercent(regSidecar, regNative, spec.regionalNaReason, true))
        .append(" | ")
        .append(formatExpPercent(regInproc, regNative, spec.regionalNaReason, true))
        .append(" | ")
        .append(formatNativePercent(zonNative, spec.zonalNaReason, true))
        .append(" | ")
        .append(formatExpPercent(zonSidecar, zonNative, spec.zonalNaReason, true))
        .append(" | ")
        .append(formatExpPercent(zonInproc, zonNative, spec.zonalNaReason, true))
        .append(" |\n");

    // Row 4: Peak Memory
    sb.append("| **Peak Memory** | ")
        .append(formatNativePercent(regNative, spec.regionalNaReason, false))
        .append(" | ")
        .append(formatExpPercent(regSidecar, regNative, spec.regionalNaReason, false))
        .append(" | ")
        .append(formatExpPercent(regInproc, regNative, spec.regionalNaReason, false))
        .append(" | ")
        .append(formatNativePercent(zonNative, spec.zonalNaReason, false))
        .append(" | ")
        .append(formatExpPercent(zonSidecar, zonNative, spec.zonalNaReason, false))
        .append(" | ")
        .append(formatExpPercent(zonInproc, zonNative, spec.zonalNaReason, false))
        .append(" |\n");
  }

  private static boolean shouldUseSecondsUnit(EnrichedCell... cells) {
    for (EnrichedCell c : cells) {
      if (c != null && c.record.p50Ns >= ONE_SECOND_NS) {
        return true;
      }
    }
    return false;
  }

  private static String formatNativeLatency(
      EnrichedCell cell, String naReason, boolean useSeconds) {
    if (naReason != null) {
      return "`" + naReason + "`";
    }
    if (cell == null) {
      return PENDING_CELL;
    }
    return "`" + formatLatencyTriple(cell.record, useSeconds) + "`";
  }

  private static String formatExpLatency(
      EnrichedCell exp, EnrichedCell base, String naReason, boolean useSeconds) {
    if (naReason != null) {
      return "`" + naReason + "`";
    }
    if (exp == null) {
      return PENDING_CELL;
    }
    String triple = "`" + formatLatencyTriple(exp.record, useSeconds) + "`";
    if (base == null) {
      return triple;
    }
    String d50 = formatPctDiff(exp.record.p50Ns, base.record.p50Ns);
    String d90 = formatPctDiff(exp.record.p90Ns, base.record.p90Ns);
    String d99 = formatPctDiff(exp.record.p99Ns, base.record.p99Ns);
    return triple + " (`" + d50 + " / " + d90 + " / " + d99 + "`)";
  }

  private static String formatLatencyTriple(WorkloadRecord r, boolean useSeconds) {
    double div = useSeconds ? 1_000_000_000.0 : 1_000_000.0;
    String unit = useSeconds ? "s" : "ms";
    return String.format(
        Locale.US, "%.2f / %.2f / %.2f %s", r.p50Ns / div, r.p90Ns / div, r.p99Ns / div, unit);
  }

  private static String formatNativeThroughput(
      EnrichedCell cell, String naReason, ThroughputStyle style) {
    if (naReason != null) {
      return "`" + naReason + "`";
    }
    if (cell == null) {
      return PENDING_CELL;
    }
    return "`" + formatThroughputValue(cell.record, style) + "`";
  }

  private static String formatExpThroughput(
      EnrichedCell exp, EnrichedCell base, String naReason, ThroughputStyle style) {
    if (naReason != null) {
      return "`" + naReason + "`";
    }
    if (exp == null) {
      return PENDING_CELL;
    }
    String val = "`" + formatThroughputValue(exp.record, style) + "`";
    if (base == null) {
      return val;
    }
    double expPrimary =
        (style == ThroughputStyle.DATA_MBPS) ? exp.record.decimalMbPerSec : exp.record.opsPerSec;
    double basePrimary =
        (style == ThroughputStyle.DATA_MBPS) ? base.record.decimalMbPerSec : base.record.opsPerSec;
    return val + " (`" + formatPctDiff(expPrimary, basePrimary) + "`)";
  }

  private static String formatThroughputValue(WorkloadRecord r, ThroughputStyle style) {
    switch (style) {
      case DATA_MBPS:
        return String.format(
            Locale.US, "%,.2f MB/s (%,.2f MiB/s)", r.decimalMbPerSec, r.binaryMibPerSec);
      case OP_RATE_WITH_MBPS:
        return String.format(
            Locale.US, "%,.1f ops/s (%.2f MB/s)", r.opsPerSec, r.decimalMbPerSec);
      case OP_RATE_ONLY:
      default:
        return String.format(Locale.US, "%,.1f ops/s", r.opsPerSec);
    }
  }

  private static String formatNativePercent(EnrichedCell cell, String naReason, boolean isCpu) {
    if (naReason != null) {
      return "`" + naReason + "`";
    }
    if (cell == null) {
      return PENDING_CELL;
    }
    double v = isCpu ? cell.metrics.cpuPercent : cell.metrics.peakMemoryPercent;
    if (Double.isNaN(v)) {
      return "`N/A`";
    }
    return String.format(Locale.US, "`%.2f%%`", v);
  }

  private static String formatExpPercent(
      EnrichedCell exp, EnrichedCell base, String naReason, boolean isCpu) {
    if (naReason != null) {
      return "`" + naReason + "`";
    }
    if (exp == null) {
      return PENDING_CELL;
    }
    double expVal = isCpu ? exp.metrics.cpuPercent : exp.metrics.peakMemoryPercent;
    if (Double.isNaN(expVal)) {
      return "`N/A`";
    }
    String formatted = String.format(Locale.US, "`%.2f%%`", expVal);
    if (base == null) {
      return formatted;
    }
    double baseVal = isCpu ? base.metrics.cpuPercent : base.metrics.peakMemoryPercent;
    if (Double.isNaN(baseVal) || baseVal == 0.0) {
      return formatted;
    }
    return formatted + " (`" + formatPctDiff(expVal, baseVal) + "`)";
  }

  /**
   * Formats {@code ((experimental - nativeVal) / nativeVal) * 100.0} rounded to 1 decimal place
   * with mandatory {@code +} / {@code -} sign ({@code 0.0%} when rounded delta is zero).
   */
  public static String formatPctDiff(double experimental, double nativeVal) {
    if (nativeVal == 0.0) {
      return "0.0%";
    }
    double rawPct = ((experimental - nativeVal) / nativeVal) * 100.0;
    long tenths = Math.round(rawPct * 10.0);
    if (tenths == 0L) {
      return "0.0%";
    }
    double rounded = tenths / 10.0;
    return String.format(Locale.US, "%+.1f%%", rounded);
  }

  private static String cellKey(String protocol, String bucketType, String driver, String wlId) {
    return protocol.toUpperCase(Locale.US)
        + "|"
        + bucketType.toLowerCase(Locale.US)
        + "|"
        + driver.toLowerCase(Locale.US)
        + "|"
        + wlId.toUpperCase(Locale.US);
  }

  private static String fetchGceMetadataInstanceId() throws IOException {
    HttpRequestFactory rf = new NetHttpTransport().createRequestFactory();
    HttpRequest req =
        rf.buildGetRequest(
            new GenericUrl("http://metadata.google.internal/computeMetadata/v1/instance/id"));
    req.getHeaders().set("Metadata-Flavor", "Google");
    HttpResponse resp = req.execute();
    try {
      return resp.parseAsString().trim();
    } finally {
      resp.disconnect();
    }
  }

  private static final class EnrichedCell {
    final WorkloadRecord record;
    final CloudMetrics metrics;

    EnrichedCell(WorkloadRecord record, CloudMetrics metrics) {
      this.record = record;
      this.metrics = metrics;
    }
  }

  /**
   * Live Google Cloud Monitoring REST API v3 client implementing exact Section 6.1 window rules:
   *
   * <ul>
   *   <li><b>CPU Utilization:</b> {@code compute.googleapis.com/instance/cpu/utilization}, interior
   *       window {@code (wlStart + 60s, wlEnd]}, arithmetic {@code MEAN × 100.0}.
   *   <li><b>Peak Memory:</b> {@code agent.googleapis.com/memory/percent_used} ({@code
   *       state="used"}), window {@code [wlStart, wlEnd + 60s]}, {@code MAX}.
   * </ul>
   */
  public static final class LiveCloudMonitoringProvider implements TelemetryProvider {
    private static final String CLOUD_PLATFORM_SCOPE =
        "https://www.googleapis.com/auth/cloud-platform";

    private final HttpRequestFactory requestFactory;

    public LiveCloudMonitoringProvider(HttpRequestFactory requestFactory) {
      this.requestFactory = requireNonNull(requestFactory, "requestFactory");
    }

    public static LiveCloudMonitoringProvider createDefault() throws IOException {
      GoogleCredentials creds = GoogleCredentials.getApplicationDefault();
      if (creds.createScopedRequired()) {
        creds = creds.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
      }
      HttpRequestFactory rf =
          new NetHttpTransport().createRequestFactory(new HttpCredentialsAdapter(creds));
      return new LiveCloudMonitoringProvider(rf);
    }

    @Override
    public CloudMetrics fetchWindowMetrics(
        String projectId, String instanceId, Instant wlStart, Instant wlEnd) throws IOException {
      double cpuPct = queryCpuMeanPercent(projectId, instanceId, wlStart, wlEnd);
      double memPct = queryPeakMemoryPercent(projectId, instanceId, wlStart, wlEnd);
      return new CloudMetrics(cpuPct, memPct);
    }

    private double queryCpuMeanPercent(
        String projectId, String instanceId, Instant wlStart, Instant wlEnd) throws IOException {
      Instant interiorStartExclusive = wlStart.plusSeconds(60);
      if (!interiorStartExclusive.isBefore(wlEnd)) {
        return Double.NaN;
      }
      String filter =
          "metric.type = \""
              + CPU_METRIC_TYPE
              + "\" AND resource.labels.instance_id = \""
              + instanceId
              + "\"";
      List<TimedPoint> points =
          listTimeSeriesPoints(projectId, filter, interiorStartExclusive, wlEnd);
      double sum = 0.0;
      int n = 0;
      for (int i = 0; i < points.size(); i++) {
        TimedPoint pt = points.get(i);
        if (pt.endTime.isAfter(interiorStartExclusive) && !pt.endTime.isAfter(wlEnd)) {
          sum += pt.value * 100.0;
          n++;
        }
      }
      return n > 0 ? (sum / n) : Double.NaN;
    }

    private double queryPeakMemoryPercent(
        String projectId, String instanceId, Instant wlStart, Instant wlEnd) throws IOException {
      Instant closedStartQuery = wlStart.minusMillis(1);
      Instant closedEnd = wlEnd.plusSeconds(60);
      String filter =
          "metric.type = \""
              + MEMORY_METRIC_TYPE
              + "\" AND metric.labels.state = \"used\""
              + " AND resource.labels.instance_id = \""
              + instanceId
              + "\"";
      List<TimedPoint> points =
          listTimeSeriesPoints(projectId, filter, closedStartQuery, closedEnd);
      double max = Double.NaN;
      for (int i = 0; i < points.size(); i++) {
        TimedPoint pt = points.get(i);
        if (!pt.endTime.isBefore(wlStart) && !pt.endTime.isAfter(closedEnd)) {
          if (Double.isNaN(max) || pt.value > max) {
            max = pt.value;
          }
        }
      }
      return max;
    }

    private List<TimedPoint> listTimeSeriesPoints(
        String projectId, String filter, Instant startTime, Instant endTime) throws IOException {
      List<TimedPoint> results = new ArrayList<TimedPoint>();
      String pageToken = null;
      do {
        StringBuilder url = new StringBuilder(256);
        url.append("https://monitoring.googleapis.com/v3/projects/")
            .append(URLEncoder.encode(projectId, "UTF-8"))
            .append("/timeSeries?filter=")
            .append(URLEncoder.encode(filter, "UTF-8"))
            .append("&interval.startTime=")
            .append(URLEncoder.encode(startTime.toString(), "UTF-8"))
            .append("&interval.endTime=")
            .append(URLEncoder.encode(endTime.toString(), "UTF-8"));
        if (pageToken != null && !pageToken.isEmpty()) {
          url.append("&pageToken=").append(URLEncoder.encode(pageToken, "UTF-8"));
        }

        HttpRequest req = requestFactory.buildGetRequest(new GenericUrl(url.toString()));
        HttpResponse resp = req.execute();
        String body;
        try {
          body = resp.parseAsString();
        } finally {
          resp.disconnect();
        }

        pageToken = parseTimeSeriesPage(body, results);
      } while (pageToken != null && !pageToken.isEmpty());
      return results;
    }

    /** Parses a {@code projects.timeSeries.list} JSON response page into {@code sink}. */
    static String parseTimeSeriesPage(String jsonBody, List<TimedPoint> sink) {
      JsonObject root = JsonParser.parseString(jsonBody).getAsJsonObject();
      JsonArray seriesArr = root.getAsJsonArray("timeSeries");
      if (seriesArr != null) {
        for (int s = 0; s < seriesArr.size(); s++) {
          JsonObject series = seriesArr.get(s).getAsJsonObject();
          JsonArray points = series.getAsJsonArray("points");
          if (points == null) {
            continue;
          }
          for (int p = 0; p < points.size(); p++) {
            JsonObject pt = points.get(p).getAsJsonObject();
            JsonObject interval = pt.getAsJsonObject("interval");
            JsonObject valObj = pt.getAsJsonObject("value");
            if (interval == null || valObj == null || !interval.has("endTime")) {
              continue;
            }
            Instant ptEnd = Instant.parse(interval.get("endTime").getAsString());
            double v;
            if (valObj.has("doubleValue")) {
              v = valObj.get("doubleValue").getAsDouble();
            } else if (valObj.has("int64Value")) {
              v = Double.parseDouble(valObj.get("int64Value").getAsString());
            } else {
              continue;
            }
            sink.add(new TimedPoint(ptEnd, v));
          }
        }
      }
      JsonElement nextTok = root.get("nextPageToken");
      return (nextTok != null && !nextTok.isJsonNull()) ? nextTok.getAsString() : null;
    }
  }

  static final class TimedPoint {
    final Instant endTime;
    final double value;

    TimedPoint(Instant endTime, double value) {
      this.endTime = endTime;
      this.value = value;
    }
  }
}
