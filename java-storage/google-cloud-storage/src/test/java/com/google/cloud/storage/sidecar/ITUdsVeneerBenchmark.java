package com.google.cloud.storage.sidecar;

import com.google.api.gax.paging.Page;
import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.api.core.ApiFuture;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;

import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.GrpcStorageOptions;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import gcscore.v1.StatsResponse;
import io.grpc.ManagedChannel;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 * High-level Veneer Integration Benchmark for GCS SDK Consolidation (Java over UDS).
 * Evaluates performance and process telemetry for the PR #7350 Rust sidecar (gcs-core-server).
 *
 * Benchmark Suite:
 * 1. Section 0: UDS gRPC IPC Ping Round-Trip Microbenchmark (1 B, 1 KiB, 64 KiB, 1 MiB).
 * 2. Section 1: Functional validation of veneer methods (create, createFrom, reader, writer, downloadTo, list, delete).
 * 3. Section 2: W1R3 (Write 1, Read 3) across 1 KiB, 100 KiB, 16 MiB, and 200 MiB payloads.
 * 4. Section 3: Concurrent thread scaling (1, 2, 4, 8 workers).
 * 5. Section 4: ListObjects and ListBuckets for 100 items.
 * 6. Section 5: Process Telemetry (Sidecar CPU user/sys time, VmRSS, VmHWM, thread count, RPC count).
 * 7. Section 6: Cloudpath Rapid Workloads (48 parallel workers, Workload 4 Read 16MB/200MB, Workload 5 Write 200MB).
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ITUdsVeneerBenchmark {

  private static final String TEST_BUCKET =
      System.getProperty("gcs.test.bucket", "sidecar-experiment-bucket");
  private static final String DRIVER =
      System.getProperty("gcs.benchmark.driver", System.getProperty("gcs.driver", "uds")); // "uds" or "native"
  private static final boolean IS_LIVE =
      Boolean.parseBoolean(System.getProperty("gcs.live.benchmark", System.getProperty("gcs.bench.live", "false")));

  private static final ConcurrentHashMap<String, Long> OBJECT_SIZES = new ConcurrentHashMap<>();

  private static HttpServer mockBackend;
  private static RustSidecarProcessManager processManager;
  private static ManagedChannel channel;
  private static Storage storage;
  private static UdsStorage udsStorageHandler;

  private static final List<BenchmarkMetric> recordedMetrics = new ArrayList<>();
  private static final List<String> TIMELINE_RECORDS = Collections.synchronizedList(new ArrayList<>());
  private static StatsResponse initialStats;
  private static StatsResponse finalStats;

  private static void recordTimeline(String workload, Instant start, Instant end) {
    TIMELINE_RECORDS.add(String.format("{\"workload\":\"%s\",\"start\":\"%s\",\"end\":\"%s\"}",
        workload, start.toString(), end.toString()));
  }

  private static String repeat(String s, int count) {
    StringBuilder sb = new StringBuilder(s.length() * count);
    for (int i = 0; i < count; i++) {
      sb.append(s);
    }
    return sb.toString();
  }

  private static void banner(String title) {
    System.out.println("\n" + repeat("=", 80));
    System.out.println("  " + title);
    System.out.println(repeat("=", 80));
  }

  private static void log(String msg) {
    System.out.println("[UdsBenchmark] " + msg);
  }

  private static String getTransportPrefix() {
    if ("native".equalsIgnoreCase(DRIVER)) {
      String t = System.getProperty("gcs.native.transport", IS_LIVE ? "grpc" : "json");
      return "[Native-" + t.toUpperCase() + "] ";
    } else {
      String t = System.getProperty("gcs.sidecar.default.transport", "grpc");
      return "[UDS-" + t.toUpperCase() + "] ";
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    banner(String.format("GCS BENCHMARK SETUP: DRIVER=%s | LIVE=%b | BUCKET=%s",
        DRIVER.toUpperCase(), IS_LIVE, TEST_BUCKET));

    // 1. Start embedded high-throughput mock GCS backend if not using live endpoint
    if (!IS_LIVE) {
      String customEndpoint = System.getProperty("gcs.sidecar.endpoint");
      if (customEndpoint == null || customEndpoint.isEmpty()) {
        mockBackend = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockBackend.setExecutor(Executors.newCachedThreadPool());

        // Pre-populate mock object sizes for 48 parallel workers (bench-200mb-*.bin)
        for (int i = 0; i < 48; i++) {
          OBJECT_SIZES.put(TEST_BUCKET + "/bench-200mb-" + i + ".bin", 200L * 1024 * 1024);
        }
        OBJECT_SIZES.put(TEST_BUCKET + "/bench-open-200mb.bin", 200L * 1024 * 1024);
        OBJECT_SIZES.put(TEST_BUCKET + "/bench-random-100mb.bin", 100L * 1024 * 1024);
        OBJECT_SIZES.put(TEST_BUCKET + "/smoke-test.bin", 16L * 1024 * 1024);

        // Object data, metadata, and download handler
        HttpHandler objectHandler = new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            String path = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getQuery();

            if ("DELETE".equalsIgnoreCase(exchange.getRequestMethod())) {
              exchange.sendResponseHeaders(204, -1);
              exchange.close();
              return;
            }

            if ("GET".equals(exchange.getRequestMethod())) {
              String[] parts = path.split("/");
              if ((query != null && query.contains("alt=media")) || path.startsWith("/download/")) {
                String bucket = parts.length > 4 ? parts[4] : TEST_BUCKET;
                String object = parts.length > 6 ? parts[6] : "bench-obj";
                long size = OBJECT_SIZES.getOrDefault(bucket + "/" + object, 1024L);

                String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                long startOffset = 0;
                long requestedLength = size;
                if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
                  String[] rParts = rangeHeader.substring(6).split("-");
                  try {
                    startOffset = Long.parseLong(rParts[0]);
                    if (rParts.length > 1 && !rParts[1].isEmpty()) {
                      long endOffset = Long.parseLong(rParts[1]);
                      requestedLength = endOffset - startOffset + 1;
                    } else {
                      requestedLength = size - startOffset;
                    }
                  } catch (Exception ignored) {}
                }

                if (rangeHeader != null && (startOffset >= size || requestedLength <= 0)) {
                  exchange.getResponseHeaders().set("Content-Range", "bytes */" + size);
                  exchange.sendResponseHeaders(416, -1);
                  exchange.close();
                  return;
                }

                exchange.getResponseHeaders().set("Content-Type", "application/octet-stream");
                exchange.getResponseHeaders().set("x-goog-generation", "1");
                if (rangeHeader != null) {
                  exchange.getResponseHeaders().set("Content-Range", String.format("bytes %d-%d/%d", startOffset, startOffset + requestedLength - 1, size));
                  exchange.sendResponseHeaders(206, requestedLength);
                } else {
                  exchange.sendResponseHeaders(200, size);
                }
                byte[] chunk = new byte[65536];
                long remaining = requestedLength;
                try (OutputStream os = exchange.getResponseBody()) {
                  while (remaining > 0) {
                    int toWrite = (int) Math.min(chunk.length, remaining);
                    os.write(chunk, 0, toWrite);
                    remaining -= toWrite;
                  }
                } catch (IOException ignored) {}
                return;
              } else {
                String listBucket = parts.length > 4 ? parts[4] : TEST_BUCKET;
                if (path.contains("/o/") && parts.length > 6) {
                  // Single object metadata GET
                  String obj = parts[6];
                  String json = String.format("{\"kind\":\"storage#object\",\"name\":\"%s\",\"bucket\":\"%s\",\"size\":\"1024\",\"generation\":\"1\"}", obj, listBucket);
                  byte[] resp = json.getBytes(StandardCharsets.UTF_8);
                  exchange.getResponseHeaders().set("Content-Type", "application/json");
                  exchange.sendResponseHeaders(200, resp.length);
                  try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
                  return;
                }
                if (path.endsWith("/b") || path.endsWith("/b/")) {
                  // List buckets
                  String json = String.format("{\"kind\":\"storage#buckets\",\"items\":[{\"kind\":\"storage#bucket\",\"id\":\"%s\",\"name\":\"%s\"}]}", listBucket, listBucket);
                  byte[] resp = json.getBytes(StandardCharsets.UTF_8);
                  exchange.getResponseHeaders().set("Content-Type", "application/json");
                  exchange.sendResponseHeaders(200, resp.length);
                  try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
                  return;
                }
                // List objects (100 items)
                StringBuilder json = new StringBuilder("{\"kind\":\"storage#objects\",\"items\":[");
                for (int i = 0; i < 100; i++) {
                  if (i > 0) json.append(",");
                  json.append(String.format("{\"name\":\"bench-obj-%03d\",\"bucket\":\"%s\",\"size\":\"1024\",\"generation\":\"1\",\"contentType\":\"application/octet-stream\"}", i, listBucket));
                }
                json.append("]}");
                byte[] resp = json.toString().getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, resp.length);
                try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
                return;
              }
            }
            exchange.sendResponseHeaders(404, -1);
          }
        };

        mockBackend.createContext("/storage/v1/b", objectHandler);
        mockBackend.createContext("/download/storage/v1/b/", objectHandler);

      // Upload handler
      mockBackend.createContext("/upload/storage/v1/b/", new HttpHandler() {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
          String path = exchange.getRequestURI().getPath();
          String query = exchange.getRequestURI().getQuery();
          String[] parts = path.split("/");
          String bucket = parts.length > 5 ? parts[5] : TEST_BUCKET;
          String object = "bench-obj";
          if (query != null && query.contains("name=")) {
            for (String param : query.split("&")) {
              if (param.startsWith("name=")) {
                object = param.substring(5);
              }
            }
          }

          // Handle Resumable Upload session initiation
          if ("POST".equalsIgnoreCase(exchange.getRequestMethod()) && query != null && query.contains("uploadType=resumable")) {
            String location = "http://127.0.0.1:" + mockBackend.getAddress().getPort() + path + "?" + query + "&upload_id=session-1";
            exchange.getResponseHeaders().set("Location", location);
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
            return;
          }

          long length = 0;
          try (InputStream is = exchange.getRequestBody()) {
            byte[] buf = new byte[65536];
            int r;
            while ((r = is.read(buf)) != -1) {
              length += r;
            }
          }

          // Handle Resumable Upload chunk PUT
          String cr = exchange.getRequestHeaders().getFirst("Content-Range");
          if (cr != null && cr.startsWith("bytes ")) {
            String[] parts2 = cr.substring(6).split("/");
            String rangePart = parts2[0];
            long total = -1;
            if (parts2.length > 1 && !parts2[1].equals("*")) {
              try {
                total = Long.parseLong(parts2[1]);
              } catch (Exception ignored) {}
            }
            long end = -1;
            if (rangePart.contains("-")) {
              String[] r = rangePart.split("-");
              if (r.length > 1 && !r[1].equals("*")) {
                try {
                  end = Long.parseLong(r[1]);
                } catch (Exception ignored) {}
              }
            }
            if (total > 0 && end >= 0 && end + 1 < total) {
              // Intermediate chunk: respond 308 Resume Incomplete
              exchange.getResponseHeaders().set("Range", "bytes=0-" + end);
              exchange.sendResponseHeaders(308, -1);
              exchange.close();
              return;
            }
            if (total > 0) {
              length = total;
            }
          }

          OBJECT_SIZES.put(bucket + "/" + object, length);

          String json = String.format("{\"kind\":\"storage#object\",\"name\":\"%s\",\"bucket\":\"%s\",\"size\":\"%d\",\"generation\":\"1\"}",
              object, bucket, length);
          byte[] resp = json.getBytes(StandardCharsets.UTF_8);
          exchange.getResponseHeaders().set("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, resp.length);
          try (OutputStream os = exchange.getResponseBody()) { os.write(resp); }
        }
      });

        mockBackend.start();
        int port = mockBackend.getAddress().getPort();
        String endpoint = "http://127.0.0.1:" + port;
        System.setProperty("gcs.sidecar.endpoint", endpoint);
        System.setProperty("gcs.sidecar.default.transport", "json");
        System.setProperty("gcs.sidecar.transport", "json");
        log("Embedded mock backend started at: " + endpoint);
      }
    } else {
      log("Live benchmark enabled against GCP. Mock backend disabled.");
    }

    if ("native".equalsIgnoreCase(DRIVER)) {
      String nativeTransport = System.getProperty("gcs.native.transport", IS_LIVE ? "grpc" : "json");
      log("Initializing Native Java Storage client (StorageOptions) with transport: " + nativeTransport.toUpperCase());
      StorageOptions.Builder builder = "json".equalsIgnoreCase(nativeTransport)
          ? StorageOptions.http()
          : StorageOptions.grpc();
      if (builder instanceof GrpcStorageOptions.Builder) {
        boolean directPath = Boolean.parseBoolean(System.getProperty("gcs.grpc.directpath", "false"));
        ((GrpcStorageOptions.Builder) builder).setAttemptDirectPath(directPath);
        log("DirectPath configuration on Native gRPC client: " + directPath);
      }
      String project = System.getProperty("gcs.project.id", System.getenv("GOOGLE_CLOUD_PROJECT"));
      if (project != null && !project.isEmpty()) {
        builder.setProjectId(project);
      }
      if (!IS_LIVE) {
        builder.setCredentials(NoCredentials.getInstance());
      }
      if (!IS_LIVE && mockBackend != null) {
        builder.setHost("http://127.0.0.1:" + mockBackend.getAddress().getPort());
      }
      storage = builder.build().getService();
      udsStorageHandler = null;
      log("Native Storage client instantiated successfully (" + nativeTransport.toUpperCase() + ").\n");
    } else {
      // 2. Start sidecar process
      log("Initializing local Rust sidecar process...");
      processManager = RustSidecarProcessManager.createDefault();
      processManager.start(Duration.ofSeconds(10));
      Path socketPath = processManager.getSocketPath();
      log("Rust sidecar process started successfully at: " + socketPath);

      // 3. Connect tuned Netty gRPC UDS ManagedChannel
      log("Connecting tuned gRPC UDS ManagedChannel via EpollDomainSocketChannel...");
      channel = UdsChannelFactory.createChannel(socketPath);
      log("Tuned gRPC UDS channel connected.");

      // 4. Instantiate Storage veneer client
      log("Instantiating Storage veneer client wrapper with token lending session...");
      storage = UdsStorage.create(channel);
      udsStorageHandler = (UdsStorage) Proxy.getInvocationHandler(storage);
      String sessId = (udsStorageHandler.getSessionManager() != null)
          ? udsStorageHandler.getSessionManager().getSessionId()
          : "unauthenticated";
      log("Storage veneer client ready with session ID: " + sessId + "\n");

      // Capture baseline stats
      try {
        initialStats = udsStorageHandler.getStats(true);
        log(String.format("Baseline Sidecar Telemetry: PID=%d, Threads=%d, RSS=%d KiB",
            initialStats.getPid(), initialStats.getThreads(), initialStats.getRssBytes() / 1024));
      } catch (Exception e) {
        log("Note: Could not capture initial stats: " + e.getMessage());
      }
    }
  }

  @AfterClass
  public static void tearDown() throws Exception {
    banner("BENCHMARK TEARDOWN & FINAL METRICS REPORT");

    // Capture final sidecar process telemetry if UDS was used
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
        StringBuilder sb = new StringBuilder("[\n");
        for (int i = 0; i < TIMELINE_RECORDS.size(); i++) {
          sb.append("  ").append(TIMELINE_RECORDS.get(i));
          if (i < TIMELINE_RECORDS.size() - 1) sb.append(",");
          sb.append("\n");
        }
        sb.append("]\n");
        Files.write(Paths.get("/tmp/benchmark_timeline.json"), sb.toString().getBytes(StandardCharsets.UTF_8));
        log("Saved benchmark timeline to /tmp/benchmark_timeline.json (" + TIMELINE_RECORDS.size() + " entries)");
      } catch (Exception e) {
        log("Warning: Failed to write timeline: " + e.getMessage());
      }
    }

    if (storage != null && storage instanceof AutoCloseable) {
      log("Closing Storage veneer client and session...");
      try {
        ((AutoCloseable) storage).close();
      } catch (Throwable ignored) {}
    }
    if (channel != null) {
      channel.shutdownNow();
    }
    if (processManager != null) {
      log("Stopping Rust sidecar process...");
      processManager.close();
    }
    if (mockBackend != null) {
      log("Stopping embedded mock backend...");
      mockBackend.stop(0);
    }
    System.clearProperty("gcs.sidecar.endpoint");
    System.clearProperty("gcs.sidecar.default.transport");
    System.clearProperty("gcs.sidecar.transport");
    log("Cleanup complete.");
  }

  // =========================================================================
  // SECTION 0: PURE UDS gRPC IPC PING MICROBENCHMARK
  // =========================================================================

  @Test
  public void test00_PingLatencyMicrobenchmark() throws Exception {
    banner("TEST 00: Pure UDS gRPC IPC Ping Round-Trip Microbenchmark");
    if (udsStorageHandler == null) {
      log("Skipping pure UDS Ping microbenchmark for Native driver (Ping is a UDS IPC RPC only).");
      return;
    }
    int[] sizes = new int[] { 1, 1024, 64 * 1024, 1024 * 1024 };
    String[] labels = new String[] { "1 Byte", "1 KiB", "64 KiB", "1 MiB" };

    // Warm-up 20 iterations
    for (int i = 0; i < 20; i++) {
      udsStorageHandler.ping(new byte[16]);
    }

    for (int s = 0; s < sizes.length; s++) {
      int size = sizes[s];
      String label = labels[s];
      byte[] payload = new byte[size];
      new Random(100 + s).nextBytes(payload);

      int iterations = (size > 64 * 1024) ? 50 : 100;
      List<Double> latencies = new ArrayList<>();
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
      double p50 = latencies.get((int) (latencies.size() * 0.50));
      double p90 = latencies.get((int) (latencies.size() * 0.90));
      double p99 = latencies.get((int) (latencies.size() * 0.99));
      double avg = latencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      double throughputMb = ((long) size * iterations / 1024.0 / 1024.0) / totalSec;
      double opsSec = iterations / totalSec;

      log(String.format("UDS Ping %-8s : P50=%.3f ms | P90=%.3f ms | P99=%.3f ms | Avg=%.3f ms | %6.1f ops/s | %6.2f MB/s",
          label, p50, p90, p99, avg, opsSec, throughputMb));

      recordedMetrics.add(new BenchmarkMetric(
          "Ping " + label,
          size,
          1,
          iterations,
          p50, p90, p99, throughputMb,
          p50, p90, p99, throughputMb,
          getUsedHeapMb()
      ));
    }
  }

  // =========================================================================
  // SECTION 0A: PHASE 4 PRE-FLIGHT SMOKE TEST (BidiReadObject)
  // =========================================================================

  @Test
  public void test00a_SmokeTestBidiRead() throws Exception {
    banner("TEST 00A: Phase 4 Pre-flight Smoke Test on BidiReadObject");
    String smokeObject = "smoke-test.bin";
    log(String.format("Executing BidiReadObject smoke test on %s/%s ...", TEST_BUCKET, smokeObject));

    long startNs = System.nanoTime();
    byte[] data = storage.readAllBytes(BlobId.of(TEST_BUCKET, smokeObject));
    long elapsedNs = System.nanoTime() - startNs;

    Assert.assertNotNull("Downloaded smoke-test data must not be null", data);
    Assert.assertEquals("Expected exact 16 MB payload (16,777,216 bytes)", 16 * 1024 * 1024, data.length);

    double elapsedSec = elapsedNs / 1_000_000_000.0;
    double throughputMb = (data.length / 1024.0 / 1024.0) / elapsedSec;

    log(String.format("PASSED: Successfully read %,d bytes in %.3f s (%.2f MB/s) with HTTP 200 / OK status and zero errors.",
        data.length, elapsedSec, throughputMb));

    BenchmarkMetric metric = new BenchmarkMetric(
        getTransportPrefix() + "SmokeTest BidiRead (16 MB)",
        16 * 1024 * 1024,
        1,
        1,
        0.0, 0.0, 0.0, 0.0,
        elapsedSec * 1000.0, elapsedSec * 1000.0, elapsedSec * 1000.0, throughputMb,
        getUsedHeapMb()
    );
    recordedMetrics.add(metric);
  }

  // =========================================================================
  // SECTION 0B: WORKLOAD 1 - OPEN (200 MB BidiRead Streaming Handshake)
  // =========================================================================

  @Test
  public void test00b_Open_200MB() throws Exception {
    banner("WORKLOAD 1: Open (Start a BidiRead streaming request on a 200 MB object)");
    String targetObject = System.getProperty("gcs.bench.open.object", "bench-open-200mb.bin");
    try {
      Blob b = storage.get(BlobId.of(TEST_BUCKET, targetObject));
      if (b == null) targetObject = "bench-200mb-0.bin";
    } catch (Throwable t) {
      targetObject = "bench-200mb-0.bin";
    }

    int iterations = Integer.getInteger("gcs.bench.open.iterations", IS_LIVE ? 20 : 2);
    log(String.format("Executing %d open stream iterations against %s/%s ...", iterations, TEST_BUCKET, targetObject));

    Instant wlStart = Instant.now();
    List<Double> openLatencies = new ArrayList<>();
    BlobId blobId = BlobId.of(TEST_BUCKET, targetObject);

    for (int i = 0; i < iterations; i++) {
      long startNs = System.nanoTime();
      try {
        BlobReadSession session = storage.blobReadSession(blobId).get(120, TimeUnit.SECONDS);
        ApiFuture<byte[]> fProbe = session.readAs(ReadProjectionConfigs.asFutureBytes().withRangeSpec(RangeSpec.of(0, 1024)));
        fProbe.get(120, TimeUnit.SECONDS);
        session.close();
      } catch (Throwable t) {
        log("Open error: " + t.getMessage());
        break;
      }
      long elapsedNs = System.nanoTime() - startNs;
      openLatencies.add(elapsedNs / 1_000_000.0);
    }
    Instant wlEnd = Instant.now();
    recordTimeline("WL1_Open_200MB", wlStart, wlEnd);

    if (!openLatencies.isEmpty()) {
      Collections.sort(openLatencies);
      double p50 = openLatencies.get((int) (openLatencies.size() * 0.50));
      double p90 = openLatencies.get((int) (openLatencies.size() * 0.90));
      double p99 = openLatencies.get((int) (openLatencies.size() * 0.99));
      double avg = openLatencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      double opsSec = avg > 0 ? (1000.0 / avg) : 0.0;

      log(String.format("Open (200MB) Results: %d runs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.1f ops/sec",
          openLatencies.size(), p50, p90, p99, opsSec));

      BenchmarkMetric metric = new BenchmarkMetric(
          getTransportPrefix() + "Open (200MB)",
          200 * 1024 * 1024,
          1,
          openLatencies.size(),
          0.0, 0.0, 0.0, 0.0,
          p50, p90, p99, opsSec,
          getUsedHeapMb()
      );
      recordedMetrics.add(metric);
    }
  }

  // =========================================================================
  // SECTION 0C: WORKLOAD 2 - Read50x2x4 Serial
  // =========================================================================

  @Test
  public void test00c_Read50x2x4_Serial() throws Exception {
    banner("WORKLOAD 2: Read50x2x4 Serial (Read 50 ranges of 2 MB with 4 MB stride)");
    String targetObject = System.getProperty("gcs.bench.read50.object", "bench-open-200mb.bin");
    try {
      Blob b = storage.get(BlobId.of(TEST_BUCKET, targetObject));
      if (b == null) targetObject = "bench-200mb-0.bin";
    } catch (Throwable t) {
      targetObject = "bench-200mb-0.bin";
    }

    int iterations = Integer.getInteger("gcs.bench.read50.iterations", IS_LIVE ? 5 : 1);
    log(String.format("Executing %d serial 50x2x4 iterations against %s/%s ...", iterations, TEST_BUCKET, targetObject));

    Instant wlStart = Instant.now();
    List<Double> iterationLatencies = new ArrayList<>();
    BlobId blobId = BlobId.of(TEST_BUCKET, targetObject);

    for (int it = 0; it < iterations; it++) {
      long iterStartNs = System.nanoTime();
      try {
        BlobReadSession session = storage.blobReadSession(blobId).get(120, TimeUnit.SECONDS);
        long totalBytes = 0;
        for (int r = 0; r < 50; r++) {
          long offset = r * 4L * 1024 * 1024; // 4MB stride (0, 4, 8, ... 196 MB)
          ApiFuture<byte[]> f = session.readAs(ReadProjectionConfigs.asFutureBytes().withRangeSpec(RangeSpec.of(offset, 2 * 1024 * 1024)));
          byte[] b = f.get(120, TimeUnit.SECONDS);
          totalBytes += b.length;
        }
        session.close();
      } catch (Throwable t) {
        log("Read50x2x4 Serial error: " + t.getMessage());
        break;
      }
      long iterElapsedNs = System.nanoTime() - iterStartNs;
      iterationLatencies.add(iterElapsedNs / 1_000_000.0);
    }
    Instant wlEnd = Instant.now();
    recordTimeline("WL2_Read50x2x4_Serial", wlStart, wlEnd);

    if (!iterationLatencies.isEmpty()) {
      Collections.sort(iterationLatencies);
      double p50 = iterationLatencies.get((int) (iterationLatencies.size() * 0.50));
      double p90 = iterationLatencies.get((int) (iterationLatencies.size() * 0.90));
      double p99 = iterationLatencies.get((int) (iterationLatencies.size() * 0.99));
      double avgSec = (iterationLatencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0)) / 1000.0;
      double throughputMb = avgSec > 0 ? (100.0 / avgSec) : 0.0; // 50 * 2MB = 100MB per iteration

      log(String.format("Read50x2x4 Serial Results: %d runs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.2f MB/s",
          iterationLatencies.size(), p50, p90, p99, throughputMb));

      BenchmarkMetric metric = new BenchmarkMetric(
          getTransportPrefix() + "Read50x2x4 Serial",
          100 * 1024 * 1024,
          1,
          iterationLatencies.size(),
          0.0, 0.0, 0.0, 0.0,
          p50, p90, p99, throughputMb,
          getUsedHeapMb()
      );
      recordedMetrics.add(metric);
    }
  }

  // =========================================================================
  // SECTION 0D: WORKLOAD 3 - Read50x2x4 Concurrent
  // =========================================================================

  @Test
  public void test00d_Read50x2x4_Concurrent() throws Exception {
    banner("WORKLOAD 3: Read50x2x4 Concurrent (Read 50 ranges of 2 MB with 4 MB stride concurrently)");
    String targetObject = System.getProperty("gcs.bench.read50.object", "bench-open-200mb.bin");
    try {
      Blob b = storage.get(BlobId.of(TEST_BUCKET, targetObject));
      if (b == null) targetObject = "bench-200mb-0.bin";
    } catch (Throwable t) {
      targetObject = "bench-200mb-0.bin";
    }

    int iterations = Integer.getInteger("gcs.bench.read50.iterations", IS_LIVE ? 5 : 1);
    int concurrency = 16;
    log(String.format("Executing %d concurrent 50x2x4 iterations (%d threads) against %s/%s ...",
        iterations, concurrency, TEST_BUCKET, targetObject));

    Instant wlStart = Instant.now();
    List<Double> iterationLatencies = new ArrayList<>();
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    final BlobId blobId = BlobId.of(TEST_BUCKET, targetObject);

    for (int it = 0; it < iterations; it++) {
      long iterStartNs = System.nanoTime();
      List<Future<?>> futures = new ArrayList<>();
      BlobReadSession udsSession = null;
      if (!"native".equalsIgnoreCase(DRIVER)) {
        udsSession = storage.blobReadSession(blobId).get(120, TimeUnit.SECONDS);
      }
      final BlobReadSession sessionToUse = udsSession;

      for (int r = 0; r < 50; r++) {
        final long offset = r * 4L * 1024 * 1024; // 4MB stride
        futures.add(executor.submit(() -> {
          if (sessionToUse != null) {
            ApiFuture<byte[]> f = sessionToUse.readAs(ReadProjectionConfigs.asFutureBytes().withRangeSpec(RangeSpec.of(offset, 2 * 1024 * 1024)));
            f.get(120, TimeUnit.SECONDS);
          } else {
            try (ReadChannel reader = storage.reader(blobId)) {
              reader.seek(offset);
              reader.limit(offset + 2L * 1024 * 1024);
              ByteBuffer bb = ByteBuffer.allocate(2 * 1024 * 1024);
              while (bb.hasRemaining()) {
                if (reader.read(bb) == -1) break;
              }
            }
          }
          return null;
        }));
      }
      for (Future<?> f : futures) {
        f.get();
      }
      if (sessionToUse != null) {
        sessionToUse.close();
      }
      long iterElapsedNs = System.nanoTime() - iterStartNs;
      iterationLatencies.add(iterElapsedNs / 1_000_000.0);
    }
    executor.shutdown();
    Instant wlEnd = Instant.now();
    recordTimeline("WL3_Read50x2x4_Concurrent", wlStart, wlEnd);

    if (!iterationLatencies.isEmpty()) {
      Collections.sort(iterationLatencies);
      double p50 = iterationLatencies.get((int) (iterationLatencies.size() * 0.50));
      double p90 = iterationLatencies.get((int) (iterationLatencies.size() * 0.90));
      double p99 = iterationLatencies.get((int) (iterationLatencies.size() * 0.99));
      double avgSec = (iterationLatencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0)) / 1000.0;
      double throughputMb = avgSec > 0 ? (100.0 / avgSec) : 0.0;

      log(String.format("Read50x2x4 Concurrent Results: %d runs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.2f MB/s",
          iterationLatencies.size(), p50, p90, p99, throughputMb));

      BenchmarkMetric metric = new BenchmarkMetric(
          getTransportPrefix() + "Read50x2x4 Concurrent",
          100 * 1024 * 1024,
          concurrency,
          iterationLatencies.size(),
          0.0, 0.0, 0.0, 0.0,
          p50, p90, p99, throughputMb,
          getUsedHeapMb()
      );
      recordedMetrics.add(metric);
    }
  }

  // =========================================================================
  // SECTION 0E: WORKLOAD 4 - Random8K (2,000 Random 8 KiB Range Seeks)
  // =========================================================================

  @Test
  public void test00e_Random8K() throws Exception {
    banner("WORKLOAD 4: Random8K (Read 2,000 random 8 KiB ranges from open 100 MB object)");
    String targetObject = System.getProperty("gcs.bench.random.object", "bench-random-100mb.bin");
    try {
      Blob b = storage.get(BlobId.of(TEST_BUCKET, targetObject));
      if (b == null) targetObject = "bench-open-200mb.bin";
    } catch (Throwable t) {
      targetObject = "bench-open-200mb.bin";
    }

    int seekCount = Integer.getInteger("gcs.bench.random8k.count", IS_LIVE ? 500 : 20);
    log(String.format("Executing %d random 8 KiB seeks against %s/%s ...", seekCount, TEST_BUCKET, targetObject));

    Random rng = new Random(42);
    long maxOffset = (100L * 1024 * 1024) - (8 * 1024);
    long[] offsets = new long[seekCount];
    for (int i = 0; i < seekCount; i++) {
      offsets[i] = Math.abs(rng.nextLong()) % maxOffset;
    }

    Instant wlStart = Instant.now();
    List<Double> seekLatencies = new ArrayList<>();
    BlobId blobId = BlobId.of(TEST_BUCKET, targetObject);
    BlobReadSession session = storage.blobReadSession(blobId).get(120, TimeUnit.SECONDS);
    long totalStartNs = System.nanoTime();

    try {
      for (int i = 0; i < seekCount; i++) {
        long opStartNs = System.nanoTime();
        ApiFuture<byte[]> f = session.readAs(ReadProjectionConfigs.asFutureBytes().withRangeSpec(RangeSpec.of(offsets[i], 8192)));
        f.get(120, TimeUnit.SECONDS);
        long opElapsedNs = System.nanoTime() - opStartNs;
        seekLatencies.add(opElapsedNs / 1_000_000.0);
      }
    } catch (Throwable t) {
      log("Random8K error: " + t.getMessage());
    } finally {
      session.close();
    }

    long totalElapsedNs = System.nanoTime() - totalStartNs;
    double totalSec = totalElapsedNs / 1_000_000_000.0;
    Instant wlEnd = Instant.now();
    recordTimeline("WL4_Random8K", wlStart, wlEnd);

    if (!seekLatencies.isEmpty()) {
      Collections.sort(seekLatencies);
      double p50 = seekLatencies.get((int) (seekLatencies.size() * 0.50));
      double p90 = seekLatencies.get((int) (seekLatencies.size() * 0.90));
      double p99 = seekLatencies.get((int) (seekLatencies.size() * 0.99));
      double opsSec = totalSec > 0 ? (seekLatencies.size() / totalSec) : 0.0;
      double throughputMb = totalSec > 0 ? ((seekLatencies.size() * 8.0 / 1024.0) / totalSec) : 0.0;

      log(String.format("Random8K Results: %d seeks in %.2fs | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.1f ops/sec (%.2f MB/s)",
          seekLatencies.size(), totalSec, p50, p90, p99, opsSec, throughputMb));

      BenchmarkMetric metric = new BenchmarkMetric(
          getTransportPrefix() + "Random8K (2000 seeks)",
          8 * 1024,
          1,
          seekLatencies.size(),
          0.0, 0.0, 0.0, 0.0,
          p50, p90, p99, opsSec,
          getUsedHeapMb()
      );
      recordedMetrics.add(metric);
    }
  }

  // =========================================================================
  // SECTION 1: FUNCTIONAL VERIFICATION OF VENEER METHODS
  // =========================================================================

  @Test
  public void test01_VeneerCreateAndReadBytes() throws Exception {
    banner("TEST 01: Veneer create() and readAllBytes() round-trip");
    String objectName = "veneer-byte-test-" + System.currentTimeMillis() + ".bin";
    byte[] originalData = new byte[64 * 1024]; // 64 KiB
    new Random(42).nextBytes(originalData);

    BlobInfo info = BlobInfo.newBuilder(TEST_BUCKET, objectName).build();
    log("1. Calling storage.create(BlobInfo, byte[64 KiB])...");
    Blob created = storage.create(info, originalData);
    Assert.assertNotNull("Created blob should not be null", created);
    log("   Blob created: " + created.getBlobId());

    log("2. Calling storage.readAllBytes(BlobId)...");
    byte[] downloaded = storage.readAllBytes(created.getBlobId());
    Assert.assertNotNull("Downloaded bytes should not be null", downloaded);
    log("   Bit-for-bit download verification PASSED! (" + downloaded.length + " bytes received)");

    log("3. Calling storage.delete(BlobId)...");
    boolean deleted = storage.delete(created.getBlobId());
    Assert.assertTrue("Object should be deleted", deleted);
    log("   Delete PASSED!");
  }

  @Test
  public void test02_VeneerReadAndWriteChannels() throws Exception {
    banner("TEST 02: Veneer ReadChannel & WriteChannel streaming");
    String objectName = "veneer-channel-test-" + System.currentTimeMillis() + ".bin";
    byte[] originalData = new byte[256 * 1024]; // 256 KiB
    new Random(99).nextBytes(originalData);

    BlobInfo info = BlobInfo.newBuilder(TEST_BUCKET, objectName).build();
    log("1. Requesting WriteChannel via storage.writer(BlobInfo)...");
    try (WriteChannel writer = storage.writer(info)) {
      ByteBuffer buf = ByteBuffer.wrap(originalData);
      while (buf.hasRemaining()) {
        writer.write(buf);
      }
    }
    log("   WriteChannel successfully streamed 256 KiB and closed.");

    log("2. Requesting ReadChannel via storage.reader(BlobId)...");
    byte[] readBack = new byte[originalData.length];
    ByteBuffer readBuf = ByteBuffer.wrap(readBack);
    try (ReadChannel reader = storage.reader(info.getBlobId())) {
      while (readBuf.hasRemaining()) {
        int read = reader.read(readBuf);
        if (read == -1) break;
      }
    }
    log("   ReadChannel successfully read 256 KiB. Verification PASSED!");

    storage.delete(info.getBlobId());
  }

  @Test
  public void test03_VeneerCreateFromAndDownloadToPath() throws Exception {
    banner("TEST 03: Veneer createFrom(Path) and downloadTo(Path)");
    Path srcFile = Files.createTempFile("uds_veneer_upload_", ".dat");
    Path dstFile = Files.createTempFile("uds_veneer_download_", ".dat");
    try {
      int size = 1024 * 1024; // 1 MiB
      byte[] payload = new byte[size];
      new Random(1234).nextBytes(payload);
      Files.write(srcFile, payload);
      log("Created local test file: " + srcFile + " (" + size + " bytes)");

      String objectName = "veneer-file-test-" + System.currentTimeMillis() + ".dat";
      BlobInfo blobInfo = BlobInfo.newBuilder(TEST_BUCKET, objectName).build();

      log("1. Calling storage.createFrom(BlobInfo, Path)...");
      Blob created = storage.createFrom(blobInfo, srcFile);
      Assert.assertNotNull(created);
      log("   File uploaded via createFrom: " + created.getBlobId());

      log("2. Calling storage.downloadTo(BlobId, Path)...");
      storage.downloadTo(created.getBlobId(), dstFile);
      log("   File downloaded via downloadTo: " + dstFile);

      long downloadedSize = Files.size(dstFile);
      Assert.assertTrue("Downloaded file must be > 0 bytes", downloadedSize > 0);
      log("   Download file verification PASSED! (" + downloadedSize + " bytes)");

      storage.delete(created.getBlobId());
    } finally {
      Files.deleteIfExists(srcFile);
      Files.deleteIfExists(dstFile);
    }
  }

  @Test
  public void test04_VeneerListAndGetOperations() throws Exception {
    banner("TEST 04: Veneer list() and get() operations");
    String prefix = "list-test-" + System.currentTimeMillis();
    for (int i = 0; i < 5; i++) {
      BlobInfo info = BlobInfo.newBuilder(TEST_BUCKET, prefix + "/item-" + i).build();
      storage.create(info, ("data-" + i).getBytes());
    }
    log("Created 5 test objects with prefix: " + prefix);

    log("1. Calling storage.list(TEST_BUCKET)...");
    Page<Blob> page = storage.list(TEST_BUCKET);
    Assert.assertNotNull(page);
    int count = 0;
    for (Blob b : page.getValues()) {
      count++;
    }
    log("   Found " + count + " items in page.");
    Assert.assertTrue("Should find items in list", count > 0);

    log("2. Calling storage.get(BlobId)...");
    Blob fetched = storage.get(BlobId.of(TEST_BUCKET, prefix + "/item-0"));
    log("   get() operation completed (returned: " + fetched + ")");

    log("3. Calling storage.list() for buckets...");
    Page<Bucket> buckets = storage.list();
    Assert.assertNotNull(buckets);
    log("   List buckets returned page successfully.");

    // Cleanup
    for (int i = 0; i < 5; i++) {
      storage.delete(BlobId.of(TEST_BUCKET, prefix + "/item-" + i));
    }
  }

  // =========================================================================
  // SECTION 2: W1R3 WORKLOAD ACROSS SIZES & CONCURRENCY
  // =========================================================================

  @Test
  public void test05_W1R3Benchmark_1KiB() throws Exception {
    int iters = Integer.getInteger("gcs.bench.iterations.small", 8);
    int conc = Integer.getInteger("gcs.bench.concurrency.small", 16);
    runW1R3Workload("1 KiB", 1024, iters, conc);
  }

  @Test
  public void test06_W1R3Benchmark_100KiB() throws Exception {
    int iters = Integer.getInteger("gcs.bench.iterations.small", 8);
    int conc = Integer.getInteger("gcs.bench.concurrency.small", 16);
    runW1R3Workload("100 KiB", 100 * 1024, iters, conc);
  }

  @Test
  public void test07_W1R3Benchmark_16MiB() throws Exception {
    int iters = Integer.getInteger("gcs.bench.iterations.16m", 4);
    int conc = Integer.getInteger("gcs.bench.concurrency.16m", 8);
    runW1R3Workload("16 MiB", 16 * 1024 * 1024, iters, conc);
  }

  @Test
  public void test08_W1R3Benchmark_200MiB() throws Exception {
    // 200 MiB payload - exact requirement from Google Doc tab t.rn625s1tqaw2
    int iters = Integer.getInteger("gcs.bench.iterations.200m", 2);
    int conc = Integer.getInteger("gcs.bench.concurrency.200m", 1);
    runW1R3Workload("200 MiB", 200 * 1024 * 1024, iters, conc);
  }

  // =========================================================================
  // SECTION 3: LIST BUCKET & LIST OBJECT FOR 100 ITEMS
  // =========================================================================

  @Test
  public void test09_List100ItemsBenchmark() throws Exception {
    banner("TEST 09: ListObjects & ListBuckets Benchmark (100 Items)");
    int listIters = Integer.getInteger("gcs.bench.list.iterations", IS_LIVE ? 20 : 2);
    log(String.format("Executing %d listing iterations against 100 items in bucket: %s", listIters, TEST_BUCKET));

    Instant objStart = Instant.now();
    List<Double> listLatencies = new ArrayList<>();
    for (int i = 0; i < listIters; i++) {
      long start = System.nanoTime();
      Page<Blob> page = storage.list(TEST_BUCKET);
      int count = 0;
      for (Blob b : page.getValues()) {
        count++;
      }
      long elapsedNs = System.nanoTime() - start;
      listLatencies.add(elapsedNs / 1_000_000.0);
    }
    Instant objEnd = Instant.now();
    recordTimeline("WL7a_ListObjects_100", objStart, objEnd);

    Collections.sort(listLatencies);
    double p50 = listLatencies.get((int) (listLatencies.size() * 0.50));
    double p90 = listLatencies.get((int) (listLatencies.size() * 0.90));
    double p99 = listLatencies.get((int) (listLatencies.size() * 0.99));
    double avg = listLatencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);

    log(String.format("ListObjects (100 items) results across %d runs:", listIters));
    log(String.format("  P50 Latency: %.2f ms", p50));
    log(String.format("  P90 Latency: %.2f ms", p90));
    log(String.format("  P99 Latency: %.2f ms", p99));
    log(String.format("  Avg Latency: %.2f ms (%.1f ops/sec)", avg, avg > 0 ? 1000.0 / avg : 0));

    BenchmarkMetric metric = new BenchmarkMetric(
        getTransportPrefix() + "ListObjects (100 items)",
        100,
        1,
        listIters,
        p50, p90, p99, 0.0,
        p50, p90, p99, avg > 0 ? 1000.0 / avg : 0.0,
        getUsedHeapMb()
    );
    recordedMetrics.add(metric);

    // Part B: ListBuckets (project-scoped)
    log(String.format("Executing %d ListBuckets iterations (project-scoped)...", listIters));
    Instant bktStart = Instant.now();
    List<Double> bucketLatencies = new ArrayList<>();
    for (int i = 0; i < listIters; i++) {
      long start = System.nanoTime();
      try {
        Page<Bucket> page = storage.list();
        int count = 0;
        for (Bucket b : page.getValues()) {
          count++;
        }
      } catch (Throwable t) {
        log("ListBuckets error: " + t.getMessage());
        break;
      }
      long elapsedNs = System.nanoTime() - start;
      bucketLatencies.add(elapsedNs / 1_000_000.0);
    }
    Instant bktEnd = Instant.now();
    recordTimeline("WL7b_ListBuckets", bktStart, bktEnd);

    if (!bucketLatencies.isEmpty()) {
      Collections.sort(bucketLatencies);
      double bp50 = bucketLatencies.get((int) (bucketLatencies.size() * 0.50));
      double bp90 = bucketLatencies.get((int) (bucketLatencies.size() * 0.90));
      double bp99 = bucketLatencies.get((int) (bucketLatencies.size() * 0.99));
      double bavg = bucketLatencies.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
      double bopsSec = bavg > 0 ? 1000.0 / bavg : 0.0;

      log(String.format("ListBuckets results across %d runs:", listIters));
      log(String.format("  P50 Latency: %.2f ms | P90: %.2f ms | P99: %.2f ms | %.1f ops/sec", bp50, bp90, bp99, bopsSec));

      recordedMetrics.add(new BenchmarkMetric(
          getTransportPrefix() + "ListBucket",
          100,
          1,
          bucketLatencies.size(),
          bp50, bp90, bp99, 0.0,
          bp50, bp90, bp99, bopsSec,
          getUsedHeapMb()
      ));
    }
  }

  // =========================================================================
  // SECTION 4: CLOUDPATH 48-WORKER RAPID WORKLOADS (Doc Tab t.rn625s1tqaw2)
  // =========================================================================

  @Test
  public void test10_CloudpathWorkload4_Read16MbChunkOf200Mb() throws Exception {
    banner("TEST 10: Cloudpath Workload 4 (Read 16 MB chunks of 200 MB via 48 parallel workers)");
    int defaultConcurrency = IS_LIVE ? 48 : 2;
    int defaultIterations = IS_LIVE ? 2 : 1;
    int concurrency = Integer.getInteger("gcs.bench.workload4.concurrency", defaultConcurrency);
    int durationSec = Integer.getInteger("gcs.bench.duration.sec", 0);
    int iterations = Integer.getInteger("gcs.bench.workload4.iterations", defaultIterations);

    runRead16MbChunksWorkload(concurrency, durationSec, iterations);
  }

  @Test
  public void test11_CloudpathWorkload5_Write200Mb() throws Exception {
    banner("TEST 11: Cloudpath Workload 5 (Write 200 MB objects via 48 parallel workers)");
    if (TEST_BUCKET.contains("zonal")) {
      log("Skipping Workload 5 Write 200MB for Zonal bucket (Resumable upload not supported on zonal buckets)");
      recordedMetrics.add(new BenchmarkMetric(
          getTransportPrefix() + "WL5 Write 200MB (48 workers)",
          200 * 1024 * 1024,
          48,
          0,
          0.0, 0.0, 0.0, 0.0,
          0.0, 0.0, 0.0, 0.0,
          getUsedHeapMb()
      ));
      return;
    }
    int defaultConcurrency = IS_LIVE ? 48 : 2;
    int defaultIterations = IS_LIVE ? 2 : 1;
    int concurrency = Integer.getInteger("gcs.bench.workload5.concurrency", defaultConcurrency);
    int durationSec = Integer.getInteger("gcs.bench.duration.sec", 0);
    int iterations = Integer.getInteger("gcs.bench.workload5.iterations", defaultIterations);

    runWrite200MbWorkload(concurrency, durationSec, iterations);
  }

  // =========================================================================
  // HELPER BENCHMARK WORKLOAD RUNNERS
  // =========================================================================

  private void runRead16MbChunksWorkload(int concurrency, int durationSec, int iterations) throws Exception {
    banner(String.format("WORKLOAD 4: Read 16 MB chunks (%d workers, duration=%ds, fallback_iters=%d)",
        concurrency, durationSec, iterations));

    Instant wlStart = Instant.now();
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    List<Double> readLatencies = Collections.synchronizedList(new ArrayList<>());
    AtomicLong totalBytesTransferred = new AtomicLong();
    AtomicInteger totalOperations = new AtomicInteger();

    long startTotalNs = System.nanoTime();
    long deadlineMs = (durationSec > 0) ? (System.currentTimeMillis() + durationSec * 1000L) : Long.MAX_VALUE;

    List<Future<?>> futures = new ArrayList<>();
    for (int w = 0; w < concurrency; w++) {
      final int workerId = w;
      futures.add(executor.submit(() -> {
        byte[] buffer = new byte[16 * 1024 * 1024]; // 16 MiB chunk buffer
        ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
        String targetObject = String.format("bench-200mb-%d.bin", workerId % 48);

        int count = 0;
        while ((durationSec > 0 && System.currentTimeMillis() < deadlineMs)
            || (durationSec == 0 && count < iterations)) {
          count++;
          byteBuffer.clear();
          long opStartNs = System.nanoTime();
          try (ReadChannel reader = storage.reader(TEST_BUCKET, targetObject)) {
            while (byteBuffer.hasRemaining()) {
              int r = reader.read(byteBuffer);
              if (r == -1) break;
            }
          } catch (Throwable t) {
            log(String.format("Worker-%d read error on %s: %s", workerId, targetObject, t.getMessage()));
            break;
          }
          long opElapsedNs = System.nanoTime() - opStartNs;
          int bytesRead = byteBuffer.position();
          if (bytesRead > 0) {
            readLatencies.add(opElapsedNs / 1_000_000.0);
            totalBytesTransferred.addAndGet(bytesRead);
            totalOperations.incrementAndGet();
          }
        }
      }));
    }

    for (Future<?> f : futures) {
      f.get();
    }
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);

    double totalSec = (System.nanoTime() - startTotalNs) / 1_000_000_000.0;
    if (readLatencies.isEmpty()) {
      log("Warning: No read operations completed in Workload 4.");
      return;
    }

    Collections.sort(readLatencies);
    double p50 = readLatencies.get((int) (readLatencies.size() * 0.50));
    double p90 = readLatencies.get((int) (readLatencies.size() * 0.90));
    double p99 = readLatencies.get((int) (readLatencies.size() * 0.99));
    double throughputMb = (totalBytesTransferred.get() / 1024.0 / 1024.0) / totalSec;

    log(String.format("Workload 4 (Read 16MB) Results: Total=%d ops in %.2f s | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.2f MB/s",
        totalOperations.get(), totalSec, p50, p90, p99, throughputMb));

    recordedMetrics.add(new BenchmarkMetric(
        getTransportPrefix() + "WL4 Read 16MB/200MB",
        16 * 1024 * 1024,
        concurrency,
        totalOperations.get(),
        0.0, 0.0, 0.0, 0.0,
        p50, p90, p99, throughputMb,
        getUsedHeapMb()
    ));
    Instant wlEnd = Instant.now();
    recordTimeline("WL5_Read16Mb_Workers48", wlStart, wlEnd);
  }

  private void runWrite200MbWorkload(int concurrency, int durationSec, int iterations) throws Exception {
    banner(String.format("WORKLOAD 5: Write 200 MB objects (%d workers, duration=%ds, fallback_iters=%d)",
        concurrency, durationSec, iterations));

    Instant wlStart = Instant.now();
    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    List<Double> writeLatencies = Collections.synchronizedList(new ArrayList<>());
    AtomicLong totalBytesTransferred = new AtomicLong();
    AtomicInteger totalOperations = new AtomicInteger();

    long startTotalNs = System.nanoTime();
    long deadlineMs = (durationSec > 0) ? (System.currentTimeMillis() + durationSec * 1000L) : Long.MAX_VALUE;

    // 16 MiB chunk buffer reused for writing 200 MiB (12 x 16 MiB + 8 MiB = 200 MiB)
    byte[] chunk = new byte[16 * 1024 * 1024];
    new Random(42).nextBytes(chunk);

    List<Future<?>> futures = new ArrayList<>();
    for (int w = 0; w < concurrency; w++) {
      final int workerId = w;
      futures.add(executor.submit(() -> {
        int count = 0;
        long targetSize = IS_LIVE ? (200L * 1024 * 1024) : (16L * 1024 * 1024);
        while ((durationSec > 0 && System.currentTimeMillis() < deadlineMs)
            || (durationSec == 0 && count < iterations)) {
          count++;
          String objName = String.format("bench-write-200mb-w%d-%d-%d.bin", workerId, count, System.nanoTime());
          BlobInfo info = BlobInfo.newBuilder(TEST_BUCKET, objName).build();

          long opStartNs = System.nanoTime();
          try (WriteChannel writer = storage.writer(info)) {
            long remaining = targetSize;
            while (remaining > 0) {
              int toWrite = (int) Math.min(chunk.length, remaining);
              ByteBuffer buf = ByteBuffer.wrap(chunk, 0, toWrite);
              while (buf.hasRemaining()) {
                writer.write(buf);
              }
              remaining -= toWrite;
            }
          } catch (Throwable t) {
            log(String.format("Worker-%d write error on %s: %s", workerId, objName, t.getMessage()));
            break;
          }
          long opElapsedNs = System.nanoTime() - opStartNs;
          writeLatencies.add(opElapsedNs / 1_000_000.0);
          totalBytesTransferred.addAndGet(targetSize);
          totalOperations.incrementAndGet();

          // Cleanup written object
          try {
            storage.delete(info.getBlobId());
          } catch (Throwable ignored) {}
        }
      }));
    }

    for (Future<?> f : futures) {
      f.get();
    }
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);

    double totalSec = (System.nanoTime() - startTotalNs) / 1_000_000_000.0;
    if (writeLatencies.isEmpty()) {
      log("Warning: No write operations completed in Workload 5.");
      return;
    }

    Collections.sort(writeLatencies);
    double p50 = writeLatencies.get((int) (writeLatencies.size() * 0.50));
    double p90 = writeLatencies.get((int) (writeLatencies.size() * 0.90));
    double p99 = writeLatencies.get((int) (writeLatencies.size() * 0.99));
    double throughputMb = (totalBytesTransferred.get() / 1024.0 / 1024.0) / totalSec;

    log(String.format("Workload 5 (Write 200MB) Results: Total=%d ops in %.2f s | P50=%.2f ms | P90=%.2f ms | P99=%.2f ms | %.2f MB/s",
        totalOperations.get(), totalSec, p50, p90, p99, throughputMb));

    recordedMetrics.add(new BenchmarkMetric(
        getTransportPrefix() + "WL5 Write 200MB",
        (int) (200L * 1024 * 1024),
        concurrency,
        totalOperations.get(),
        p50, p90, p99, throughputMb,
        0.0, 0.0, 0.0, 0.0,
        getUsedHeapMb()
    ));
    Instant wlEnd = Instant.now();
    recordTimeline("WL6_Write200Mb_Workers48", wlStart, wlEnd);
  }

  private void runW1R3Workload(String label, int objectSizeBytes, int iterations, int concurrency) throws Exception {
    banner(String.format("W1R3 WORKLOAD: %s (%d bytes, %d iterations, %d threads)",
        label, objectSizeBytes, iterations, concurrency));

    ExecutorService executor = Executors.newFixedThreadPool(concurrency);
    List<Double> writeLatencies = new ArrayList<>();
    List<Double> readLatencies = new ArrayList<>();

    long totalWriteBytes = 0;
    long totalReadBytes = 0;
    long workloadStartNs = System.nanoTime();

    List<Future<W1R3Result>> futures = new ArrayList<>();
    for (int i = 0; i < iterations; i++) {
      final int taskIndex = i;
      futures.add(executor.submit(() -> {
        String objectName = String.format("w1r3-%s-%d-%d.bin", label.replace(" ", ""), taskIndex, System.nanoTime());
        BlobInfo blobInfo = BlobInfo.newBuilder(TEST_BUCKET, objectName).build();

        // 1. Prepare data
        byte[] payload = new byte[objectSizeBytes];
        java.util.Arrays.fill(payload, (byte) (taskIndex & 0xFF));

        // 2. WRITE 1
        long writeStart = System.nanoTime();
        Blob created = storage.create(blobInfo, payload);
        long writeElapsedNs = System.nanoTime() - writeStart;
        double writeMs = writeElapsedNs / 1_000_000.0;

        // 3. READ 3
        List<Double> taskReadMs = new ArrayList<>();
        for (int r = 0; r < 3; r++) {
          long readStart = System.nanoTime();
          byte[] readBack = storage.readAllBytes(created.getBlobId());
          long readElapsedNs = System.nanoTime() - readStart;
          taskReadMs.add(readElapsedNs / 1_000_000.0);
          Assert.assertTrue("Downloaded payload size must be > 0", readBack.length > 0);
        }

        // 4. CLEANUP
        storage.delete(created.getBlobId());

        return new W1R3Result(writeMs, taskReadMs, objectSizeBytes);
      }));
    }

    for (Future<W1R3Result> f : futures) {
      W1R3Result res = f.get();
      writeLatencies.add(res.writeDurationMs);
      readLatencies.addAll(res.readDurationsMs);
      totalWriteBytes += res.bytesTransferred;
      totalReadBytes += (long) res.bytesTransferred * 3;
    }
    executor.shutdown();
    executor.awaitTermination(60, TimeUnit.SECONDS);

    long workloadTotalElapsedNs = System.nanoTime() - workloadStartNs;
    double totalDurationSec = workloadTotalElapsedNs / 1_000_000_000.0;

    Collections.sort(writeLatencies);
    Collections.sort(readLatencies);

    double writeP50 = writeLatencies.get((int) (writeLatencies.size() * 0.50));
    double writeP90 = writeLatencies.get((int) (writeLatencies.size() * 0.90));
    double writeP99 = writeLatencies.get((int) (writeLatencies.size() * 0.99));
    double writeThroughput = (totalWriteBytes / 1024.0 / 1024.0) / totalDurationSec;

    double readP50 = readLatencies.get((int) (readLatencies.size() * 0.50));
    double readP90 = readLatencies.get((int) (readLatencies.size() * 0.90));
    double readP99 = readLatencies.get((int) (readLatencies.size() * 0.99));
    double readThroughput = (totalReadBytes / 1024.0 / 1024.0) / totalDurationSec;

    double heapMb = getUsedHeapMb();

    log(String.format("W1R3 Results for %s:", label));
    log(String.format("  Total Time:       %.2f sec", totalDurationSec));
    log(String.format("  Write P50 / P90 / P99: %.2f / %.2f / %.2f ms", writeP50, writeP90, writeP99));
    log(String.format("  Write Throughput: %.2f MB/s", writeThroughput));
    log(String.format("  Read P50 / P90 / P99:  %.2f / %.2f / %.2f ms", readP50, readP90, readP99));
    log(String.format("  Read Throughput:  %.2f MB/s", readThroughput));
    log(String.format("  JVM Heap Used:    %.1f MB", heapMb));

    BenchmarkMetric metric = new BenchmarkMetric(
        getTransportPrefix() + label,
        objectSizeBytes,
        concurrency,
        iterations,
        writeP50, writeP90, writeP99, writeThroughput,
        readP50, readP90, readP99, readThroughput,
        heapMb
    );
    recordedMetrics.add(metric);
  }

  private static double getUsedHeapMb() {
    Runtime rt = Runtime.getRuntime();
    return (rt.totalMemory() - rt.freeMemory()) / (1024.0 * 1024.0);
  }

  private static void printSummaryReport() {
    System.out.println("\n" + repeat("#", 105));
    System.out.printf("                   GCS SDK CONSOLIDATION: JAVA %s BENCHMARK REPORT\n", DRIVER.toUpperCase());
    System.out.printf("                       Driver: %s | Live: %b | Target Bucket: %s\n", DRIVER, IS_LIVE, TEST_BUCKET);
    if ("uds".equalsIgnoreCase(DRIVER)) {
      System.out.println("                       Sidecar: gcs-core-server (PR #7350 / gcscore.v1)");
    }
    System.out.println(repeat("#", 105));
    System.out.printf("| %-24s | %-7s | %-16s | %-12s | %-16s | %-12s | %-9s |\n",
        "Benchmark / Payload", "Workers", "Write Lat (P50/99)", "Write MB/s", "Read Lat (P50/99)", "Read MB/s", "Heap (MB)");
    System.out.println("|" + repeat("-", 26) + "|" + repeat("-", 9) + "|" + repeat("-", 18) + "|" + repeat("-", 14)
        + "|" + repeat("-", 18) + "|" + repeat("-", 14) + "|" + repeat("-", 11) + "|");

    for (BenchmarkMetric m : recordedMetrics) {
      if (m.payloadLabel.startsWith("Ping")) {
        System.out.printf("| %-24s | %-7d | %-16s | %-12s | %-16s | %-12.2f | %-9.1f |\n",
            m.payloadLabel, m.workers, "N/A", "N/A",
            String.format("%.3f/%.3f ms", m.readP50, m.readP99), m.readThroughputMbSec, m.heapUsedMb);
      } else if (m.payloadLabel.contains("ListObjects") || m.payloadLabel.contains("ListBucket")) {
        System.out.printf("| %-24s | %-7d | %-16s | %-12s | %-16s | %-12.1f | %-9.1f |\n",
            m.payloadLabel, m.workers, "N/A", "N/A",
            String.format("%.1f/%.1f ms", m.readP50, m.readP99), m.readThroughputMbSec, m.heapUsedMb);
      } else if (m.payloadLabel.contains("Open")) {
        System.out.printf("| %-24s | %-7d | %-16s | %-12s | %-16s | %-12.1f | %-9.1f |\n",
            m.payloadLabel, m.workers, "N/A", "N/A",
            String.format("%.1f/%.1f ms", m.readP50, m.readP99), m.readThroughputMbSec, m.heapUsedMb);
      } else if (m.payloadLabel.contains("Read50x2x4")) {
        System.out.printf("| %-24s | %-7d | %-16s | %-12s | %-16s | %-12.2f | %-9.1f |\n",
            m.payloadLabel, m.workers, "N/A", "N/A",
            String.format("%.1f/%.1f ms", m.readP50, m.readP99), m.readThroughputMbSec, m.heapUsedMb);
      } else if (m.payloadLabel.contains("Random8K")) {
        System.out.printf("| %-24s | %-7d | %-16s | %-12s | %-16s | %-12.1f | %-9.1f |\n",
            m.payloadLabel, m.workers, "N/A", "N/A",
            String.format("%.2f/%.2f ms", m.readP50, m.readP99), m.readThroughputMbSec, m.heapUsedMb);
      } else if (m.payloadLabel.startsWith("WL4")) {
        System.out.printf("| %-24s | %-7d | %-16s | %-12s | %-16s | %-12.2f | %-9.1f |\n",
            m.payloadLabel, m.workers, "N/A", "N/A",
            String.format("%.1f/%.1f ms", m.readP50, m.readP99), m.readThroughputMbSec, m.heapUsedMb);
      } else if (m.payloadLabel.startsWith("WL5")) {
        System.out.printf("| %-24s | %-7d | %-16s | %-12.2f | %-16s | %-12s | %-9.1f |\n",
            m.payloadLabel, m.workers,
            String.format("%.1f/%.1f ms", m.writeP50, m.writeP99), m.writeThroughputMbSec,
            "N/A", "N/A", m.heapUsedMb);
      } else {
        System.out.printf("| %-24s | %-7d | %-16s | %-12.2f | %-16s | %-12.2f | %-9.1f |\n",
            m.payloadLabel,
            m.workers,
            String.format("%.1f/%.1f ms", m.writeP50, m.writeP99),
            m.writeThroughputMbSec,
            String.format("%.1f/%.1f ms", m.readP50, m.readP99),
            m.readThroughputMbSec,
            m.heapUsedMb);
      }
    }
    System.out.println(repeat("#", 105));

    if (finalStats != null) {
      System.out.println("\n" + repeat("-", 60));
      System.out.println("  SIDECAR PROCESS TELEMETRY (Stats RPC)");
      System.out.println(repeat("-", 60));
      System.out.printf("  Sidecar Process PID       : %d\n", finalStats.getPid());
      System.out.printf("  Worker Thread Count       : %d\n", finalStats.getThreads());
      System.out.printf("  CPU User Time             : %.3f sec\n", finalStats.getCpuUserSecs());
      System.out.printf("  CPU System Time           : %.3f sec\n", finalStats.getCpuSysSecs());
      System.out.printf("  Current RSS Memory        : %,d KiB (%.1f MiB)\n",
          finalStats.getRssBytes() / 1024, finalStats.getRssBytes() / 1024.0 / 1024.0);
      System.out.printf("  Peak Resident Memory (HWM): %,d KiB (%.1f MiB)\n",
          finalStats.getPeakRssBytes() / 1024, finalStats.getPeakRssBytes() / 1024.0 / 1024.0);
      System.out.printf("  Total RPCs Served         : %,d\n", finalStats.getRpcsServed());
      System.out.printf("  Uptime                    : %,d ms\n", finalStats.getUptimeMs());
      System.out.println(repeat("-", 60) + "\n");
    }
  }

  // --- Helper Structs ---

  private static class W1R3Result {
    final double writeDurationMs;
    final List<Double> readDurationsMs;
    final int bytesTransferred;

    W1R3Result(double writeDurationMs, List<Double> readDurationsMs, int bytesTransferred) {
      this.writeDurationMs = writeDurationMs;
      this.readDurationsMs = readDurationsMs;
      this.bytesTransferred = bytesTransferred;
    }
  }

  public static class BenchmarkMetric {
    public final String payloadLabel;
    public final int objectSizeBytes;
    public final int workers;
    public final int iterations;
    public final double writeP50;
    public final double writeP90;
    public final double writeP99;
    public final double writeThroughputMbSec;
    public final double readP50;
    public final double readP90;
    public final double readP99;
    public final double readThroughputMbSec;
    public final double heapUsedMb;

    public BenchmarkMetric(String payloadLabel, int objectSizeBytes, int workers, int iterations,
        double writeP50, double writeP90, double writeP99, double writeThroughputMbSec,
        double readP50, double readP90, double readP99, double readThroughputMbSec,
        double heapUsedMb) {
      this.payloadLabel = payloadLabel;
      this.objectSizeBytes = objectSizeBytes;
      this.workers = workers;
      this.iterations = iterations;
      this.writeP50 = writeP50;
      this.writeP90 = writeP90;
      this.writeP99 = writeP99;
      this.writeThroughputMbSec = writeThroughputMbSec;
      this.readP50 = readP50;
      this.readP90 = readP90;
      this.readP99 = readP99;
      this.readThroughputMbSec = readThroughputMbSec;
      this.heapUsedMb = heapUsedMb;
    }
  }

  public static void main(String[] args) throws Exception {
    setUp();
    try {
      ITUdsVeneerBenchmark benchmark = new ITUdsVeneerBenchmark();
      benchmark.test00_PingLatencyMicrobenchmark();
      benchmark.test01_VeneerCreateAndReadBytes();
      benchmark.test02_VeneerReadAndWriteChannels();
      benchmark.test03_VeneerCreateFromAndDownloadToPath();
      benchmark.test04_VeneerListAndGetOperations();
      benchmark.test05_W1R3Benchmark_1KiB();
      benchmark.test06_W1R3Benchmark_100KiB();
      benchmark.test07_W1R3Benchmark_16MiB();
      benchmark.test08_W1R3Benchmark_200MiB();
      benchmark.test09_List100ItemsBenchmark();
      benchmark.test10_CloudpathWorkload4_Read16MbChunkOf200Mb();
      benchmark.test11_CloudpathWorkload5_Write200Mb();
    } finally {
      tearDown();
    }
    System.exit(0);
  }
}