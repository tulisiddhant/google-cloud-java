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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkConfig;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkReportCli;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkReportCli.CloudMetrics;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkReportCli.LiveCloudMonitoringProvider;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkSuiteRunner;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkSuiteRunner.SuiteRunRecord;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkSuiteRunner.WorkloadRecord;
import com.google.cloud.storage.sidecar.benchmark.ClientProvider;
import com.google.cloud.storage.sidecar.benchmark.Wl7aListObjectsWorkload;
import com.google.cloud.storage.sidecar.benchmark.Wl7bListBucketsWorkload;
import com.google.cloud.storage.sidecar.benchmark.Workload;
import com.google.cloud.storage.sidecar.benchmark.WorkloadResult;
import com.google.cloud.storage.sidecar.proto.CreateSessionRequest;
import com.google.cloud.storage.sidecar.proto.CreateSessionResponse;
import com.google.cloud.storage.sidecar.proto.Empty;
import com.google.cloud.storage.sidecar.proto.GcsCoreGrpc;
import com.google.cloud.storage.sidecar.proto.ListBucketsRequest;
import com.google.cloud.storage.sidecar.proto.ListBucketsResponse;
import com.google.cloud.storage.sidecar.proto.ListObjectsRequest;
import com.google.cloud.storage.sidecar.proto.ListObjectsResponse;
import com.google.cloud.storage.sidecar.proto.ListedBucket;
import com.google.cloud.storage.sidecar.proto.ListedObject;
import com.google.cloud.storage.sidecar.proto.PingRequest;
import com.google.cloud.storage.sidecar.proto.PingResponse;
import com.google.cloud.storage.sidecar.proto.SessionRef;
import com.google.cloud.storage.sidecar.proto.StatsRequest;
import com.google.cloud.storage.sidecar.proto.StatsResponse;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

/**
 * Step 7 verification tests covering {@code UdsStorage.list(...)}, {@code WL7a}/{@code WL7b},
 * {@link BenchmarkSuiteRunner}, and {@link BenchmarkReportCli}.
 */
public class Wl7ListAndRunnerTest {

  private static final Context.Key<String> SESSION_CTX_KEY = Context.key("x-session");

  @Test
  public void udsListObjectsAndBucketsAndWl7aWl7bSinglePageSemanticsMatchSpec() throws Exception {
    HermeticListServer server = new HermeticListServer(100, 68);

    // 1. JSON transport must reject control-plane list() calls per Section 3 & 4
    try (UdsStorage jsonUds =
        UdsStorage.newBuilder(syntheticStorageOptions())
            .setTransport(UdsTransport.JSON)
            .setServerRuntime(server)
            .build()) {
      assertThrows(
          UnsupportedOperationException.class,
          () -> jsonUds.list("regional-bkt", Storage.BlobListOption.pageSize(100)));
      assertThrows(
          UnsupportedOperationException.class,
          () -> jsonUds.list(Storage.BucketListOption.pageSize(100)));
    }

    // 2. gRPC UdsStorage.list(...) + WL7a + WL7b single-page getValues() semantics
    HermeticListServer grpcServer = new HermeticListServer(100, 68);
    BenchmarkConfig grpcCfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.UDS_SIDECAR)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.REGIONAL)
            .build();

    try (UdsStorage grpcUds =
            UdsStorage.newBuilder(syntheticStorageOptions())
                .setTransport(UdsTransport.GRPC)
                .setServerRuntime(grpcServer)
                .build();
        ClientProvider provider = ClientProvider.forSingleClient(grpcCfg, grpcUds)) {
      // WL7a: 5 warmup + 10 timed single-page calls (even with next_page_token set on page 1,
      // page.getValues() must NEVER auto-fetch page 2!)
      grpcServer.failIfPageTokenPresent = true;
      Wl7aListObjectsWorkload wl7a = new Wl7aListObjectsWorkload(5, 10, 0L);
      WorkloadResult res7a = wl7a.execute(provider, grpcCfg);
      assertThat(res7a.completedOps()).isEqualTo(10L);
      assertThat(res7a.totalBytes()).isEqualTo(0L);
      assertThat(res7a.opsPerSec()).isGreaterThan(0.0);
      assertThat(grpcServer.listObjectsCalls.get()).isEqualTo(15);
      ListObjectsRequest lastObjReq = grpcServer.lastListObjectsReq;
      assertThat(lastObjReq.getPrefix()).isEqualTo("list-bench-100/");
      assertThat(lastObjReq.getPageSize()).isEqualTo(100);

      // Verify explicit Page.getNextPage() pagination works when caller requests it
      grpcServer.failIfPageTokenPresent = false;
      Page<Blob> firstPage =
          grpcUds.list(
              "regional-bkt",
              Storage.BlobListOption.prefix("list-bench-100/"),
              Storage.BlobListOption.pageSize(100));
      assertThat(firstPage.hasNextPage()).isTrue();
      Page<Blob> secondPage = firstPage.getNextPage();
      assertThat(secondPage.hasNextPage()).isFalse();
      int page2Count = 0;
      for (Blob b : secondPage.getValues()) {
        assertThat(b.isDirectory()).isTrue();
        page2Count++;
      }
      assertThat(page2Count).isEqualTo(1);

      // WL7b: 5 warmup + 10 timed single-page calls returning 68 project buckets
      Wl7bListBucketsWorkload wl7b = new Wl7bListBucketsWorkload(5, 10, 0L);
      WorkloadResult res7b = wl7b.execute(provider, grpcCfg);
      assertThat(res7b.completedOps()).isEqualTo(10L);
      assertThat(res7b.totalBytes()).isEqualTo(0L);
      assertThat(grpcServer.listBucketsCalls.get()).isEqualTo(15);
      assertThat(grpcServer.lastListBucketsReq.getProject()).isEqualTo("vaibhavpratap-sdk-test");
      assertThat(grpcServer.lastListBucketsReq.getPageSize()).isEqualTo(100);

      Page<Bucket> bucketPage = grpcUds.list(Storage.BucketListOption.pageSize(100));
      assertThat(bucketPage.hasNextPage()).isFalse();
    }
  }

  @Test
  public void suiteRunnerMatchesSection3MatrixEnforcesQuiescenceAndPersistsSuiteJson()
      throws Exception {
    List<Workload> defaults = BenchmarkSuiteRunner.defaultWorkloads();

    BenchmarkConfig grpcRegional =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.NATIVE)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.REGIONAL)
            .build();
    assertThat(idsOf(BenchmarkSuiteRunner.selectApplicableWorkloads(defaults, grpcRegional)))
        .containsExactly("WL1", "WL2", "WL3", "WL4", "WL5", "WL7a", "WL7b")
        .inOrder();

    BenchmarkConfig grpcZonal =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.NATIVE)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.ZONAL)
            .build();
    assertThat(idsOf(BenchmarkSuiteRunner.selectApplicableWorkloads(defaults, grpcZonal)))
        .containsExactly("WL1", "WL2", "WL3", "WL4", "WL5", "WL6", "WL7a", "WL7b")
        .inOrder();

    BenchmarkConfig jsonRegional =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.NATIVE)
            .setProtocol(UdsTransport.JSON)
            .setBucketType(BenchmarkConfig.BucketType.REGIONAL)
            .build();
    assertThat(idsOf(BenchmarkSuiteRunner.selectApplicableWorkloads(defaults, jsonRegional)))
        .containsExactly("WL2", "WL3", "WL4", "WL5", "WL6")
        .inOrder();

    // Verify inter-workload quiescence sleep and JSON serialization round-trip
    HermeticListServer server = new HermeticListServer(100, 68);
    BenchmarkConfig runCfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.UDS_SIDECAR)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.REGIONAL)
            .setQuiescenceSec(1)
            .build();
    List<Workload> miniSuite =
        Arrays.<Workload>asList(
            new Wl7aListObjectsWorkload(1, 2, 0L), new Wl7bListBucketsWorkload(1, 2, 0L));

    Path tmpJson = Files.createTempFile("uds-suite-test-", ".json");
    try (UdsStorage uds =
            UdsStorage.newBuilder(syntheticStorageOptions())
                .setTransport(UdsTransport.GRPC)
                .setServerRuntime(server)
                .build();
        ClientProvider provider = ClientProvider.forSingleClient(runCfg, uds)) {
      SuiteRunRecord rec = BenchmarkSuiteRunner.executeSuite(runCfg, provider, miniSuite);
      BenchmarkSuiteRunner.writeSuiteJson(tmpJson, rec);
      SuiteRunRecord loaded = BenchmarkSuiteRunner.readSuiteJson(tmpJson);

      assertThat(loaded.driver).isEqualTo("uds-sidecar");
      assertThat(loaded.workloads.keySet()).containsExactly("WL7a", "WL7b").inOrder();
      Instant wl7aEnd = Instant.parse(loaded.workloads.get("WL7a").wlEndUtc);
      Instant wl7bStart = Instant.parse(loaded.workloads.get("WL7b").wlStartUtc);
      assertThat(wl7bStart).isAtLeast(wl7aEnd.plusMillis(950));
    } finally {
      Files.deleteIfExists(tmpJson);
    }
  }

  @Test
  public void reportCliAppliesExactCloudMonitoringWindowsAndRendersVerbatimSection62Tables()
      throws Exception {
    // 1. Verify LiveCloudMonitoringProvider window boundaries:
    //    CPU:    (wlStart + 60s, wlEnd] -> MEAN * 100.0
    //    Memory: [wlStart, wlEnd + 60s] -> MAX
    final Instant wlStart = Instant.parse("2026-11-22T12:00:00Z");
    final Instant wlEnd = Instant.parse("2026-11-22T12:03:00Z");

    HttpTransport stubMonitoringTransport =
        new HttpTransport() {
          @Override
          protected LowLevelHttpRequest buildRequest(String method, final String url) {
            return new LowLevelHttpRequest() {
              @Override
              public void addHeader(String name, String value) {}

              @Override
              public LowLevelHttpResponse execute() throws IOException {
                String decoded = URLDecoder.decode(url, "UTF-8");
                final String body;
                if (decoded.contains(BenchmarkReportCli.CPU_METRIC_TYPE)) {
                  // Include boundary points at wlStart (0.90) and wlStart+60s (0.80) which MUST be
                  // excluded, plus two interior points at +120s (0.20) and +180s (0.30) -> MEAN =
                  // 25.00%
                  body =
                      "{\"timeSeries\":[{\"points\":["
                          + "{\"interval\":{\"endTime\":\"2026-11-22T12:00:00Z\"},"
                          + "\"value\":{\"doubleValue\":0.90}},"
                          + "{\"interval\":{\"endTime\":\"2026-11-22T12:01:00Z\"},"
                          + "\"value\":{\"doubleValue\":0.80}},"
                          + "{\"interval\":{\"endTime\":\"2026-11-22T12:02:00Z\"},"
                          + "\"value\":{\"doubleValue\":0.20}},"
                          + "{\"interval\":{\"endTime\":\"2026-11-22T12:03:00Z\"},"
                          + "\"value\":{\"doubleValue\":0.30}}"
                          + "]}]}";
                } else {
                  assertThat(decoded).contains("metric.labels.state = \"used\"");
                  // Include points at wlStart (2.10%), +120s (3.45%), wlEnd+60s (4.08%), and
                  // out-of-window wlEnd+61s (9.99%) -> MAX over [wlStart, wlEnd+60s] = 4.08%
                  body =
                      "{\"timeSeries\":[{\"points\":["
                          + "{\"interval\":{\"endTime\":\"2026-11-22T12:00:00Z\"},"
                          + "\"value\":{\"doubleValue\":2.10}},"
                          + "{\"interval\":{\"endTime\":\"2026-11-22T12:02:00Z\"},"
                          + "\"value\":{\"doubleValue\":3.45}},"
                          + "{\"interval\":{\"endTime\":\"2026-11-22T12:04:00Z\"},"
                          + "\"value\":{\"doubleValue\":4.08}},"
                          + "{\"interval\":{\"endTime\":\"2026-11-22T12:04:01Z\"},"
                          + "\"value\":{\"doubleValue\":9.99}}"
                          + "]}]}";
                }
                return new LowLevelHttpResponse() {
                  @Override
                  public InputStream getContent() {
                    return new ByteArrayInputStream(body.getBytes(StandardCharsets.UTF_8));
                  }

                  @Override
                  public String getContentEncoding() {
                    return null;
                  }

                  @Override
                  public long getContentLength() {
                    return body.length();
                  }

                  @Override
                  public String getContentType() {
                    return "application/json; charset=UTF-8";
                  }

                  @Override
                  public String getStatusLine() {
                    return "HTTP/1.1 200 OK";
                  }

                  @Override
                  public int getStatusCode() {
                    return 200;
                  }

                  @Override
                  public String getReasonPhrase() {
                    return "OK";
                  }

                  @Override
                  public int getHeaderCount() {
                    return 0;
                  }

                  @Override
                  public String getHeaderName(int index) {
                    return null;
                  }

                  @Override
                  public String getHeaderValue(int index) {
                    return null;
                  }
                };
              }
            };
          }
        };

    LiveCloudMonitoringProvider liveClient =
        new LiveCloudMonitoringProvider(stubMonitoringTransport.createRequestFactory());
    CloudMetrics windowMetrics =
        liveClient.fetchWindowMetrics("vaibhavpratap-sdk-test", "1503915486421500670", wlStart, wlEnd);
    assertThat(windowMetrics.cpuPercent).isWithin(1e-6).of(25.00);
    assertThat(windowMetrics.peakMemoryPercent).isWithin(1e-6).of(4.08);

    // 2. Verify full 13-table Markdown rendering & % Diff rules against Section 6.2 example
    List<SuiteRunRecord> suites = new ArrayList<SuiteRunRecord>();
    suites.add(
        singleWorkloadSuite(
            "native", "GRPC", "regional", "WL2", 4_980_000_000L, 5_460_000_000L, 6_090_000_000L,
            20.87, 19.91, 0.2));
    suites.add(
        singleWorkloadSuite(
            "uds-sidecar",
            "GRPC",
            "regional",
            "WL2",
            5_210_000_000L,
            5_630_000_000L,
            5_720_000_000L,
            20.25,
            19.31,
            0.2));
    suites.add(
        singleWorkloadSuite(
            "native", "GRPC", "zonal", "WL6", 2_050_000_000L, 3_170_000_000L, 4_320_000_000L,
            4514.00, 4304.80, 21.5));
    suites.add(
        singleWorkloadSuite(
            "uds-sidecar", "GRPC", "zonal", "WL6", 4_700_000_000L, 4_910_000_000L, 5_260_000_000L,
            2117.00, 2018.90, 10.1));
    suites.add(
        singleWorkloadSuite(
            "native", "JSON", "regional", "WL2", 7_130_000_000L, 8_180_000_000L, 12_820_000_000L,
            13.60, 12.97, 0.13));
    suites.add(
        singleWorkloadSuite(
            "uds-sidecar",
            "JSON",
            "regional",
            "WL2",
            7_240_000_000L,
            8_210_000_000L,
            12_080_000_000L,
            13.49,
            12.87,
            0.13));

    String md =
        BenchmarkReportCli.renderFullReport(
            suites,
            "vaibhavpratap-sdk-test",
            "1503915486421500670",
            new BenchmarkReportCli.TelemetryProvider() {
              @Override
              public CloudMetrics fetchWindowMetrics(
                  String projectId, String instanceId, Instant start, Instant end) {
                return new CloudMetrics(0.11, 1.07);
              }
            });

    // Verify all 8 Set 1 [gRPC] headings + all 5 Set 2 [JSON] headings + N/A cells + % Diff math
    assertThat(md).contains("### WL 1: Open (200 MiB BidiReadObject Session Handshake) — `[gRPC]`");
    assertThat(md).contains("### WL 6: Scale Write (200 MiB Objects via BidiWriteObject");
    assertThat(md).contains("### WL 7a: ListObjects (100 Objects Under Prefix, Single Page)");
    assertThat(md).contains("### WL 7b: ListBuckets (Project Buckets, pageSize=100, Single Page)");
    assertThat(md).contains("### WL 2: Read50x2x4 Serial (50 \u00d7 2 MiB Ranges at 4 MiB Stride via ReadChannel) — `[JSON]`");
    assertThat(md).contains("`N/A (Regional rejects BidiWriteObject)`");
    assertThat(md).contains("`N/A (Zonal Rapid is gRPC-only)`");
    assertThat(md).contains("`5.21 / 5.63 / 5.72 s` (`+4.6% / +3.1% / -6.1%`)");
    assertThat(md).contains("`2,117.00 MB/s (2,018.90 MiB/s)` (`-53.1%`)");
    assertThat(md).contains("`0.11%` (`0.0%`)");
  }

  private static SuiteRunRecord singleWorkloadSuite(
      String driver,
      String protocol,
      String bucketType,
      String wlId,
      long p50Ns,
      long p90Ns,
      long p99Ns,
      double mbps,
      double mibps,
      double opsPerSec) {
    SuiteRunRecord s = new SuiteRunRecord();
    s.driver = driver;
    s.protocol = protocol;
    s.bucketType = bucketType;
    s.projectId = "vaibhavpratap-sdk-test";
    WorkloadRecord r = new WorkloadRecord();
    r.workloadId = wlId;
    r.wlStartUtc = "2026-11-22T12:00:00Z";
    r.wlEndUtc = "2026-11-22T12:03:00Z";
    r.p50Ns = p50Ns;
    r.p90Ns = p90Ns;
    r.p99Ns = p99Ns;
    r.decimalMbPerSec = mbps;
    r.binaryMibPerSec = mibps;
    r.opsPerSec = opsPerSec;
    s.workloads.put(wlId, r);
    return s;
  }

  private static List<String> idsOf(List<Workload> list) {
    List<String> ids = new ArrayList<String>(list.size());
    for (Workload wl : list) {
      ids.add(wl.id());
    }
    return ids;
  }

  private static StorageOptions syntheticStorageOptions() {
    OAuth2Credentials creds =
        OAuth2Credentials.create(
            new AccessToken("test-lent-token", new Date(System.currentTimeMillis() + 3600_000L)));
    return StorageOptions.grpc()
        .setProjectId("vaibhavpratap-sdk-test")
        .setCredentials(creds)
        .setAttemptDirectPath(false)
        .build();
  }

  private static final class HermeticListServer extends GcsCoreGrpc.GcsCoreImplBase
      implements UdsServerRuntime {
    private final String socketPath = UdsServerRuntime.newUniqueSocketPath();
    private final int objectCount;
    private final int bucketCount;
    private final Set<String> validSessions = ConcurrentHashMap.newKeySet();

    final AtomicInteger listObjectsCalls = new AtomicInteger(0);
    final AtomicInteger listBucketsCalls = new AtomicInteger(0);
    volatile boolean failIfPageTokenPresent = false;
    volatile ListObjectsRequest lastListObjectsReq;
    volatile ListBucketsRequest lastListBucketsReq;

    private EpollEventLoopGroup bossGroup;
    private EpollEventLoopGroup workerGroup;
    private Server server;

    HermeticListServer(int objectCount, int bucketCount) {
      this.objectCount = objectCount;
      this.bucketCount = bucketCount;
    }

    @Override
    public void start() throws IOException {
      UdsServerRuntime.unlinkSocketFiles(socketPath);
      bossGroup = new EpollEventLoopGroup(1, new DefaultThreadFactory("wl7-boss", true));
      workerGroup = new EpollEventLoopGroup(2, new DefaultThreadFactory("wl7-worker", true));

      ServerInterceptor sessionRecorder =
          new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
                ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
              String sid = headers.get(GcsSessionManager.SESSION_HEADER_KEY);
              Context ctx = Context.current().withValue(SESSION_CTX_KEY, sid);
              return Contexts.interceptCall(ctx, call, headers, next);
            }
          };

      server =
          NettyServerBuilder.forAddress(new DomainSocketAddress(socketPath))
              .channelType(EpollServerDomainSocketChannel.class)
              .bossEventLoopGroup(bossGroup)
              .workerEventLoopGroup(workerGroup)
              .addService(ServerInterceptors.intercept(this, sessionRecorder))
              .build()
              .start();
    }

    @Override
    public String socketPath() {
      return socketPath;
    }

    @Override
    public boolean isAlive() {
      return server != null && !server.isShutdown();
    }

    @Override
    public OptionalLong serverPid() {
      return OptionalLong.of(78901L);
    }

    @Override
    public void close() {
      if (server != null) {
        server.shutdownNow();
      }
      if (workerGroup != null) {
        workerGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      }
      if (bossGroup != null) {
        bossGroup.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      }
      UdsServerRuntime.unlinkSocketFiles(socketPath);
    }

    private boolean requireSession(StreamObserver<?> observer) {
      String sid = SESSION_CTX_KEY.get();
      if (sid == null || !validSessions.contains(sid)) {
        observer.onError(Status.UNAUTHENTICATED.asRuntimeException());
        return false;
      }
      return true;
    }

    @Override
    public void ping(PingRequest request, StreamObserver<PingResponse> responseObserver) {
      responseObserver.onNext(PingResponse.newBuilder().setPayload(request.getPayload()).build());
      responseObserver.onCompleted();
    }

    @Override
    public void createSession(
        CreateSessionRequest request, StreamObserver<CreateSessionResponse> responseObserver) {
      String sid = "sess-" + validSessions.size();
      validSessions.add(sid);
      responseObserver.onNext(CreateSessionResponse.newBuilder().setSessionId(sid).build());
      responseObserver.onCompleted();
    }

    @Override
    public void closeSession(SessionRef request, StreamObserver<Empty> responseObserver) {
      validSessions.remove(request.getSessionId());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void stats(StatsRequest request, StreamObserver<StatsResponse> responseObserver) {
      responseObserver.onNext(StatsResponse.newBuilder().setPid(78901).setRssBytes(8192L).build());
      responseObserver.onCompleted();
    }

    @Override
    public void shutdown(Empty request, StreamObserver<Empty> responseObserver) {
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void listObjects(
        ListObjectsRequest request, StreamObserver<ListObjectsResponse> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      listObjectsCalls.incrementAndGet();
      lastListObjectsReq = request;
      if (!request.getPageToken().isEmpty()) {
        if (failIfPageTokenPresent) {
          responseObserver.onError(
              Status.FAILED_PRECONDITION
                  .withDescription("Unexpected second page fetch during single-page WL7a!")
                  .asRuntimeException());
          return;
        }
        responseObserver.onNext(
            ListObjectsResponse.newBuilder().addPrefixes("list-bench-100/subdir/").build());
        responseObserver.onCompleted();
        return;
      }

      ListObjectsResponse.Builder b =
          ListObjectsResponse.newBuilder().setNextPageToken("page2-token");
      for (int i = 0; i < objectCount; i++) {
        b.addItems(
            ListedObject.newBuilder()
                .setName(String.format("list-bench-100/item-%03d.bin", i))
                .setSize(1024L)
                .setGeneration(1L)
                .setMetageneration(1L)
                .setStorageClass("STANDARD")
                .build());
      }
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }

    @Override
    public void listBuckets(
        ListBucketsRequest request, StreamObserver<ListBucketsResponse> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      listBucketsCalls.incrementAndGet();
      lastListBucketsReq = request;
      ListBucketsResponse.Builder b = ListBucketsResponse.newBuilder();
      for (int i = 0; i < bucketCount; i++) {
        b.addItems(
            ListedBucket.newBuilder()
                .setName("bench-bucket-" + i)
                .setLocation("US-WEST1")
                .setStorageClass("STANDARD")
                .setMetageneration(1L)
                .build());
      }
      responseObserver.onNext(b.build());
      responseObserver.onCompleted();
    }
  }
}
