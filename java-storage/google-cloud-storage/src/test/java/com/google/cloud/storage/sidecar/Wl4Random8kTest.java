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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkConfig;
import com.google.cloud.storage.sidecar.benchmark.ClientProvider;
import com.google.cloud.storage.sidecar.benchmark.Wl4Random8kWorkload;
import com.google.cloud.storage.sidecar.benchmark.WorkloadResult;
import com.google.cloud.storage.sidecar.proto.CloseObjectRequest;
import com.google.cloud.storage.sidecar.proto.CreateSessionRequest;
import com.google.cloud.storage.sidecar.proto.CreateSessionResponse;
import com.google.cloud.storage.sidecar.proto.Empty;
import com.google.cloud.storage.sidecar.proto.GcsCoreGrpc;
import com.google.cloud.storage.sidecar.proto.ObjectMeta;
import com.google.cloud.storage.sidecar.proto.OpenObjectRequest;
import com.google.cloud.storage.sidecar.proto.OpenObjectResponse;
import com.google.cloud.storage.sidecar.proto.PingRequest;
import com.google.cloud.storage.sidecar.proto.PingResponse;
import com.google.cloud.storage.sidecar.proto.ReadChunk;
import com.google.cloud.storage.sidecar.proto.ReadObjectRequest;
import com.google.cloud.storage.sidecar.proto.ReadRangeRequest;
import com.google.cloud.storage.sidecar.proto.SessionRef;
import com.google.cloud.storage.sidecar.proto.StatsRequest;
import com.google.cloud.storage.sidecar.proto.StatsResponse;
import com.google.cloud.storage.sidecar.proto.Transport;
import com.google.protobuf.ByteString;
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
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assume;
import org.junit.Test;

/** Step 4 ({@code WL4}: {@code Random8K}) unit and UDS protocol verification tests. */
public class Wl4Random8kTest {

  private static final Context.Key<String> SESSION_CTX_KEY = Context.key("x-session");

  @Test
  public void applicabilityConstantsAndDeterministicPrngMatchSpec() {
    Wl4Random8kWorkload wl4 = new Wl4Random8kWorkload();
    assertThat(wl4.id()).isEqualTo("WL4");
    assertThat(Wl4Random8kWorkload.DEFAULT_OBJECT_SIZE_BYTES).isEqualTo(104_857_600L);
    assertThat(Wl4Random8kWorkload.DEFAULT_SEEK_BYTES).isEqualTo(8_192);
    assertThat(Wl4Random8kWorkload.DEFAULT_MAX_OFFSET_INCLUSIVE).isEqualTo(104_849_408L);
    assertThat(Wl4Random8kWorkload.PRNG_SEED).isEqualTo(42L);
    assertThat(Wl4Random8kWorkload.DEFAULT_WARMUP_SEEKS_GRPC).isEqualTo(50);
    assertThat(Wl4Random8kWorkload.DEFAULT_WARMUP_SEEKS_JSON).isEqualTo(5);
    assertThat(Wl4Random8kWorkload.DEFAULT_MIN_SEEKS).isEqualTo(2_000);

    assertThat(wl4.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl4.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.ZONAL)).isTrue();
    assertThat(wl4.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl4.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.ZONAL)).isFalse();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BenchmarkConfig.newBuilder()
                .setDriver(BenchmarkConfig.DriverType.NATIVE)
                .setProtocol(UdsTransport.JSON)
                .setBucketType(BenchmarkConfig.BucketType.ZONAL)
                .build());

    Random rngA = new Random(Wl4Random8kWorkload.PRNG_SEED);
    Random rngB = new Random(Wl4Random8kWorkload.PRNG_SEED);
    for (int i = 0; i < 10_000; i++) {
      long offA =
          Wl4Random8kWorkload.nextUniformOffset(
              rngA, Wl4Random8kWorkload.DEFAULT_MAX_OFFSET_INCLUSIVE);
      long offB =
          Wl4Random8kWorkload.nextUniformOffset(
              rngB, Wl4Random8kWorkload.DEFAULT_MAX_OFFSET_INCLUSIVE);
      assertThat(offA).isEqualTo(offB);
      assertThat(offA).isAtLeast(0L);
      assertThat(offA).isAtMost(Wl4Random8kWorkload.DEFAULT_MAX_OFFSET_INCLUSIVE);
    }
  }

  @Test
  public void wl4GrpcReusesOnePersistentSessionAcross50WarmupAnd2000Timed8KibSeeks()
      throws Exception {
    HermeticRandom8kServer grpcServer = new HermeticRandom8kServer();
    BenchmarkConfig grpcCfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.UDS_SIDECAR)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.ZONAL)
            .build();

    List<Long> grpcTimedOffsets;
    try (UdsStorage grpcUds =
            UdsStorage.newBuilder(syntheticStorageOptions())
                .setTransport(UdsTransport.GRPC)
                .setServerRuntime(grpcServer)
                .build();
        ClientProvider grpcProvider = ClientProvider.forSingleClient(grpcCfg, grpcUds)) {
      Wl4Random8kWorkload wl4 = new Wl4Random8kWorkload(50, 5, 2_000, 0L);
      WorkloadResult grpcRes = wl4.execute(grpcProvider, grpcCfg);

      // Must reuse 1 persistent BlobReadSession across all 50 warmup + 2,000 timed seeks = 2,050
      assertThat(grpcServer.openCalls.get()).isEqualTo(1);
      assertThat(grpcServer.closeCalls.get()).isEqualTo(1);
      assertThat(grpcServer.activeHandles).isEmpty();
      assertThat(grpcServer.readRangeRequests.size()).isEqualTo(2_050);

      assertThat(grpcRes.completedOps()).isEqualTo(2_000L);
      assertThat(grpcRes.totalBytes()).isEqualTo(2_000L * 8_192L);
      assertThat(grpcRes.opsPerSec()).isGreaterThan(0.0);
      assertThat(grpcRes.decimalMbPerSec()).isGreaterThan(0.0);

      grpcTimedOffsets = new ArrayList<Long>(2_000);
      Random expectedRng = new Random(Wl4Random8kWorkload.PRNG_SEED);
      for (int i = 0; i < 2_000; i++) {
        ReadRangeRequest req = grpcServer.readRangeRequests.get(50 + i);
        long expectedOffset =
            Wl4Random8kWorkload.nextUniformOffset(
                expectedRng, Wl4Random8kWorkload.DEFAULT_MAX_OFFSET_INCLUSIVE);
        assertThat(req.getHandle()).isGreaterThan(0L);
        assertThat(req.getLength()).isEqualTo(8_192L);
        assertThat(req.getOffset()).isEqualTo(expectedOffset);
        grpcTimedOffsets.add(req.getOffset());
      }
    }

    // Verify JSON executes 5 untimed warmup seeks and hits the exact same seed-42 timed offsets
    HermeticRandom8kServer jsonServer = new HermeticRandom8kServer();
    BenchmarkConfig jsonCfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.UDS_SIDECAR)
            .setProtocol(UdsTransport.JSON)
            .setBucketType(BenchmarkConfig.BucketType.REGIONAL)
            .build();

    try (UdsStorage jsonUds =
            UdsStorage.newBuilder(syntheticStorageOptions())
                .setTransport(UdsTransport.JSON)
                .setServerRuntime(jsonServer)
                .build();
        ClientProvider jsonProvider = ClientProvider.forSingleClient(jsonCfg, jsonUds)) {
      Wl4Random8kWorkload wl4 = new Wl4Random8kWorkload(50, 5, 250, 0L);
      WorkloadResult jsonRes = wl4.execute(jsonProvider, jsonCfg);

      assertThat(jsonServer.openCalls.get()).isEqualTo(0);
      assertThat(jsonServer.readObjectRequests.size()).isEqualTo(255);
      assertThat(jsonRes.completedOps()).isEqualTo(250L);
      assertThat(jsonRes.totalBytes()).isEqualTo(250L * 8_192L);

      for (int i = 0; i < 250; i++) {
        ReadObjectRequest req = jsonServer.readObjectRequests.get(5 + i);
        assertThat(req.getObject()).isEqualTo(Wl4Random8kWorkload.OBJECT_NAME);
        assertThat(req.getTransport()).isEqualTo(Transport.JSON);
        assertThat(req.getLength()).isEqualTo(8_192L);
        assertThat(req.getOffset()).isEqualTo(grpcTimedOffsets.get(i));
      }
    }
  }

  @Test
  public void liveSidecarWl4SmokeAgainstRealBucketsWhenEnabled() throws Exception {
    Assume.assumeTrue(
        "Pass -Duds.live.gcs=true on GCE VM to run live GCS WL4 smoke test",
        Boolean.getBoolean("uds.live.gcs"));
    String bin = System.getProperty("uds.sidecar.bin", "");
    Assume.assumeFalse("uds.sidecar.bin is required", bin.isEmpty());

    BenchmarkConfig cfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.UDS_SIDECAR)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.ZONAL)
            .setSidecarBinaryPath(bin)
            .build();
    try (ClientProvider provider = ClientProvider.create(cfg)) {
      Wl4Random8kWorkload smoke = new Wl4Random8kWorkload(2, 1, 5, 0L);
      WorkloadResult res = smoke.execute(provider, cfg);
      assertThat(res.completedOps()).isEqualTo(5L);
      assertThat(res.totalBytes()).isEqualTo(5L * 8_192L);
    }
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

  /** Local {@code AF_UNIX} {@code GcsCore} test endpoint for deterministic 8 KiB seek checks. */
  private static final class HermeticRandom8kServer extends GcsCoreGrpc.GcsCoreImplBase
      implements UdsServerRuntime {
    private static final ByteString PAYLOAD_8KIB = ByteString.copyFrom(new byte[8 * 1024]);

    private final String socketPath = UdsServerRuntime.newUniqueSocketPath();
    private final AtomicLong nextHandle = new AtomicLong(1200L);
    private final Set<String> validSessions = ConcurrentHashMap.newKeySet();

    final Set<Long> activeHandles = ConcurrentHashMap.newKeySet();
    final AtomicInteger openCalls = new AtomicInteger(0);
    final AtomicInteger closeCalls = new AtomicInteger(0);
    final List<ReadRangeRequest> readRangeRequests =
        Collections.synchronizedList(new ArrayList<ReadRangeRequest>());
    final List<ReadObjectRequest> readObjectRequests =
        Collections.synchronizedList(new ArrayList<ReadObjectRequest>());

    private EpollEventLoopGroup bossGroup;
    private EpollEventLoopGroup workerGroup;
    private Server server;

    @Override
    public void start() throws IOException {
      UdsServerRuntime.unlinkSocketFiles(socketPath);
      bossGroup = new EpollEventLoopGroup(1, new DefaultThreadFactory("wl4-boss", true));
      workerGroup = new EpollEventLoopGroup(2, new DefaultThreadFactory("wl4-worker", true));

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
              .maxInboundMessageSize(64 * 1024 * 1024)
              .initialFlowControlWindow(64 * 1024 * 1024)
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
      return OptionalLong.of(45678L);
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
      responseObserver.onNext(StatsResponse.newBuilder().setPid(45678).setRssBytes(8192L).build());
      responseObserver.onCompleted();
    }

    @Override
    public void shutdown(Empty request, StreamObserver<Empty> responseObserver) {
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void openObject(
        OpenObjectRequest request, StreamObserver<OpenObjectResponse> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      assertThat(request.getObject()).isEqualTo(Wl4Random8kWorkload.OBJECT_NAME);
      openCalls.incrementAndGet();
      long h = nextHandle.incrementAndGet();
      activeHandles.add(h);
      ObjectMeta meta =
          ObjectMeta.newBuilder()
              .setSize(Wl4Random8kWorkload.DEFAULT_OBJECT_SIZE_BYTES)
              .setGeneration(1L)
              .build();
      responseObserver.onNext(OpenObjectResponse.newBuilder().setHandle(h).setMeta(meta).build());
      responseObserver.onCompleted();
    }

    @Override
    public void closeObject(CloseObjectRequest request, StreamObserver<Empty> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      closeCalls.incrementAndGet();
      activeHandles.remove(request.getHandle());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void readRange(ReadRangeRequest request, StreamObserver<ReadChunk> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      if (!activeHandles.contains(request.getHandle())) {
        responseObserver.onError(Status.NOT_FOUND.asRuntimeException());
        return;
      }
      readRangeRequests.add(request);
      int len = (int) request.getLength();
      ByteString slice = (len == 8192) ? PAYLOAD_8KIB : ByteString.copyFrom(new byte[len]);
      responseObserver.onNext(ReadChunk.newBuilder().addData(slice).build());
      responseObserver.onCompleted();
    }

    @Override
    public void readObject(ReadObjectRequest request, StreamObserver<ReadChunk> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      readObjectRequests.add(request);
      int len = (int) request.getLength();
      ByteString slice = (len == 8192) ? PAYLOAD_8KIB : ByteString.copyFrom(new byte[len]);
      responseObserver.onNext(ReadChunk.newBuilder().addData(slice).build());
      responseObserver.onCompleted();
    }
  }
}
