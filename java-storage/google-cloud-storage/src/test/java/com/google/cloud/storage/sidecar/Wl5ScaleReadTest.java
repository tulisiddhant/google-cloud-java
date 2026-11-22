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
import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkConfig;
import com.google.cloud.storage.sidecar.benchmark.ClientProvider;
import com.google.cloud.storage.sidecar.benchmark.Wl5ScaleReadWorkload;
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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assume;
import org.junit.Test;

/** Step 5 ({@code WL5}: {@code Scale Read}) 48-thread concurrency and UDS verification tests. */
public class Wl5ScaleReadTest {

  private static final Context.Key<String> SESSION_CTX_KEY = Context.key("x-session");
  private static final long OBJECT_SIZE_200_MIB = 200L * 1024L * 1024L;

  @Test
  public void applicabilityAndGeometryConstantsMatchSpec() {
    Wl5ScaleReadWorkload wl5 = new Wl5ScaleReadWorkload();
    assertThat(wl5.id()).isEqualTo("WL5");
    assertThat(Wl5ScaleReadWorkload.DEFAULT_WORKER_THREADS).isEqualTo(48);
    assertThat(Wl5ScaleReadWorkload.DEFAULT_CHUNK_BYTES).isEqualTo(16 * 1024 * 1024);
    assertThat(Wl5ScaleReadWorkload.DEFAULT_CHUNKS_PER_OBJECT).isEqualTo(12);
    assertThat(Wl5ScaleReadWorkload.DEFAULT_WARMUP_SEC).isEqualTo(15L);
    assertThat(Wl5ScaleReadWorkload.DEFAULT_DURATION_SEC).isEqualTo(300L);
    assertThat(Wl5ScaleReadWorkload.objectNameForWorker(0)).isEqualTo("bench-200mb-0.bin");
    assertThat(Wl5ScaleReadWorkload.objectNameForWorker(47)).isEqualTo("bench-200mb-47.bin");

    assertThat(wl5.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl5.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.ZONAL)).isTrue();
    assertThat(wl5.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl5.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.ZONAL)).isFalse();
    assertThrows(
        IllegalArgumentException.class,
        () ->
            BenchmarkConfig.newBuilder()
                .setDriver(BenchmarkConfig.DriverType.NATIVE)
                .setProtocol(UdsTransport.JSON)
                .setBucketType(BenchmarkConfig.BucketType.ZONAL)
                .build());
  }

  @Test
  public void wl5GrpcOpens48DistinctObjectsStepsAcross12ChunksAndUsesWallClockActiveSec()
      throws Exception {
    int testChunkBytes = 32 * 1024;
    HermeticScaleReadServer grpcServer = new HermeticScaleReadServer(testChunkBytes, 4L);
    BenchmarkConfig grpcCfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.UDS_SIDECAR)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.ZONAL)
            .build();

    try (UdsStorage grpcUds =
            UdsStorage.newBuilder(syntheticStorageOptions())
                .setTransport(UdsTransport.GRPC)
                .setServerRuntime(grpcServer)
                .build();
        ClientProvider grpcProvider = ClientProvider.forSingleClient(grpcCfg, grpcUds)) {
      // 48 workers, 14 chunks per worker (exercises 0..11 + wraparound 0, 1)
      Wl5ScaleReadWorkload wl5 = new Wl5ScaleReadWorkload(48, testChunkBytes, 12, 0L, 0L, 14);
      WorkloadResult grpcRes = wl5.execute(grpcProvider, grpcCfg);

      Set<String> expectedNames = new HashSet<String>(48);
      for (int w = 0; w < 48; w++) {
        expectedNames.add("bench-200mb-" + w + ".bin");
      }
      assertThat(grpcServer.openedObjectNames).isEqualTo(expectedNames);
      assertThat(grpcServer.openCalls.get()).isEqualTo(48);
      assertThat(grpcServer.closeCalls.get()).isEqualTo(48);
      assertThat(grpcServer.activeHandles).isEmpty();
      assertThat(grpcRes.completedOps()).isEqualTo(48L * 14L);
      assertThat(grpcRes.totalBytes()).isEqualTo(48L * 14L * testChunkBytes);

      // Verify chunk offset progression 0..11 and wraparound back to 0, 1 on worker 0's handle
      Long handle0 = grpcServer.objectToHandle.get("bench-200mb-0.bin");
      List<Long> offsets0 = grpcServer.handleOffsets.get(handle0);
      assertThat(offsets0).hasSize(14);
      for (int idx = 0; idx < 14; idx++) {
        assertThat(offsets0.get(idx)).isEqualTo((idx % 12) * (long) testChunkBytes);
      }

      // Prove activeSec is outer wall-clock duration, NOT sum of 48 parallel thread latencies:
      // 48 workers * 14 ops * >=4ms = >=2.68s summed thread time, while outer wall-clock is < 1.5s.
      double minPossibleThreadSumSec = (48.0 * 14.0 * 4.0) / 1000.0;
      assertThat(grpcRes.activeSec()).isLessThan(minPossibleThreadSumSec * 0.6);
    }
  }

  @Test
  public void wl5JsonEnforcesMandatoryRangeLimitAcross48WorkersAndDistributes16ClientPool()
      throws Exception {
    int testChunkBytes = 32 * 1024;
    HermeticScaleReadServer jsonServer = new HermeticScaleReadServer(testChunkBytes, 2L);
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
      Wl5ScaleReadWorkload wl5 = new Wl5ScaleReadWorkload(48, testChunkBytes, 12, 0L, 0L, 4);
      WorkloadResult jsonRes = wl5.execute(jsonProvider, jsonCfg);

      Set<String> expectedNames = new HashSet<String>(48);
      for (int w = 0; w < 48; w++) {
        expectedNames.add("bench-200mb-" + w + ".bin");
      }
      assertThat(jsonServer.jsonReadObjectNames).isEqualTo(expectedNames);
      assertThat(jsonServer.openCalls.get()).isEqualTo(0);
      assertThat(jsonServer.readObjectCalls.get()).isEqualTo(48 * 4);
      assertThat(jsonServer.unboundedReadObjectDetected.get()).isEqualTo(0);
      assertThat(jsonRes.completedOps()).isEqualTo(48L * 4L);
      assertThat(jsonRes.totalBytes()).isEqualTo(48L * 4L * testChunkBytes);
    }

    // Verify 48 workers distribute evenly across a 16-client Native pool (3 workers per client)
    BenchmarkConfig nativeCfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.NATIVE)
            .setProtocol(UdsTransport.JSON)
            .setBucketType(BenchmarkConfig.BucketType.REGIONAL)
            .build();
    List<Storage> pool = new ArrayList<Storage>(16);
    List<AtomicInteger> clientCounts = new ArrayList<AtomicInteger>(16);
    for (int i = 0; i < 16; i++) {
      AtomicInteger counter = new AtomicInteger(0);
      clientCounts.add(counter);
      pool.add(newCountingStorage(counter));
    }
    try (ClientProvider nativeProvider = ClientProvider.forClientPool(nativeCfg, pool)) {
      Wl5ScaleReadWorkload wl5 = new Wl5ScaleReadWorkload(48, testChunkBytes, 12, 0L, 0L, 2);
      WorkloadResult res = wl5.execute(nativeProvider, nativeCfg);
      assertThat(res.completedOps()).isEqualTo(96L);
      for (int i = 0; i < 16; i++) {
        // 3 workers mapped via (w % 16) * 2 ops each = 6 ops per client
        assertThat(clientCounts.get(i).get()).isEqualTo(6);
      }
    }
  }

  @Test
  public void liveSidecarWl5SmokeAgainstRealBucketsWhenEnabled() throws Exception {
    Assume.assumeTrue(
        "Pass -Duds.live.gcs=true on GCE VM to run live GCS WL5 smoke test",
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
      Wl5ScaleReadWorkload smoke =
          new Wl5ScaleReadWorkload(4, 16 * 1024 * 1024, 12, 1L, 2L, 1);
      WorkloadResult res = smoke.execute(provider, cfg);
      assertThat(res.completedOps()).isAtLeast(4L);
      assertThat(res.decimalMbPerSec()).isGreaterThan(0.0);
    }
  }

  private static Storage newCountingStorage(final AtomicInteger counter) {
    InvocationHandler handler =
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) {
            if ("reader".equals(method.getName())) {
              counter.incrementAndGet();
              return new BoundedSyntheticReadChannel();
            }
            if ("close".equals(method.getName())) {
              return null;
            }
            throw new UnsupportedOperationException("Unexpected call: " + method.getName());
          }
        };
    return (Storage)
        Proxy.newProxyInstance(
            Storage.class.getClassLoader(), new Class<?>[] {Storage.class}, handler);
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

  private static final class BoundedSyntheticReadChannel implements ReadChannel {
    private long pos = 0L;
    private long endLimit = Long.MAX_VALUE;
    private boolean open = true;

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void seek(long position) {
      this.pos = position;
    }

    @Override
    public ReadChannel limit(long limit) {
      this.endLimit = limit;
      return this;
    }

    @Override
    public long limit() {
      return endLimit;
    }

    @Override
    public void setChunkSize(int chunkSize) {}

    @Override
    public RestorableState<ReadChannel> capture() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int read(ByteBuffer dst) {
      if (endLimit == Long.MAX_VALUE) {
        throw new AssertionError("WL5 JSON ReadChannel.limit(...) was omitted");
      }
      long rem = endLimit - pos;
      if (rem <= 0L) {
        return -1;
      }
      int n = (int) Math.min(dst.remaining(), rem);
      dst.position(dst.position() + n);
      pos += n;
      return n;
    }

    @Override
    public void close() {
      open = false;
    }
  }

  private static final class HermeticScaleReadServer extends GcsCoreGrpc.GcsCoreImplBase
      implements UdsServerRuntime {
    private final String socketPath = UdsServerRuntime.newUniqueSocketPath();
    private final int expectedChunkBytes;
    private final ByteString chunkSlice;
    private final long opDelayMs;
    private final AtomicLong nextHandle = new AtomicLong(2000L);
    private final Set<String> validSessions = ConcurrentHashMap.newKeySet();

    final Set<Long> activeHandles = ConcurrentHashMap.newKeySet();
    final Set<String> openedObjectNames = ConcurrentHashMap.newKeySet();
    final Set<String> jsonReadObjectNames = ConcurrentHashMap.newKeySet();
    final Map<String, Long> objectToHandle = new ConcurrentHashMap<String, Long>();
    final Map<Long, List<Long>> handleOffsets = new ConcurrentHashMap<Long, List<Long>>();
    final AtomicInteger openCalls = new AtomicInteger(0);
    final AtomicInteger closeCalls = new AtomicInteger(0);
    final AtomicInteger readObjectCalls = new AtomicInteger(0);
    final AtomicInteger unboundedReadObjectDetected = new AtomicInteger(0);

    private EpollEventLoopGroup bossGroup;
    private EpollEventLoopGroup workerGroup;
    private ExecutorService rpcExecutor;
    private Server server;

    HermeticScaleReadServer(int expectedChunkBytes, long opDelayMs) {
      this.expectedChunkBytes = expectedChunkBytes;
      this.chunkSlice = ByteString.copyFrom(new byte[expectedChunkBytes]);
      this.opDelayMs = opDelayMs;
    }

    @Override
    public void start() throws IOException {
      UdsServerRuntime.unlinkSocketFiles(socketPath);
      bossGroup = new EpollEventLoopGroup(1, new DefaultThreadFactory("wl5-boss", true));
      workerGroup = new EpollEventLoopGroup(4, new DefaultThreadFactory("wl5-worker", true));
      rpcExecutor =
          Executors.newFixedThreadPool(48, new DefaultThreadFactory("wl5-rpc-exec", true));

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
              .executor(rpcExecutor)
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
      return OptionalLong.of(56789L);
    }

    @Override
    public void close() {
      if (server != null) {
        server.shutdownNow();
      }
      if (rpcExecutor != null) {
        rpcExecutor.shutdownNow();
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
      responseObserver.onNext(StatsResponse.newBuilder().setPid(56789).setRssBytes(8192L).build());
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
      openCalls.incrementAndGet();
      long h = nextHandle.incrementAndGet();
      activeHandles.add(h);
      openedObjectNames.add(request.getObject());
      objectToHandle.put(request.getObject(), h);
      handleOffsets.put(h, Collections.synchronizedList(new ArrayList<Long>()));
      ObjectMeta meta =
          ObjectMeta.newBuilder().setSize(OBJECT_SIZE_200_MIB).setGeneration(1L).build();
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
      if (request.getLength() != expectedChunkBytes) {
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Unexpected length").asRuntimeException());
        return;
      }
      sleepQuietly(opDelayMs);
      handleOffsets.get(request.getHandle()).add(request.getOffset());
      responseObserver.onNext(ReadChunk.newBuilder().addData(chunkSlice).build());
      responseObserver.onCompleted();
    }

    @Override
    public void readObject(ReadObjectRequest request, StreamObserver<ReadChunk> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      if (request.getTransport() != Transport.JSON) {
        responseObserver.onError(Status.INVALID_ARGUMENT.asRuntimeException());
        return;
      }
      if (request.getLength() != expectedChunkBytes) {
        unboundedReadObjectDetected.incrementAndGet();
        responseObserver.onError(
            Status.INVALID_ARGUMENT.withDescription("Missing limit()").asRuntimeException());
        return;
      }
      sleepQuietly(opDelayMs);
      readObjectCalls.incrementAndGet();
      jsonReadObjectNames.add(request.getObject());
      responseObserver.onNext(ReadChunk.newBuilder().addData(chunkSlice).build());
      responseObserver.onCompleted();
    }

    private static void sleepQuietly(long ms) {
      if (ms <= 0L) {
        return;
      }
      try {
        Thread.sleep(ms);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
