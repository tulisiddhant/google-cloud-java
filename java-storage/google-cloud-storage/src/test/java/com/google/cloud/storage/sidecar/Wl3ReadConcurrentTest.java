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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkConfig;
import com.google.cloud.storage.sidecar.benchmark.ClientProvider;
import com.google.cloud.storage.sidecar.benchmark.Wl3ReadConcurrentWorkload;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/** Step 3 ({@code WL3}: {@code Read50x2x4 Concurrent}) unit and UDS concurrency tests. */
public class Wl3ReadConcurrentTest {

  private static final Context.Key<String> SESSION_CTX_KEY = Context.key("x-session");
  private static final long OBJECT_SIZE_200_MIB = 200L * 1024L * 1024L;

  @Test
  public void applicabilityMatchesSection3Matrix() {
    Wl3ReadConcurrentWorkload wl3 = new Wl3ReadConcurrentWorkload();
    assertThat(wl3.id()).isEqualTo("WL3");
    assertThat(wl3.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl3.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.ZONAL)).isTrue();
    assertThat(wl3.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl3.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.ZONAL)).isFalse();
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
  public void udsBlobReadSessionMultiplexes50ConcurrentRangeReadsWithoutSliceInterleaving()
      throws Exception {
    FakeConcurrentReadRuntime server = new FakeConcurrentReadRuntime(true, 0L, 15L);
    try (UdsStorage uds =
        UdsStorage.newBuilder(syntheticStorageOptions())
            .setTransport(UdsTransport.GRPC)
            .setServerRuntime(server)
            .build()) {
      BlobId blobId = BlobId.of("zonal-bkt", Wl3ReadConcurrentWorkload.OBJECT_NAME);
      int rangeCount = 50;
      int rangeLen = 128 * 1024;
      long stride = 4L * 1024L * 1024L;

      try (BlobReadSession session = uds.blobReadSession(blobId).get(5, TimeUnit.SECONDS)) {
        List<ApiFuture<byte[]>> futures = new ArrayList<ApiFuture<byte[]>>(rangeCount);
        for (int i = 0; i < rangeCount; i++) {
          long offset = i * stride;
          futures.add(
              session.readAs(
                  ReadProjectionConfigs.asFutureBytes()
                      .withRangeSpec(RangeSpec.of(offset, rangeLen))));
        }
        List<byte[]> results = ApiFutures.allAsList(futures).get(10, TimeUnit.SECONDS);
        assertThat(results.size()).isEqualTo(rangeCount);
        for (int i = 0; i < rangeCount; i++) {
          byte[] data = results.get(i);
          assertThat(data.length).isEqualTo(rangeLen);
          assertPatternMatches(data, i * stride);
        }
      }
      assertThat(server.maxConcurrentReadRanges.get()).isGreaterThan(4);
      assertThat(server.activeHandles).isEmpty();
    }
  }

  @Test
  public void wl3GrpcExecutesConcurrent50x2x4OnOneSessionPerPassAndExcludesOpenClose()
      throws Exception {
    // Inject 60 ms on OpenObject + 60 ms on CloseObject (= 120 ms untimed per pass, 240 ms across
    // 2 timed passes) and 8 ms per ReadRange on an async server executor so 50 concurrent ranges
    // overlap cleanly on 1 session.
    FakeConcurrentReadRuntime grpcServer = new FakeConcurrentReadRuntime(false, 60L, 8L);
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
      Wl3ReadConcurrentWorkload wl3 = new Wl3ReadConcurrentWorkload(1, 2, 0L);
      WorkloadResult grpcRes = wl3.execute(grpcProvider, grpcCfg);

      assertThat(grpcServer.openCalls.get()).isEqualTo(3);
      assertThat(grpcServer.closeCalls.get()).isEqualTo(3);
      assertThat(grpcServer.activeHandles).isEmpty();
      assertThat(grpcServer.readRangeCount.get()).isEqualTo(150);
      assertThat(grpcServer.maxConcurrentReadRanges.get()).isGreaterThan(4);

      assertThat(grpcRes.completedOps()).isEqualTo(2L);
      assertThat(grpcRes.totalBytes())
          .isEqualTo(2L * Wl3ReadConcurrentWorkload.EXPECTED_DEFAULT_PASS_BYTES);
      double outerWallMs =
          Duration.between(grpcRes.wlStartUtc(), grpcRes.wlEndUtc()).toNanos() / 1_000_000.0;
      double excludedLifecycleMs = outerWallMs - (grpcRes.activeSec() * 1000.0);
      assertThat(excludedLifecycleMs).isAtLeast(200.0);
      assertThat(grpcRes.decimalMbPerSec()).isGreaterThan(0.0);
    }
  }

  @Test
  public void wl3JsonDispatches50RangesAcross16ThreadPoolAndBinds1To1With16ClientPool()
      throws Exception {
    // 1. Verify UdsStorage(Transport.JSON) over UDS with 16-thread concurrent ReadChannel fan-out
    FakeConcurrentReadRuntime jsonServer = new FakeConcurrentReadRuntime(false, 0L, 8L);
    BenchmarkConfig udsJsonCfg =
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
        ClientProvider jsonProvider = ClientProvider.forSingleClient(udsJsonCfg, jsonUds)) {
      Wl3ReadConcurrentWorkload wl3 = new Wl3ReadConcurrentWorkload(1, 2, 0L);
      WorkloadResult jsonRes = wl3.execute(jsonProvider, udsJsonCfg);

      assertThat(jsonServer.openCalls.get()).isEqualTo(0);
      assertThat(jsonServer.readObjectCount.get()).isEqualTo(150);
      assertThat(jsonServer.lastReadObjectTransport).isEqualTo(Transport.JSON);
      assertThat(jsonServer.maxConcurrentReadObjects.get()).isGreaterThan(2);
      assertThat(jsonServer.maxConcurrentReadObjects.get()).isAtMost(16);
      assertThat(jsonRes.completedOps()).isEqualTo(2L);
      assertThat(jsonRes.totalBytes())
          .isEqualTo(2L * Wl3ReadConcurrentWorkload.EXPECTED_DEFAULT_PASS_BYTES);
    }

    // 2. Verify 16-client Native pool affinity: all 16 clients participate and each client has at
    // most 1 concurrent active request at any instant (1-to-1 workerThread -> clientForWorker).
    BenchmarkConfig nativeJsonCfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.NATIVE)
            .setProtocol(UdsTransport.JSON)
            .setBucketType(BenchmarkConfig.BucketType.REGIONAL)
            .build();
    List<Storage> pool = new ArrayList<Storage>(16);
    List<AtomicInteger> perClientCalls = new ArrayList<AtomicInteger>(16);
    List<AtomicInteger> perClientMaxInflight = new ArrayList<AtomicInteger>(16);
    for (int c = 0; c < 16; c++) {
      AtomicInteger calls = new AtomicInteger(0);
      AtomicInteger maxInflight = new AtomicInteger(0);
      perClientCalls.add(calls);
      perClientMaxInflight.add(maxInflight);
      pool.add(newCountingReadStorage(calls, maxInflight, 8L));
    }

    try (ClientProvider nativeProvider = ClientProvider.forClientPool(nativeJsonCfg, pool)) {
      Wl3ReadConcurrentWorkload wl3 =
          new Wl3ReadConcurrentWorkload(0, 1, 0L, 50, 64 * 1024, 4L * 1024L * 1024L, 16);
      WorkloadResult res = wl3.execute(nativeProvider, nativeJsonCfg);
      assertThat(res.completedOps()).isEqualTo(1L);
      assertThat(res.totalBytes()).isEqualTo(50L * 64 * 1024);

      int totalCalls = 0;
      for (int c = 0; c < 16; c++) {
        assertThat(perClientCalls.get(c).get()).isGreaterThan(0);
        assertThat(perClientMaxInflight.get(c).get()).isEqualTo(1);
        totalCalls += perClientCalls.get(c).get();
      }
      assertThat(totalCalls).isEqualTo(50);
    }
  }

  private static Storage newCountingReadStorage(
      final AtomicInteger callCounter, final AtomicInteger maxInflight, final long readDelayMs) {
    final AtomicInteger currentInflight = new AtomicInteger(0);
    InvocationHandler handler =
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) {
            String name = method.getName();
            if ("reader".equals(name)) {
              callCounter.incrementAndGet();
              int active = currentInflight.incrementAndGet();
              updateMax(maxInflight, active);
              return new SyntheticRangeReadChannel(currentInflight, readDelayMs);
            }
            if ("close".equals(name)) {
              return null;
            }
            throw new UnsupportedOperationException("Unexpected Storage call: " + name);
          }
        };
    return (Storage)
        Proxy.newProxyInstance(
            Storage.class.getClassLoader(), new Class<?>[] {Storage.class}, handler);
  }

  private static void updateMax(AtomicInteger maxHolder, int candidate) {
    while (true) {
      int prev = maxHolder.get();
      if (candidate <= prev || maxHolder.compareAndSet(prev, candidate)) {
        return;
      }
    }
  }

  private static void assertPatternMatches(byte[] actual, long startOffset) {
    for (int i = 0; i < actual.length; i += 2039) {
      byte expected = expectedByteAt(startOffset + i);
      if (actual[i] != expected) {
        throw new AssertionError(
            "Byte mismatch at index " + i + ": expected " + expected + ", got " + actual[i]);
      }
    }
    int last = actual.length - 1;
    assertThat(actual[last]).isEqualTo(expectedByteAt(startOffset + last));
  }

  private static byte expectedByteAt(long absoluteOffset) {
    return (byte) ((absoluteOffset * 31L + 7L) & 0xFFL);
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

  private static final class SyntheticRangeReadChannel implements ReadChannel {
    private final AtomicInteger inflightCounter;
    private final long delayMs;
    private long pos = 0L;
    private long endLimit = Long.MAX_VALUE;
    private boolean open = true;
    private boolean delayedOnce = false;

    SyntheticRangeReadChannel(AtomicInteger inflightCounter, long delayMs) {
      this.inflightCounter = inflightCounter;
      this.delayMs = delayMs;
    }

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
      if (!delayedOnce && delayMs > 0L) {
        delayedOnce = true;
        try {
          Thread.sleep(delayMs);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      long remainingInRange = endLimit - pos;
      if (remainingInRange <= 0L) {
        return -1;
      }
      int toWrite = (int) Math.min(dst.remaining(), remainingInRange);
      dst.position(dst.position() + toWrite);
      pos += toWrite;
      return toWrite;
    }

    @Override
    public void close() {
      if (open) {
        open = false;
        inflightCounter.decrementAndGet();
      }
    }
  }

  /**
   * Hermetic Netty {@code AF_UNIX} {@code GcsCore} server dispatching {@code ReadRange} and {@code
   * ReadObject} onto a 32-thread worker pool so concurrent streams genuinely overlap.
   */
  private static final class FakeConcurrentReadRuntime extends GcsCoreGrpc.GcsCoreImplBase
      implements UdsServerRuntime {
    private static final ByteString FAST_ZERO_1MIB = ByteString.copyFrom(new byte[1024 * 1024]);

    private final String socketPath = UdsServerRuntime.newUniqueSocketPath();
    private final boolean fillPattern;
    private final long openCloseDelayMs;
    private final long streamHoldMs;
    private final AtomicLong nextHandle = new AtomicLong(900L);
    private final Set<String> validSessions = ConcurrentHashMap.newKeySet();

    final Set<Long> activeHandles = ConcurrentHashMap.newKeySet();
    final AtomicInteger openCalls = new AtomicInteger(0);
    final AtomicInteger closeCalls = new AtomicInteger(0);
    final AtomicInteger readRangeCount = new AtomicInteger(0);
    final AtomicInteger readObjectCount = new AtomicInteger(0);
    final AtomicInteger activeReadRanges = new AtomicInteger(0);
    final AtomicInteger maxConcurrentReadRanges = new AtomicInteger(0);
    final AtomicInteger activeReadObjects = new AtomicInteger(0);
    final AtomicInteger maxConcurrentReadObjects = new AtomicInteger(0);
    volatile Transport lastReadObjectTransport = Transport.TRANSPORT_UNSPECIFIED;

    private EpollEventLoopGroup bossGroup;
    private EpollEventLoopGroup workerGroup;
    private ExecutorService rpcExecutor;
    private Server server;

    FakeConcurrentReadRuntime(boolean fillPattern, long openCloseDelayMs, long streamHoldMs) {
      this.fillPattern = fillPattern;
      this.openCloseDelayMs = openCloseDelayMs;
      this.streamHoldMs = streamHoldMs;
    }

    @Override
    public void start() throws IOException {
      UdsServerRuntime.unlinkSocketFiles(socketPath);
      bossGroup = new EpollEventLoopGroup(1, new DefaultThreadFactory("wl3-boss", true));
      workerGroup = new EpollEventLoopGroup(4, new DefaultThreadFactory("wl3-worker", true));
      rpcExecutor =
          Executors.newFixedThreadPool(32, new DefaultThreadFactory("wl3-rpc-exec", true));

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
      return OptionalLong.of(34567L);
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
      responseObserver.onNext(StatsResponse.newBuilder().setPid(34567).setRssBytes(8192L).build());
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
      sleepQuietly(openCloseDelayMs);
      openCalls.incrementAndGet();
      long h = nextHandle.incrementAndGet();
      activeHandles.add(h);
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
      sleepQuietly(openCloseDelayMs);
      closeCalls.incrementAndGet();
      activeHandles.remove(request.getHandle());
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void readRange(
        ReadRangeRequest request, StreamObserver<ReadChunk> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      if (!activeHandles.contains(request.getHandle())) {
        observerError(responseObserver, Status.NOT_FOUND.withDescription("Unknown handle"));
        return;
      }
      readRangeCount.incrementAndGet();
      int active = activeReadRanges.incrementAndGet();
      updateMax(maxConcurrentReadRanges, active);
      try {
        sleepQuietly(streamHoldMs);
        streamPayload(request.getOffset(), request.getLength(), responseObserver);
      } finally {
        activeReadRanges.decrementAndGet();
      }
    }

    @Override
    public void readObject(
        ReadObjectRequest request, StreamObserver<ReadChunk> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      readObjectCount.incrementAndGet();
      lastReadObjectTransport = request.getTransport();
      int active = activeReadObjects.incrementAndGet();
      updateMax(maxConcurrentReadObjects, active);
      try {
        sleepQuietly(streamHoldMs);
        streamPayload(request.getOffset(), request.getLength(), responseObserver);
      } finally {
        activeReadObjects.decrementAndGet();
      }
    }

    private void streamPayload(
        long startOffset, long reqLen, StreamObserver<ReadChunk> responseObserver) {
      long endOffset =
          (reqLen < 0)
              ? OBJECT_SIZE_200_MIB
              : Math.min(OBJECT_SIZE_200_MIB, startOffset + reqLen);
      long pos = startOffset;
      while (pos < endOffset) {
        int chunkLen = (int) Math.min(1024L * 1024L, endOffset - pos);
        ReadChunk.Builder chunkB = ReadChunk.newBuilder();
        if (!fillPattern && chunkLen == 1024 * 1024) {
          chunkB.addData(FAST_ZERO_1MIB);
        } else if (!fillPattern) {
          chunkB.addData(FAST_ZERO_1MIB.substring(0, chunkLen));
        } else {
          // Split into 2 slices per chunk to exercise multi-slice assembly under concurrency
          int half = chunkLen / 2;
          chunkB.addData(buildPatternSlice(pos, half));
          chunkB.addData(buildPatternSlice(pos + half, chunkLen - half));
        }
        responseObserver.onNext(chunkB.build());
        pos += chunkLen;
      }
      responseObserver.onCompleted();
    }

    private static ByteString buildPatternSlice(long startOffset, int len) {
      byte[] buf = new byte[len];
      for (int i = 0; i < len; i++) {
        buf[i] = expectedByteAt(startOffset + i);
      }
      return ByteString.copyFrom(buf);
    }

    private static void observerError(StreamObserver<?> obs, Status status) {
      obs.onError(status.asRuntimeException());
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
