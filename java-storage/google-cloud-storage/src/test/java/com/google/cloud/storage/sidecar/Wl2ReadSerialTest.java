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
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkConfig;
import com.google.cloud.storage.sidecar.benchmark.ClientProvider;
import com.google.cloud.storage.sidecar.benchmark.Wl2ReadSerialWorkload;
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
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;

/** Step 2 ({@code WL2}: {@code Read50x2x4 Serial}) unit and UDS protocol verification tests. */
public class Wl2ReadSerialTest {

  private static final Context.Key<String> SESSION_CTX_KEY = Context.key("x-session");
  private static final long OBJECT_SIZE_200_MIB = 200L * 1024L * 1024L;

  @Test
  public void applicabilityMatchesSection3Matrix() {
    Wl2ReadSerialWorkload wl2 = new Wl2ReadSerialWorkload();
    assertThat(wl2.id()).isEqualTo("WL2");
    assertThat(wl2.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl2.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.ZONAL)).isTrue();
    assertThat(wl2.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl2.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.ZONAL)).isFalse();
  }

  @Test
  public void udsBlobReadSessionReadRangeReassemblesMultiChunkMultiSlicePayload() throws Exception {
    FakeReadGcsCoreRuntime server = new FakeReadGcsCoreRuntime(true, 0L);
    try (UdsStorage uds =
        UdsStorage.newBuilder(syntheticStorageOptions())
            .setTransport(UdsTransport.GRPC)
            .setServerRuntime(server)
            .build()) {
      BlobId blobId = BlobId.of("regional-bkt", Wl2ReadSerialWorkload.OBJECT_NAME);
      try (BlobReadSession session = uds.blobReadSession(blobId).get(5, TimeUnit.SECONDS)) {
        long offset = 4L * 1024L * 1024L;
        int len = 2 * 1024 * 1024;
        byte[] actual =
            session
                .readAs(
                    ReadProjectionConfigs.asFutureBytes()
                        .withRangeSpec(RangeSpec.of(offset, len)))
                .get(5, TimeUnit.SECONDS);
        assertThat(actual.length).isEqualTo(len);
        assertPatternMatches(actual, offset);
      }
      assertThat(server.activeHandles).isEmpty();
    }
  }

  @Test
  public void udsReadChannelHonorsSeekLimitAndCancelsServerStreamOnEarlyClose() throws Exception {
    FakeReadGcsCoreRuntime server = new FakeReadGcsCoreRuntime(true, 0L);
    try (UdsStorage uds =
        UdsStorage.newBuilder(syntheticStorageOptions())
            .setTransport(UdsTransport.JSON)
            .setServerRuntime(server)
            .build()) {
      BlobId blobId = BlobId.of("regional-bkt", Wl2ReadSerialWorkload.OBJECT_NAME, 55L);
      long offset = 8L * 1024L * 1024L;
      int len = 512 * 1024;

      byte[] received = new byte[len];
      int pos = 0;
      try (ReadChannel r =
          uds.reader(
              blobId,
              Storage.BlobSourceOption.generationMatch(),
              Storage.BlobSourceOption.shouldReturnRawInputStream(true))) {
        r.setChunkSize(0);
        r.seek(offset);
        r.limit(offset + len);
        ByteBuffer buf = ByteBuffer.allocate(64 * 1024);
        int n;
        while ((n = r.read(buf)) != -1) {
          buf.flip();
          buf.get(received, pos, n);
          pos += n;
          buf.clear();
        }
      }
      assertThat(pos).isEqualTo(len);
      assertPatternMatches(received, offset);

      ReadObjectRequest captured = server.readObjectRequests.get(0);
      assertThat(captured.getTransport()).isEqualTo(Transport.JSON);
      assertThat(captured.getOffset()).isEqualTo(offset);
      assertThat(captured.getLength()).isEqualTo(len);
      assertThat(captured.getGeneration()).isEqualTo(55L);
      assertThat(captured.hasIfGenerationMatch()).isTrue();
      assertThat(captured.getIfGenerationMatch()).isEqualTo(55L);

      // Verify closing ReadChannel mid-stream without limit() cancels the UDS server stream
      server.streamInfiniteUntilCancelled = true;
      try (ReadChannel unbounded =
          uds.reader(BlobId.of("regional-bkt", Wl2ReadSerialWorkload.OBJECT_NAME))) {
        ByteBuffer small = ByteBuffer.allocate(4096);
        int readBytes = unbounded.read(small);
        assertThat(readBytes).isEqualTo(4096);
      }
      assertThat(server.readStreamCancelledLatch.await(3, TimeUnit.SECONDS)).isTrue();
    }
  }

  @Test
  public void wl2GrpcAndJsonExecuteFull50x2x4PassGeometryAndExcludeOpenCloseFromGrpcTimer()
      throws Exception {
    // Inject 60 ms on OpenObject + 60 ms on CloseObject (= 120 ms untimed per pass, 240 ms across
    // 2 timed passes) to prove WL2 gRPC pass timer excludes session open/close from activeSec.
    FakeReadGcsCoreRuntime grpcServer = new FakeReadGcsCoreRuntime(false, 60L);
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
      Wl2ReadSerialWorkload wl2 = new Wl2ReadSerialWorkload(1, 2, 0L);
      WorkloadResult grpcRes = wl2.execute(grpcProvider, grpcCfg);

      // 1 warmup + 2 timed = 3 sessions opened & closed, 3 * 50 = 150 range reads
      assertThat(grpcServer.openCalls.get()).isEqualTo(3);
      assertThat(grpcServer.closeCalls.get()).isEqualTo(3);
      assertThat(grpcServer.activeHandles).isEmpty();
      assertThat(grpcServer.readRangeRequests.size()).isEqualTo(150);
      assertThat(grpcServer.readRangeRequests.get(0).getOffset()).isEqualTo(0L);
      assertThat(grpcServer.readRangeRequests.get(49).getOffset()).isEqualTo(196L * 1024L * 1024L);
      assertThat(grpcServer.readRangeRequests.get(49).getLength()).isEqualTo(2L * 1024L * 1024L);

      assertThat(grpcRes.completedOps()).isEqualTo(2L);
      assertThat(grpcRes.totalBytes())
          .isEqualTo(2L * Wl2ReadSerialWorkload.EXPECTED_DEFAULT_PASS_BYTES);
      double outerWallMs =
          Duration.between(grpcRes.wlStartUtc(), grpcRes.wlEndUtc()).toNanos() / 1_000_000.0;
      double excludedLifecycleMs = outerWallMs - (grpcRes.activeSec() * 1000.0);
      assertThat(excludedLifecycleMs).isAtLeast(200.0);
      assertThat(grpcRes.decimalMbPerSec()).isGreaterThan(0.0);
    }

    FakeReadGcsCoreRuntime jsonServer = new FakeReadGcsCoreRuntime(false, 0L);
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
      Wl2ReadSerialWorkload wl2 = new Wl2ReadSerialWorkload(1, 2, 0L);
      WorkloadResult jsonRes = wl2.execute(jsonProvider, jsonCfg);

      assertThat(jsonServer.openCalls.get()).isEqualTo(0);
      assertThat(jsonServer.readObjectRequests.size()).isEqualTo(150);
      assertThat(jsonServer.readObjectRequests.get(0).getOffset()).isEqualTo(0L);
      assertThat(jsonServer.readObjectRequests.get(49).getOffset()).isEqualTo(196L * 1024L * 1024L);
      assertThat(jsonServer.readObjectRequests.get(49).getLength()).isEqualTo(2L * 1024L * 1024L);
      assertThat(jsonServer.readObjectRequests.get(49).getTransport()).isEqualTo(Transport.JSON);

      assertThat(jsonRes.completedOps()).isEqualTo(2L);
      assertThat(jsonRes.totalBytes()).isEqualTo(2L * Wl2ReadSerialWorkload.EXPECTED_DEFAULT_PASS_BYTES);
    }
  }

  private static void assertPatternMatches(byte[] actual, long startOffset) {
    // Check boundaries and strided interior positions for deterministic pattern integrity.
    for (int i = 0; i < actual.length; i += 4093) {
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

  /** Hermetic Netty AF_UNIX server implementing {@code OpenObject}, {@code ReadRange}, and {@code ReadObject}. */
  private static final class FakeReadGcsCoreRuntime extends GcsCoreGrpc.GcsCoreImplBase
      implements UdsServerRuntime {
    private static final ByteString FAST_ZERO_1MIB = ByteString.copyFrom(new byte[1024 * 1024]);

    private final String socketPath = UdsServerRuntime.newUniqueSocketPath();
    private final boolean fillDeterministicPattern;
    private final long openCloseDelayMs;
    private final AtomicLong nextHandle = new AtomicLong(500L);
    private final Set<String> validSessions = ConcurrentHashMap.newKeySet();
    final Set<Long> activeHandles = ConcurrentHashMap.newKeySet();
    final AtomicInteger openCalls = new AtomicInteger(0);
    final AtomicInteger closeCalls = new AtomicInteger(0);
    final List<ReadRangeRequest> readRangeRequests =
        Collections.synchronizedList(new ArrayList<ReadRangeRequest>());
    final List<ReadObjectRequest> readObjectRequests =
        Collections.synchronizedList(new ArrayList<ReadObjectRequest>());
    final CountDownLatch readStreamCancelledLatch = new CountDownLatch(1);
    volatile boolean streamInfiniteUntilCancelled = false;

    private EpollEventLoopGroup bossGroup;
    private EpollEventLoopGroup workerGroup;
    private Server server;

    FakeReadGcsCoreRuntime(boolean fillDeterministicPattern, long openCloseDelayMs) {
      this.fillDeterministicPattern = fillDeterministicPattern;
      this.openCloseDelayMs = openCloseDelayMs;
    }

    @Override
    public void start() throws IOException {
      UdsServerRuntime.unlinkSocketFiles(socketPath);
      bossGroup = new EpollEventLoopGroup(1, new DefaultThreadFactory("wl2-boss", true));
      workerGroup = new EpollEventLoopGroup(2, new DefaultThreadFactory("wl2-worker", true));

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
      return OptionalLong.of(23456L);
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
      responseObserver.onNext(StatsResponse.newBuilder().setPid(23456).setRssBytes(8192L).build());
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
        responseObserver.onError(Status.NOT_FOUND.withDescription("unknown handle").asRuntimeException());
        return;
      }
      readRangeRequests.add(request);
      long len =
          request.getLength() < 0
              ? (OBJECT_SIZE_200_MIB - request.getOffset())
              : request.getLength();
      streamPayload(request.getOffset(), len, false, responseObserver);
    }

    @Override
    public void readObject(
        ReadObjectRequest request, StreamObserver<ReadChunk> responseObserver) {
      if (!requireSession(responseObserver)) {
        return;
      }
      ServerCallStreamObserver<ReadChunk> serverObs =
          (ServerCallStreamObserver<ReadChunk>) responseObserver;
      serverObs.setOnCancelHandler(readStreamCancelledLatch::countDown);

      if (streamInfiniteUntilCancelled) {
        // Send one 64 KiB chunk and leave stream open so client close() must explicitly cancel it.
        ReadChunk first =
            ReadChunk.newBuilder()
                .addData(ByteString.copyFrom(new byte[64 * 1024]))
                .setMeta(ObjectMeta.newBuilder().setSize(OBJECT_SIZE_200_MIB).build())
                .build();
        serverObs.onNext(first);
        return;
      }

      readObjectRequests.add(request);
      long len =
          request.getLength() < 0
              ? (OBJECT_SIZE_200_MIB - request.getOffset())
              : request.getLength();
      streamPayload(request.getOffset(), len, true, responseObserver);
    }

    private void streamPayload(
        long startOffset,
        long length,
        boolean includeFirstMeta,
        StreamObserver<ReadChunk> observer) {
      if (!fillDeterministicPattern && length == 2L * 1024L * 1024L) {
        ReadChunk.Builder chunk =
            ReadChunk.newBuilder().addData(FAST_ZERO_1MIB).addData(FAST_ZERO_1MIB);
        if (includeFirstMeta) {
          chunk.setMeta(ObjectMeta.newBuilder().setSize(OBJECT_SIZE_200_MIB).build());
        }
        observer.onNext(chunk.build());
        observer.onCompleted();
        return;
      }

      long remaining = length;
      long curOffset = startOffset;
      boolean first = true;
      int maxChunkBytes = 512 * 1024;
      int sliceBytes = 128 * 1024;

      while (remaining > 0) {
        int thisChunk = (int) Math.min(remaining, maxChunkBytes);
        ReadChunk.Builder cb = ReadChunk.newBuilder();
        if (first && includeFirstMeta) {
          cb.setMeta(ObjectMeta.newBuilder().setSize(OBJECT_SIZE_200_MIB).build());
        }
        first = false;

        int built = 0;
        while (built < thisChunk) {
          int sl = Math.min(sliceBytes, thisChunk - built);
          byte[] arr = new byte[sl];
          if (fillDeterministicPattern) {
            for (int i = 0; i < sl; i++) {
              arr[i] = expectedByteAt(curOffset + i);
            }
          }
          cb.addData(ByteString.copyFrom(arr));
          curOffset += sl;
          built += sl;
        }
        observer.onNext(cb.build());
        remaining -= thisChunk;
      }
      observer.onCompleted();
    }

    private static void sleepQuietly(long ms) {
      if (ms <= 0) {
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
