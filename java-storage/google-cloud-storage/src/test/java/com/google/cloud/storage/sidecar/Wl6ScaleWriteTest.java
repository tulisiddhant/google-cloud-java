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
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageChannelUtils;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkConfig;
import com.google.cloud.storage.sidecar.benchmark.ClientProvider;
import com.google.cloud.storage.sidecar.benchmark.Wl6ScaleWriteWorkload;
import com.google.cloud.storage.sidecar.benchmark.WorkloadResult;
import com.google.cloud.storage.sidecar.proto.CreateSessionRequest;
import com.google.cloud.storage.sidecar.proto.CreateSessionResponse;
import com.google.cloud.storage.sidecar.proto.Empty;
import com.google.cloud.storage.sidecar.proto.GcsCoreGrpc;
import com.google.cloud.storage.sidecar.proto.PingRequest;
import com.google.cloud.storage.sidecar.proto.PingResponse;
import com.google.cloud.storage.sidecar.proto.SessionRef;
import com.google.cloud.storage.sidecar.proto.StatsRequest;
import com.google.cloud.storage.sidecar.proto.StatsResponse;
import com.google.cloud.storage.sidecar.proto.Transport;
import com.google.cloud.storage.sidecar.proto.WriteHeader;
import com.google.cloud.storage.sidecar.proto.WriteObjectResponse;
import com.google.cloud.storage.sidecar.proto.WriteRequest;
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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalLong;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assume;
import org.junit.Test;

/** Step 6 ({@code WL6}: {@code Scale Write}) UDS streaming write, backpressure, and slot tests. */
public class Wl6ScaleWriteTest {

  private static final Context.Key<String> SESSION_CTX_KEY = Context.key("x-session");

  @Test
  public void applicabilityAndSlotRotationMatchSection2And3Spec() {
    Wl6ScaleWriteWorkload wl6 = new Wl6ScaleWriteWorkload();
    assertThat(wl6.id()).isEqualTo("WL6");
    assertThat(Wl6ScaleWriteWorkload.DEFAULT_WORKER_THREADS).isEqualTo(48);
    assertThat(Wl6ScaleWriteWorkload.DEFAULT_BUFFER_BYTES).isEqualTo(4 * 1024 * 1024);
    assertThat(Wl6ScaleWriteWorkload.DEFAULT_WRITES_PER_OBJECT).isEqualTo(50);
    assertThat(Wl6ScaleWriteWorkload.EXPECTED_DEFAULT_OBJECT_BYTES).isEqualTo(209_715_200L);
    assertThat(Wl6ScaleWriteWorkload.DEFAULT_WARMUP_SEC).isEqualTo(15L);
    assertThat(Wl6ScaleWriteWorkload.DEFAULT_DURATION_SEC).isEqualTo(300L);

    assertThat(Wl6ScaleWriteWorkload.slotObjectName(0, 0L)).isEqualTo("bench-write-w0-slot0.bin");
    assertThat(Wl6ScaleWriteWorkload.slotObjectName(0, 1L)).isEqualTo("bench-write-w0-slot1.bin");
    assertThat(Wl6ScaleWriteWorkload.slotObjectName(47, 2L))
        .isEqualTo("bench-write-w47-slot0.bin");
    assertThat(Wl6ScaleWriteWorkload.slotObjectName(47, 3L))
        .isEqualTo("bench-write-w47-slot1.bin");

    // Strict Section 3 matrix: ONLY GRPC+ZONAL and JSON+REGIONAL are REQUIRED; others are N/A
    assertThat(wl6.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.ZONAL)).isTrue();
    assertThat(wl6.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.REGIONAL)).isTrue();
    assertThat(wl6.isApplicable(UdsTransport.GRPC, BenchmarkConfig.BucketType.REGIONAL)).isFalse();
    assertThat(wl6.isApplicable(UdsTransport.JSON, BenchmarkConfig.BucketType.ZONAL)).isFalse();

    BenchmarkConfig regionalGrpcCfg =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.NATIVE)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.REGIONAL)
            .build();
    Storage dummyAdmin =
        newRecordingDeleteStorage(ConcurrentHashMap.newKeySet(), new AtomicReference<Instant>());
    try (ClientProvider provider = ClientProvider.forSingleClient(regionalGrpcCfg, dummyAdmin)) {
      assertThrows(IllegalArgumentException.class, () -> wl6.execute(provider, regionalGrpcCfg));
    }
  }

  @Test
  public void udsWriteStreamsSliceAt2MibLeaveJsonKnownSizeUnsetAndWakeWriterOnError()
      throws Exception {
    HermeticWriteServer server = new HermeticWriteServer(true);

    // 1. JSON writer(BlobInfo): 5 MiB buffer -> 2 MiB + 2 MiB + 1 MiB frames, known_size unset
    byte[] expected5Mib = new byte[5 * 1024 * 1024];
    new Random(99L).nextBytes(expected5Mib);

    try (UdsStorage jsonUds =
        UdsStorage.newBuilder(syntheticStorageOptions())
            .setTransport(UdsTransport.JSON)
            .setServerRuntime(server)
            .build()) {
      BlobInfo info =
          BlobInfo.newBuilder("regional-bkt", "json-test.bin")
              .setContentType("application/octet-stream")
              .build();
      assertThrows(
          UnsupportedOperationException.class,
          () -> jsonUds.blobAppendableUpload(info, BlobAppendableUploadConfig.of()));

      try (WriteChannel wch = jsonUds.writer(info)) {
        int written = StorageChannelUtils.blockingEmptyTo(ByteBuffer.wrap(expected5Mib), wch);
        assertThat(written).isEqualTo(expected5Mib.length);
      }

      WriteHeader capturedHeader = server.headers.get(0);
      assertThat(capturedHeader.getTransport()).isEqualTo(Transport.JSON);
      assertThat(capturedHeader.hasKnownSize()).isFalse();
      assertThat(capturedHeader.hasIfGenerationMatch()).isFalse();
      assertThat(capturedHeader.getContentType()).isEqualTo("application/octet-stream");
      assertThat(server.frameSizesPerUpload.get(0))
          .containsExactly(2 * 1024 * 1024, 2 * 1024 * 1024, 1 * 1024 * 1024)
          .inOrder();
      assertThat(server.capturedPayloads.get("json-test.bin")).isEqualTo(expected5Mib);
    }

    // 2. gRPC blobAppendableUpload: 4 MiB buffer -> 2 MiB + 2 MiB frames + terminal error wakeup
    HermeticWriteServer grpcServer = new HermeticWriteServer(false);
    try (UdsStorage grpcUds =
        UdsStorage.newBuilder(syntheticStorageOptions())
            .setTransport(UdsTransport.GRPC)
            .setServerRuntime(grpcServer)
            .build()) {
      BlobInfo target = BlobInfo.newBuilder("zonal-bkt", "append-ok.bin").build();
      BlobAppendableUpload upload =
          grpcUds.blobAppendableUpload(
              target,
              BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING));
      byte[] payload4Mib = new byte[4 * 1024 * 1024];
      try (AppendableUploadWriteableByteChannel ch = upload.open()) {
        assertThrows(IllegalStateException.class, upload::open);
        int written = StorageChannelUtils.blockingEmptyTo(ByteBuffer.wrap(payload4Mib), ch);
        assertThat(written).isEqualTo(payload4Mib.length);
        ch.finalizeAndClose();
      }
      BlobInfo finalized = upload.getResult().get(5, TimeUnit.SECONDS);
      assertThat(finalized.getSize()).isEqualTo(4L * 1024L * 1024L);
      assertThat(finalized.getGeneration()).isEqualTo(777L);
      assertThat(grpcServer.headers.get(0).getTransport()).isEqualTo(Transport.GRPC);
      assertThat(grpcServer.frameSizesPerUpload.get(0))
          .containsExactly(2 * 1024 * 1024, 2 * 1024 * 1024)
          .inOrder();

      // Inject mid-stream server error (RESOURCE_EXHAUSTED -> HTTP 429) and verify writer wakes
      // promptly without deadlocking on stream readiness.
      grpcServer.failWithResourceExhausted.set(true);
      BlobAppendableUpload failingUpload =
          grpcUds.blobAppendableUpload(
              BlobInfo.newBuilder("zonal-bkt", "append-fail.bin").build(),
              BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING));
      IOException thrown =
          assertThrows(
              IOException.class,
              () -> {
                try (AppendableUploadWriteableByteChannel ch = failingUpload.open()) {
                  for (int i = 0; i < 4; i++) {
                    StorageChannelUtils.blockingEmptyTo(ByteBuffer.wrap(payload4Mib), ch);
                  }
                  ch.finalizeAndClose();
                }
              });
      assertThat(thrown.getCause()).isInstanceOf(StorageException.class);
      assertThat(((StorageException) thrown.getCause()).getCode()).isEqualTo(429);
    }
  }

  @Test
  public void wl6Executes48Worker2SlotBoundedRotationAndStrictlyPostWindowCleanup()
      throws Exception {
    // 48 workers, 3 writes per worker (slot0, slot1, slot0 -> 144 uploads touching <= 96 slots)
    int testBufBytes = 16 * 1024;
    int testWritesPerObj = 4;

    // 1. Zonal gRPC WL6
    HermeticWriteServer grpcServer = new HermeticWriteServer(false);
    Set<String> grpcDeletedNames = ConcurrentHashMap.newKeySet();
    AtomicReference<Instant> firstGrpcDeleteTime = new AtomicReference<Instant>(null);
    Storage grpcAdmin = newRecordingDeleteStorage(grpcDeletedNames, firstGrpcDeleteTime);

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
        ClientProvider grpcProvider =
            ClientProvider.forSingleClientWithAdmin(grpcCfg, grpcUds, grpcAdmin)) {
      Wl6ScaleWriteWorkload wl6 =
          new Wl6ScaleWriteWorkload(48, testBufBytes, testWritesPerObj, 0L, 0L, 3);
      WorkloadResult grpcRes = wl6.execute(grpcProvider, grpcCfg);

      Set<String> expected96Slots = new HashSet<String>(96);
      for (int w = 0; w < 48; w++) {
        expected96Slots.add("bench-write-w" + w + "-slot0.bin");
        expected96Slots.add("bench-write-w" + w + "-slot1.bin");
      }

      assertThat(grpcRes.completedOps()).isEqualTo(144L);
      assertThat(grpcRes.totalBytes()).isEqualTo(144L * testBufBytes * testWritesPerObj);
      assertThat(grpcServer.uploadCount.get()).isEqualTo(144);
      assertThat(grpcServer.uniqueObjectNames).isEqualTo(expected96Slots);
      assertThat(grpcServer.preconditionCount.get()).isEqualTo(0);

      // Verify post-window cleanup deleted all 96 slots strictly AFTER wlEndUtc
      assertThat(grpcDeletedNames).isEqualTo(expected96Slots);
      assertThat(firstGrpcDeleteTime.get()).isAtLeast(grpcRes.wlEndUtc());
    }

    // 2. Regional JSON WL6
    HermeticWriteServer jsonServer = new HermeticWriteServer(false);
    Set<String> jsonDeletedNames = ConcurrentHashMap.newKeySet();
    AtomicReference<Instant> firstJsonDeleteTime = new AtomicReference<Instant>(null);
    Storage jsonAdmin = newRecordingDeleteStorage(jsonDeletedNames, firstJsonDeleteTime);

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
        ClientProvider jsonProvider =
            ClientProvider.forSingleClientWithAdmin(jsonCfg, jsonUds, jsonAdmin)) {
      Wl6ScaleWriteWorkload wl6 =
          new Wl6ScaleWriteWorkload(48, testBufBytes, testWritesPerObj, 0L, 0L, 2);
      WorkloadResult jsonRes = wl6.execute(jsonProvider, jsonCfg);

      assertThat(jsonRes.completedOps()).isEqualTo(96L);
      assertThat(jsonServer.uploadCount.get()).isEqualTo(96);
      assertThat(jsonServer.knownSizeSetCount.get()).isEqualTo(0);
      assertThat(jsonDeletedNames).hasSize(96);
      assertThat(firstJsonDeleteTime.get()).isAtLeast(jsonRes.wlEndUtc());
    }
  }

  @Test
  public void liveSidecarWl6SmokeAgainstRealZonalAndRegionalBucketsWhenEnabled() throws Exception {
    Assume.assumeTrue(
        "Pass -Duds.live.gcs=true on GCE VM to run live GCS WL6 smoke test",
        Boolean.getBoolean("uds.live.gcs"));
    String bin = System.getProperty("uds.sidecar.bin", "");
    Assume.assumeFalse("uds.sidecar.bin is required", bin.isEmpty());

    BenchmarkConfig zonalGrpc =
        BenchmarkConfig.newBuilder()
            .setDriver(BenchmarkConfig.DriverType.UDS_SIDECAR)
            .setProtocol(UdsTransport.GRPC)
            .setBucketType(BenchmarkConfig.BucketType.ZONAL)
            .setSidecarBinaryPath(bin)
            .build();
    try (ClientProvider provider = ClientProvider.create(zonalGrpc)) {
      Wl6ScaleWriteWorkload smoke = new Wl6ScaleWriteWorkload(2, 4 * 1024 * 1024, 2, 0L, 0L, 1);
      WorkloadResult res = smoke.execute(provider, zonalGrpc);
      assertThat(res.completedOps()).isEqualTo(2L);
      assertThat(res.totalBytes()).isEqualTo(2L * 8L * 1024L * 1024L);
    }
  }

  private static Storage newRecordingDeleteStorage(
      final Set<String> deletedNames, final AtomicReference<Instant> firstDeleteTime) {
    InvocationHandler handler =
        new InvocationHandler() {
          @Override
          public Object invoke(Object proxy, Method method, Object[] args) {
            if ("delete".equals(method.getName()) && args != null && args[0] instanceof BlobId) {
              firstDeleteTime.compareAndSet(null, Instant.now());
              deletedNames.add(((BlobId) args[0]).getName());
              return Boolean.TRUE;
            }
            if ("close".equals(method.getName())) {
              return null;
            }
            throw new UnsupportedOperationException("Unexpected admin call: " + method.getName());
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

  private static final class HermeticWriteServer extends GcsCoreGrpc.GcsCoreImplBase
      implements UdsServerRuntime {
    private final String socketPath = UdsServerRuntime.newUniqueSocketPath();
    private final boolean captureBytes;
    private final Set<String> validSessions = ConcurrentHashMap.newKeySet();

    final AtomicBoolean failWithResourceExhausted = new AtomicBoolean(false);
    final AtomicInteger uploadCount = new AtomicInteger(0);
    final AtomicInteger preconditionCount = new AtomicInteger(0);
    final AtomicInteger knownSizeSetCount = new AtomicInteger(0);
    final Set<String> uniqueObjectNames = ConcurrentHashMap.newKeySet();
    final List<WriteHeader> headers = Collections.synchronizedList(new ArrayList<WriteHeader>());
    final List<List<Integer>> frameSizesPerUpload =
        Collections.synchronizedList(new ArrayList<List<Integer>>());
    final ConcurrentHashMap<String, byte[]> capturedPayloads =
        new ConcurrentHashMap<String, byte[]>();

    private EpollEventLoopGroup bossGroup;
    private EpollEventLoopGroup workerGroup;
    private ExecutorService rpcExecutor;
    private Server server;

    HermeticWriteServer(boolean captureBytes) {
      this.captureBytes = captureBytes;
    }

    @Override
    public void start() throws IOException {
      UdsServerRuntime.unlinkSocketFiles(socketPath);
      bossGroup = new EpollEventLoopGroup(1, new DefaultThreadFactory("wl6-boss", true));
      workerGroup = new EpollEventLoopGroup(4, new DefaultThreadFactory("wl6-worker", true));
      rpcExecutor =
          Executors.newFixedThreadPool(48, new DefaultThreadFactory("wl6-rpc-exec", true));

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
      return OptionalLong.of(67890L);
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
      responseObserver.onNext(StatsResponse.newBuilder().setPid(67890).setRssBytes(8192L).build());
      responseObserver.onCompleted();
    }

    @Override
    public void shutdown(Empty request, StreamObserver<Empty> responseObserver) {
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<WriteRequest> writeObject(
        final StreamObserver<WriteObjectResponse> responseObserver) {
      String sid = SESSION_CTX_KEY.get();
      if (sid == null || !validSessions.contains(sid)) {
        responseObserver.onError(Status.UNAUTHENTICATED.asRuntimeException());
        return new NoopRequestObserver();
      }

      return new StreamObserver<WriteRequest>() {
        private WriteHeader header;
        private long totalSize = 0L;
        private final List<Integer> frameSizes = new ArrayList<Integer>();
        private final ByteArrayOutputStream captured =
            captureBytes ? new ByteArrayOutputStream() : null;
        private boolean terminated = false;

        @Override
        public void onNext(WriteRequest value) {
          if (terminated) {
            return;
          }
          if (value.hasHeader()) {
            header = value.getHeader();
            headers.add(header);
            uniqueObjectNames.add(header.getObject());
            if (header.hasIfGenerationMatch()) {
              preconditionCount.incrementAndGet();
            }
            if (header.hasKnownSize()) {
              knownSizeSetCount.incrementAndGet();
            }
            return;
          }
          if (failWithResourceExhausted.get()) {
            terminated = true;
            responseObserver.onError(
                Status.RESOURCE_EXHAUSTED
                    .withDescription("Simulated quota breach")
                    .asRuntimeException());
            return;
          }
          int len = value.getData().size();
          frameSizes.add(len);
          totalSize += len;
          if (captured != null) {
            try {
              value.getData().writeTo(captured);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          }
        }

        @Override
        public void onError(Throwable t) {
          terminated = true;
        }

        @Override
        public void onCompleted() {
          if (terminated) {
            return;
          }
          uploadCount.incrementAndGet();
          frameSizesPerUpload.add(frameSizes);
          if (captured != null && header != null) {
            capturedPayloads.put(header.getObject(), captured.toByteArray());
          }
          responseObserver.onNext(
              WriteObjectResponse.newBuilder()
                  .setSize(totalSize)
                  .setGeneration(777L)
                  .setCrc32C(123456)
                  .setTransportUsed(header != null ? header.getTransport() : Transport.GRPC)
                  .build());
          responseObserver.onCompleted();
        }
      };
    }

    private static final class NoopRequestObserver implements StreamObserver<WriteRequest> {
      @Override
      public void onNext(WriteRequest value) {}

      @Override
      public void onError(Throwable t) {}

      @Override
      public void onCompleted() {}
    }
  }
}
