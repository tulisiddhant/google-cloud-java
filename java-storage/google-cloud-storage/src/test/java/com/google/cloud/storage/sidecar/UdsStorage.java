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

import static java.util.Objects.requireNonNull;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.Policy;
import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Acl.Entity;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.BlobWriteSession;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.HmacKey;
import com.google.cloud.storage.HmacKey.HmacKeyMetadata;
import com.google.cloud.storage.Notification;
import com.google.cloud.storage.NotificationInfo;
import com.google.cloud.storage.PostPolicyV4;
import com.google.cloud.storage.PostPolicyV4.PostConditionsV4;
import com.google.cloud.storage.PostPolicyV4.PostFieldsV4;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadAsFutureBytes;
import com.google.cloud.storage.ReadProjectionConfig;
import com.google.cloud.storage.ServiceAccount;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.UdsPackageBridge;
import com.google.cloud.storage.sidecar.proto.CloseObjectRequest;
import com.google.cloud.storage.sidecar.proto.Empty;
import com.google.cloud.storage.sidecar.proto.GcsCoreGrpc;
import com.google.cloud.storage.sidecar.proto.ObjectMeta;
import com.google.cloud.storage.sidecar.proto.OpenObjectRequest;
import com.google.cloud.storage.sidecar.proto.OpenObjectResponse;
import com.google.cloud.storage.sidecar.proto.ReadChunk;
import com.google.cloud.storage.sidecar.proto.ReadObjectRequest;
import com.google.cloud.storage.sidecar.proto.ReadRangeRequest;
import com.google.cloud.storage.sidecar.proto.StatsRequest;
import com.google.cloud.storage.sidecar.proto.StatsResponse;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link Storage} veneer implementation backed by the shared Rust {@code gcscore.v1.GcsCore}
 * service over a local Unix Domain Socket ({@code AF_UNIX}).
 *
 * <p>Step 0 provides server lifecycle management, Netty UDS channel setup, OAuth2 credential
 * lending, and {@code GcsCore.Stats} telemetry. Data-plane and control-plane veneer methods are
 * added incrementally per workload in Steps 1–7.
 */
public final class UdsStorage implements Storage {

  private static final long DEFAULT_STARTUP_TIMEOUT_MS = 15_000L;

  private final StorageOptions options;
  private final DriverMode driverMode;
  private final UdsTransport transport;
  private final UdsServerRuntime serverRuntime;
  private final UdsChannelFactory.ChannelHandle channelHandle;
  private final GcsSessionManager sessionManager;
  private final GcsCoreGrpc.GcsCoreBlockingStub blockingStub;
  private final GcsCoreGrpc.GcsCoreStub asyncStub;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private UdsStorage(
      StorageOptions options,
      DriverMode driverMode,
      UdsTransport transport,
      UdsServerRuntime serverRuntime,
      UdsChannelFactory.ChannelHandle channelHandle,
      GcsSessionManager sessionManager) {
    this.options = options;
    this.driverMode = driverMode;
    this.transport = transport;
    this.serverRuntime = serverRuntime;
    this.channelHandle = channelHandle;
    this.sessionManager = sessionManager;

    Channel intercepted = ClientInterceptors.intercept(channelHandle.channel(), sessionManager);
    this.blockingStub = GcsCoreGrpc.newBlockingStub(intercepted);
    this.asyncStub = GcsCoreGrpc.newStub(intercepted);
  }

  public static Builder newBuilder(StorageOptions baseOptions) {
    return new Builder(baseOptions);
  }

  /** Builder for {@link UdsStorage}. */
  public static final class Builder {
    private final StorageOptions baseOptions;
    private DriverMode driverMode = DriverMode.SIDECAR;
    private UdsTransport transport = UdsTransport.GRPC;
    private String socketPath;
    private String sidecarBinaryPath;
    private UdsServerRuntime customRuntime;
    private long startupTimeoutMs = DEFAULT_STARTUP_TIMEOUT_MS;

    private Builder(StorageOptions baseOptions) {
      this.baseOptions = requireNonNull(baseOptions, "baseOptions");
    }

    public Builder setDriverMode(DriverMode driverMode) {
      this.driverMode = requireNonNull(driverMode, "driverMode");
      return this;
    }

    public Builder setTransport(UdsTransport transport) {
      this.transport = requireNonNull(transport, "transport");
      return this;
    }

    public Builder setSocketPath(String socketPath) {
      this.socketPath = socketPath;
      return this;
    }

    public Builder setSidecarBinaryPath(String sidecarBinaryPath) {
      this.sidecarBinaryPath = sidecarBinaryPath;
      return this;
    }

    Builder setServerRuntime(UdsServerRuntime customRuntime) {
      this.customRuntime = requireNonNull(customRuntime, "customRuntime");
      this.socketPath = customRuntime.socketPath();
      return this;
    }

    public Builder setStartupTimeoutMs(long startupTimeoutMs) {
      this.startupTimeoutMs = startupTimeoutMs;
      return this;
    }

    public UdsStorage build() throws IOException {
      String resolvedSocket =
          (socketPath == null || socketPath.isEmpty())
              ? UdsServerRuntime.newUniqueSocketPath()
              : socketPath;

      UdsServerRuntime runtime =
          customRuntime != null ? customRuntime : createRuntime(resolvedSocket);
      UdsChannelFactory.ChannelHandle chHandle = null;
      GcsSessionManager sessMgr = null;
      boolean ok = false;
      try {
        runtime.start();
        chHandle = UdsChannelFactory.open(resolvedSocket);
        UdsChannelFactory.awaitReady(chHandle.channel(), startupTimeoutMs, runtime);
        sessMgr = GcsSessionManager.create(chHandle.channel(), baseOptions);
        if (runtime instanceof SidecarServerRuntime && !runtime.serverPid().isPresent()) {
          StatsResponse stats =
              GcsCoreGrpc.newBlockingStub(
                      ClientInterceptors.intercept(chHandle.channel(), sessMgr))
                  .withDeadlineAfter(5, TimeUnit.SECONDS)
                  .stats(StatsRequest.newBuilder().setResetPeakRss(false).build());
          ((SidecarServerRuntime) runtime).recordServerPid(stats.getPid());
        }
        UdsStorage instance =
            new UdsStorage(baseOptions, driverMode, transport, runtime, chHandle, sessMgr);
        ok = true;
        return instance;
      } finally {
        if (!ok) {
          if (sessMgr != null) {
            sessMgr.close();
          }
          if (chHandle != null) {
            chHandle.close();
          }
          runtime.close();
        }
      }
    }

    private UdsServerRuntime createRuntime(String resolvedSocket) {
      if (driverMode == DriverMode.SIDECAR) {
        String bin =
            sidecarBinaryPath != null
                ? sidecarBinaryPath
                : System.getProperty("uds.sidecar.bin", "");
        if (bin.isEmpty()) {
          throw new IllegalArgumentException(
              "sidecarBinaryPath must be set via Builder or -Duds.sidecar.bin for SIDECAR mode");
        }
        return new SidecarServerRuntime(bin, resolvedSocket);
      }
      throw new UnsupportedOperationException("Unsupported DriverMode: " + driverMode);
    }
  }

  public DriverMode getDriverMode() {
    return driverMode;
  }

  public UdsTransport getTransport() {
    return transport;
  }

  public String getSocketPath() {
    return serverRuntime.socketPath();
  }

  public OptionalLong getServerPid() {
    return serverRuntime.serverPid();
  }

  public GcsSessionManager getSessionManager() {
    return sessionManager;
  }

  /** Queries {@code GcsCore.Stats} for supplementary Rust core CPU & VmHWM diagnostics. */
  public StatsResponse getCoreStats(boolean resetPeakRss) {
    return blockingStub
        .withDeadlineAfter(10, TimeUnit.SECONDS)
        .stats(StatsRequest.newBuilder().setResetPeakRss(resetPeakRss).build());
  }

  GcsCoreGrpc.GcsCoreBlockingStub blockingStub() {
    return blockingStub;
  }

  GcsCoreGrpc.GcsCoreStub asyncStub() {
    return asyncStub;
  }

  @Override
  public StorageOptions getOptions() {
    return options;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      sessionManager.close();
    } finally {
      try {
        GcsCoreGrpc.newBlockingStub(channelHandle.channel())
            .withDeadlineAfter(2, TimeUnit.SECONDS)
            .shutdown(Empty.getDefaultInstance());
      } catch (RuntimeException ignored) {
        // Server may already be shutting down.
      } finally {
        try {
          channelHandle.close();
        } finally {
          serverRuntime.close();
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // 6 POC Veneer APIs — implemented step by step in Steps 1–7
  // ---------------------------------------------------------------------------

  private void ensureOpen() {
    if (closed.get()) {
      throw new IllegalStateException("UdsStorage client is closed");
    }
  }

  @Override
  public ApiFuture<BlobReadSession> blobReadSession(BlobId id, BlobSourceOption... options) {
    requireNonNull(id, "id");
    ensureOpen();
    if (transport != UdsTransport.GRPC) {
      throw new UnsupportedOperationException(
          "blobReadSession is only supported when UdsTransport is GRPC");
    }

    OpenObjectRequest.Builder reqBuilder =
        OpenObjectRequest.newBuilder().setBucket(id.getBucket()).setObject(id.getName());
    if (id.getGeneration() != null && id.getGeneration() > 0) {
      reqBuilder.setGeneration(id.getGeneration());
    }

    final SettableApiFuture<BlobReadSession> future = SettableApiFuture.create();
    asyncStub.openObject(
        reqBuilder.build(),
        new ClientResponseObserver<OpenObjectRequest, OpenObjectResponse>() {
          @Override
          public void beforeStart(
              final ClientCallStreamObserver<OpenObjectRequest> requestStream) {
            future.addListener(
                () -> {
                  if (future.isCancelled()) {
                    requestStream.cancel("ApiFuture cancelled by caller", null);
                  }
                },
                MoreExecutors.directExecutor());
          }

          @Override
          public void onNext(OpenObjectResponse resp) {
            ObjectMeta meta = resp.getMeta();
            long resolvedGen =
                meta.getGeneration() > 0
                    ? meta.getGeneration()
                    : (id.getGeneration() != null ? id.getGeneration() : 0L);
            BlobInfo info =
                UdsPackageBridge.buildBlobInfo(
                    id.getBucket(),
                    id.getName(),
                    resolvedGen,
                    meta.getSize(),
                    meta.getMetageneration(),
                    meta.getContentType(),
                    meta.getContentEncoding(),
                    "",
                    0L,
                    meta.getCrc32C());
            UdsBlobReadSession session = new UdsBlobReadSession(resp.getHandle(), info);
            if (!future.set(session)) {
              session.closeQuietly();
            }
          }

          @Override
          public void onError(Throwable t) {
            future.setException(UdsPackageBridge.toStorageException(t));
          }

          @Override
          public void onCompleted() {
            if (!future.isDone()) {
              future.setException(
                  new StorageException(
                      500, "GcsCore.OpenObject completed without returning OpenObjectResponse"));
            }
          }
        });
    return future;
  }

  /**
   * Thin {@link BlobReadSession} implementation forwarding session lifecycle and range reads to
   * {@code GcsCore.OpenObject}, {@code GcsCore.ReadRange}, and {@code GcsCore.CloseObject}.
   */
  private final class UdsBlobReadSession implements BlobReadSession {
    private final long handle;
    private final BlobInfo blobInfo;
    private final AtomicBoolean sessionClosed = new AtomicBoolean(false);
    private final Set<SettableApiFuture<?>> inflightReads = ConcurrentHashMap.newKeySet();

    UdsBlobReadSession(long handle, BlobInfo blobInfo) {
      this.handle = handle;
      this.blobInfo = blobInfo;
    }

    @Override
    public BlobInfo getBlobInfo() {
      return blobInfo;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Projection> Projection readAs(ReadProjectionConfig<Projection> config) {
      requireNonNull(config, "config");
      if (sessionClosed.get()) {
        throw new IllegalStateException("BlobReadSession is already closed");
      }
      if (config instanceof ReadAsFutureBytes) {
        RangeSpec range = ((ReadAsFutureBytes) config).getRange();
        return (Projection) readRangeAsFutureBytes(range);
      }
      return unsupported("UdsBlobReadSession.readAs(" + config.getClass().getSimpleName() + ")");
    }

    private ApiFuture<byte[]> readRangeAsFutureBytes(RangeSpec range) {
      long offset = range.begin();
      OptionalLong maxLenOpt = range.maxLength();
      if (maxLenOpt.isPresent() && maxLenOpt.getAsLong() == 0L) {
        return ApiFutures.immediateFuture(new byte[0]);
      }

      long requestedLen = maxLenOpt.isPresent() ? maxLenOpt.getAsLong() : -1L;
      int initialCapacity = computeInitialCapacity(offset, maxLenOpt);

      ReadRangeRequest req =
          ReadRangeRequest.newBuilder()
              .setHandle(handle)
              .setOffset(offset)
              .setLength(requestedLen)
              .build();

      final SettableApiFuture<byte[]> future = SettableApiFuture.create();
      inflightReads.add(future);
      future.addListener(() -> inflightReads.remove(future), MoreExecutors.directExecutor());

      if (sessionClosed.get()) {
        future.setException(new IOException("BlobReadSession closed"));
        return future;
      }

      asyncStub.readRange(
          req,
          new ClientResponseObserver<ReadRangeRequest, ReadChunk>() {
            private byte[] buffer =
                initialCapacity > 0 ? new byte[initialCapacity] : new byte[64 * 1024];
            private int writePos = 0;

            @Override
            public void beforeStart(
                final ClientCallStreamObserver<ReadRangeRequest> requestStream) {
              future.addListener(
                  () -> {
                    if (future.isCancelled()) {
                      requestStream.cancel("Range read cancelled", null);
                    }
                  },
                  MoreExecutors.directExecutor());
            }

            @Override
            public void onNext(ReadChunk chunk) {
              List<ByteString> slices = chunk.getDataList();
              for (int i = 0; i < slices.size(); i++) {
                ByteString slice = slices.get(i);
                int len = slice.size();
                if (len == 0) {
                  continue;
                }
                if (writePos + len > buffer.length) {
                  int nextCap = Math.max(buffer.length * 2, writePos + len);
                  buffer = Arrays.copyOf(buffer, nextCap);
                }
                slice.copyTo(buffer, writePos);
                writePos += len;
              }
            }

            @Override
            public void onError(Throwable t) {
              future.setException(UdsPackageBridge.toStorageException(t));
            }

            @Override
            public void onCompleted() {
              byte[] result = (writePos == buffer.length) ? buffer : Arrays.copyOf(buffer, writePos);
              future.set(result);
            }
          });
      return future;
    }

    private int computeInitialCapacity(long offset, OptionalLong maxLenOpt) {
      Long objSize = blobInfo != null ? blobInfo.getSize() : null;
      long remainingInObj =
          (objSize != null && objSize > offset) ? (objSize - offset) : Long.MAX_VALUE;
      long target =
          maxLenOpt.isPresent()
              ? Math.min(maxLenOpt.getAsLong(), remainingInObj)
              : remainingInObj;
      if (target > 0L && target <= Integer.MAX_VALUE - 8) {
        return (int) target;
      }
      return 0;
    }

    @Override
    public void close() throws IOException {
      if (!sessionClosed.compareAndSet(false, true)) {
        return;
      }
      for (SettableApiFuture<?> pending : inflightReads) {
        pending.setException(new IOException("BlobReadSession closed while range read was active"));
      }
      inflightReads.clear();
      try {
        blockingStub
            .withDeadlineAfter(30, TimeUnit.SECONDS)
            .closeObject(CloseObjectRequest.newBuilder().setHandle(handle).build());
      } catch (RuntimeException e) {
        throw new IOException("Failed to close UDS object handle " + handle, e);
      }
    }

    void closeQuietly() {
      try {
        close();
      } catch (IOException ignored) {
        // Best-effort cleanup when caller cancelled open future mid-flight.
      }
    }
  }

  @Override
  public ReadChannel reader(String bucket, String blob, BlobSourceOption... options) {
    return reader(BlobId.of(bucket, blob), options);
  }

  @Override
  public ReadChannel reader(BlobId blob, BlobSourceOption... options) {
    requireNonNull(blob, "blob");
    ensureOpen();
    OptionalLong ifGenMatch = UdsPackageBridge.extractSourceIfGenerationMatch(blob, options);
    return new UdsReadChannel(blob, ifGenMatch);
  }

  /**
   * Thin {@link ReadChannel} forwarding unidirectional range and full-object reads to {@code
   * GcsCore.ReadObject} with explicit {@link Context.CancellableContext} stream cancellation on
   * {@link #close()}.
   */
  private final class UdsReadChannel implements ReadChannel {
    private final BlobId blobId;
    private final OptionalLong ifGenerationMatch;

    private long position = 0L;
    private long endLimit = Long.MAX_VALUE;
    private int chunkSize = 0;
    private boolean channelOpen = true;

    private Context.CancellableContext streamCtx;
    private Iterator<ReadChunk> chunkIterator;
    private List<ByteString> currentSlices;
    private int sliceIndex;
    private int sliceOffset;
    private boolean streamEof;

    UdsReadChannel(BlobId blobId, OptionalLong ifGenerationMatch) {
      this.blobId = blobId;
      this.ifGenerationMatch = ifGenerationMatch;
    }

    @Override
    public synchronized boolean isOpen() {
      return channelOpen;
    }

    @Override
    public synchronized void seek(long newPosition) throws IOException {
      if (!channelOpen) {
        throw new ClosedChannelException();
      }
      if (newPosition < 0L) {
        throw new IllegalArgumentException("seek position must be >= 0, got: " + newPosition);
      }
      if (newPosition != this.position) {
        cancelActiveStream();
        this.position = newPosition;
        this.streamEof = false;
      }
    }

    @Override
    public synchronized ReadChannel limit(long newLimit) {
      if (newLimit < 0L) {
        throw new IllegalArgumentException("limit must be >= 0, got: " + newLimit);
      }
      if (newLimit != this.endLimit) {
        cancelActiveStream();
        this.endLimit = newLimit;
        this.streamEof = false;
      }
      return this;
    }

    @Override
    public synchronized long limit() {
      return endLimit;
    }

    @Override
    public synchronized void setChunkSize(int chunkSize) {
      this.chunkSize = Math.max(0, chunkSize);
    }

    @Override
    public RestorableState<ReadChannel> capture() {
      return unsupported("UdsReadChannel.capture");
    }

    @Override
    public synchronized int read(ByteBuffer dst) throws IOException {
      requireNonNull(dst, "dst");
      if (!channelOpen) {
        throw new ClosedChannelException();
      }
      if (endLimit != Long.MAX_VALUE && position >= endLimit) {
        return -1;
      }
      if (!dst.hasRemaining()) {
        return 0;
      }
      if (streamEof) {
        return -1;
      }

      try {
        if (chunkIterator == null) {
          openStream();
        }

        int totalCopied = 0;
        while (dst.hasRemaining()) {
          ByteString activeSlice = nextNonEmptySlice(totalCopied == 0);
          if (activeSlice == null) {
            if (totalCopied > 0) {
              return totalCopied;
            }
            streamEof = true;
            cancelActiveStream();
            return -1;
          }

          long maxByLimit =
              (endLimit == Long.MAX_VALUE) ? Long.MAX_VALUE : Math.max(0L, endLimit - position);
          if (maxByLimit == 0L) {
            cancelActiveStream();
            return totalCopied > 0 ? totalCopied : -1;
          }

          int availableInSlice = activeSlice.size() - sliceOffset;
          int toCopy = (int) Math.min(Math.min(dst.remaining(), availableInSlice), maxByLimit);

          ByteBuffer ro = activeSlice.asReadOnlyByteBuffer();
          ro.position(sliceOffset);
          ro.limit(sliceOffset + toCopy);
          dst.put(ro);

          sliceOffset += toCopy;
          position += toCopy;
          totalCopied += toCopy;

          if (sliceOffset >= activeSlice.size()) {
            sliceIndex++;
            sliceOffset = 0;
          }
          if (endLimit != Long.MAX_VALUE && position >= endLimit) {
            cancelActiveStream();
            return totalCopied;
          }
        }
        return totalCopied;
      } catch (RuntimeException e) {
        cancelActiveStream();
        if (Status.fromThrowable(e).getCode() == Status.Code.OUT_OF_RANGE) {
          streamEof = true;
          return -1;
        }
        throw new IOException(UdsPackageBridge.toStorageException(e));
      }
    }

    private void openStream() {
      long reqLength = (endLimit == Long.MAX_VALUE) ? -1L : (endLimit - position);
      ReadObjectRequest.Builder reqB =
          ReadObjectRequest.newBuilder()
              .setBucket(blobId.getBucket())
              .setObject(blobId.getName())
              .setOffset(position)
              .setLength(reqLength)
              .setTransport(transport.toProto())
              .setChunkSize(chunkSize > 0 ? chunkSize : 0);
      if (blobId.getGeneration() != null && blobId.getGeneration() > 0) {
        reqB.setGeneration(blobId.getGeneration());
      }
      if (ifGenerationMatch.isPresent()) {
        reqB.setIfGenerationMatch(ifGenerationMatch.getAsLong());
      }
      ReadObjectRequest req = reqB.build();
      streamCtx = Context.current().withCancellation();
      Context prev = streamCtx.attach();
      try {
        chunkIterator = blockingStub.readObject(req);
      } finally {
        streamCtx.detach(prev);
      }
      currentSlices = null;
      sliceIndex = 0;
      sliceOffset = 0;
    }

    private ByteString nextNonEmptySlice(boolean blockForNextChunk) {
      while (true) {
        if (currentSlices != null) {
          while (sliceIndex < currentSlices.size()) {
            ByteString candidate = currentSlices.get(sliceIndex);
            if (sliceOffset < candidate.size()) {
              return candidate;
            }
            sliceIndex++;
            sliceOffset = 0;
          }
          currentSlices = null;
        }
        if (!blockForNextChunk) {
          return null;
        }
        if (chunkIterator == null || !chunkIterator.hasNext()) {
          return null;
        }
        ReadChunk nextChunk = chunkIterator.next();
        currentSlices = nextChunk.getDataList();
        sliceIndex = 0;
        sliceOffset = 0;
      }
    }

    private void cancelActiveStream() {
      Context.CancellableContext ctx = this.streamCtx;
      this.streamCtx = null;
      this.chunkIterator = null;
      this.currentSlices = null;
      this.sliceIndex = 0;
      this.sliceOffset = 0;
      if (ctx != null) {
        ctx.cancel(null);
      }
    }

    @Override
    public synchronized void close() {
      if (!channelOpen) {
        return;
      }
      channelOpen = false;
      cancelActiveStream();
    }
  }

  @Override
  public BlobAppendableUpload blobAppendableUpload(
      BlobInfo blobInfo, BlobAppendableUploadConfig uploadConfig, BlobWriteOption... options) {
    return unsupported("blobAppendableUpload (implemented in Step 6 / WL6)");
  }

  @Override
  public WriteChannel writer(BlobInfo blobInfo, BlobWriteOption... options) {
    return unsupported("writer (implemented in Step 6 / WL6)");
  }

  @Override
  public Page<Blob> list(String bucket, BlobListOption... options) {
    return unsupported("list(bucket, BlobListOption...) (implemented in Step 7 / WL7a)");
  }

  @Override
  public Page<Bucket> list(BucketListOption... options) {
    return unsupported("list(BucketListOption...) (implemented in Step 7 / WL7b)");
  }

  // ---------------------------------------------------------------------------
  // Out-of-scope Storage interface methods
  // ---------------------------------------------------------------------------

  private static <T> T unsupported(String method) {
    throw new UnsupportedOperationException("UdsStorage: " + method + " is not supported");
  }

  @Override
  public Bucket create(BucketInfo bucketInfo, BucketTargetOption... options) {
    return unsupported("create(BucketInfo)");
  }

  @Override
  public Blob create(BlobInfo blobInfo, BlobTargetOption... options) {
    return unsupported("create(BlobInfo)");
  }

  @Override
  public Blob create(BlobInfo blobInfo, byte[] content, BlobTargetOption... options) {
    return unsupported("create(BlobInfo, byte[])");
  }

  @Override
  public Blob create(
      BlobInfo blobInfo, byte[] content, int offset, int length, BlobTargetOption... options) {
    return unsupported("create(BlobInfo, byte[], int, int)");
  }

  @Override
  @Deprecated
  public Blob create(BlobInfo blobInfo, InputStream content, BlobWriteOption... options) {
    return unsupported("create(BlobInfo, InputStream)");
  }

  @Override
  public Blob createFrom(BlobInfo blobInfo, Path path, BlobWriteOption... options) {
    return unsupported("createFrom(BlobInfo, Path)");
  }

  @Override
  public Blob createFrom(
      BlobInfo blobInfo, Path path, int bufferSize, BlobWriteOption... options) {
    return unsupported("createFrom(BlobInfo, Path, int)");
  }

  @Override
  public Blob createFrom(BlobInfo blobInfo, InputStream content, BlobWriteOption... options) {
    return unsupported("createFrom(BlobInfo, InputStream)");
  }

  @Override
  public Blob createFrom(
      BlobInfo blobInfo, InputStream content, int bufferSize, BlobWriteOption... options) {
    return unsupported("createFrom(BlobInfo, InputStream, int)");
  }

  @Override
  public Bucket get(String bucket, BucketGetOption... options) {
    return unsupported("get(String)");
  }

  @Override
  public Bucket lockRetentionPolicy(BucketInfo bucket, BucketTargetOption... options) {
    return unsupported("lockRetentionPolicy");
  }

  @Override
  public Blob get(String bucket, String blob, BlobGetOption... options) {
    return unsupported("get(String, String)");
  }

  @Override
  public Blob get(BlobId blob, BlobGetOption... options) {
    return unsupported("get(BlobId, BlobGetOption...)");
  }

  @Override
  public Blob get(BlobId blob) {
    return unsupported("get(BlobId)");
  }

  @Override
  public Blob restore(BlobId blob, BlobRestoreOption... options) {
    return unsupported("restore");
  }

  @Override
  public Bucket update(BucketInfo bucketInfo, BucketTargetOption... options) {
    return unsupported("update(BucketInfo)");
  }

  @Override
  public Blob update(BlobInfo blobInfo, BlobTargetOption... options) {
    return unsupported("update(BlobInfo, BlobTargetOption...)");
  }

  @Override
  public Blob update(BlobInfo blobInfo) {
    return unsupported("update(BlobInfo)");
  }

  @Override
  public boolean delete(String bucket, BucketSourceOption... options) {
    return unsupported("delete(String)");
  }

  @Override
  public boolean delete(String bucket, String blob, BlobSourceOption... options) {
    return unsupported("delete(String, String)");
  }

  @Override
  public boolean delete(BlobId blob, BlobSourceOption... options) {
    return unsupported("delete(BlobId, BlobSourceOption...)");
  }

  @Override
  public boolean delete(BlobId blob) {
    return unsupported("delete(BlobId)");
  }

  @Override
  public Blob compose(ComposeRequest composeRequest) {
    return unsupported("compose");
  }

  @Override
  public CopyWriter copy(CopyRequest copyRequest) {
    return unsupported("copy");
  }

  @Override
  public byte[] readAllBytes(String bucket, String blob, BlobSourceOption... options) {
    return unsupported("readAllBytes(String, String)");
  }

  @Override
  public byte[] readAllBytes(BlobId blob, BlobSourceOption... options) {
    return unsupported("readAllBytes(BlobId)");
  }

  @Override
  public StorageBatch batch() {
    return unsupported("batch");
  }

  @Override
  public void downloadTo(BlobId blob, Path path, BlobSourceOption... options) {
    unsupported("downloadTo(BlobId, Path)");
  }

  @Override
  public void downloadTo(BlobId blob, OutputStream outputStream, BlobSourceOption... options) {
    unsupported("downloadTo(BlobId, OutputStream)");
  }

  @Override
  public WriteChannel writer(URL signedURL) {
    return unsupported("writer(URL)");
  }

  @Override
  public URL signUrl(BlobInfo blobInfo, long duration, TimeUnit unit, SignUrlOption... options) {
    return unsupported("signUrl");
  }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(
      BlobInfo blobInfo,
      long duration,
      TimeUnit unit,
      PostFieldsV4 fields,
      PostConditionsV4 conditions,
      PostPolicyV4Option... options) {
    return unsupported("generateSignedPostPolicyV4");
  }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(
      BlobInfo blobInfo,
      long duration,
      TimeUnit unit,
      PostFieldsV4 fields,
      PostPolicyV4Option... options) {
    return unsupported("generateSignedPostPolicyV4");
  }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(
      BlobInfo blobInfo,
      long duration,
      TimeUnit unit,
      PostConditionsV4 conditions,
      PostPolicyV4Option... options) {
    return unsupported("generateSignedPostPolicyV4");
  }

  @Override
  public PostPolicyV4 generateSignedPostPolicyV4(
      BlobInfo blobInfo, long duration, TimeUnit unit, PostPolicyV4Option... options) {
    return unsupported("generateSignedPostPolicyV4");
  }

  @Override
  public List<Blob> get(BlobId... blobIds) {
    return unsupported("get(BlobId...)");
  }

  @Override
  public List<Blob> get(Iterable<BlobId> blobIds) {
    return unsupported("get(Iterable)");
  }

  @Override
  public List<Blob> update(BlobInfo... blobInfos) {
    return unsupported("update(BlobInfo...)");
  }

  @Override
  public List<Blob> update(Iterable<BlobInfo> blobInfos) {
    return unsupported("update(Iterable)");
  }

  @Override
  public List<Boolean> delete(BlobId... blobIds) {
    return unsupported("delete(BlobId...)");
  }

  @Override
  public List<Boolean> delete(Iterable<BlobId> blobIds) {
    return unsupported("delete(Iterable)");
  }

  @Override
  public Acl getAcl(String bucket, Entity entity, BucketSourceOption... options) {
    return unsupported("getAcl");
  }

  @Override
  public Acl getAcl(String bucket, Entity entity) {
    return unsupported("getAcl");
  }

  @Override
  public boolean deleteAcl(String bucket, Entity entity, BucketSourceOption... options) {
    return unsupported("deleteAcl");
  }

  @Override
  public boolean deleteAcl(String bucket, Entity entity) {
    return unsupported("deleteAcl");
  }

  @Override
  public Acl createAcl(String bucket, Acl acl, BucketSourceOption... options) {
    return unsupported("createAcl");
  }

  @Override
  public Acl createAcl(String bucket, Acl acl) {
    return unsupported("createAcl");
  }

  @Override
  public Acl updateAcl(String bucket, Acl acl, BucketSourceOption... options) {
    return unsupported("updateAcl");
  }

  @Override
  public Acl updateAcl(String bucket, Acl acl) {
    return unsupported("updateAcl");
  }

  @Override
  public List<Acl> listAcls(String bucket, BucketSourceOption... options) {
    return unsupported("listAcls");
  }

  @Override
  public List<Acl> listAcls(String bucket) {
    return unsupported("listAcls");
  }

  @Override
  public Acl getDefaultAcl(String bucket, Entity entity) {
    return unsupported("getDefaultAcl");
  }

  @Override
  public boolean deleteDefaultAcl(String bucket, Entity entity) {
    return unsupported("deleteDefaultAcl");
  }

  @Override
  public Acl createDefaultAcl(String bucket, Acl acl) {
    return unsupported("createDefaultAcl");
  }

  @Override
  public Acl updateDefaultAcl(String bucket, Acl acl) {
    return unsupported("updateDefaultAcl");
  }

  @Override
  public List<Acl> listDefaultAcls(String bucket) {
    return unsupported("listDefaultAcls");
  }

  @Override
  public Acl getAcl(BlobId blob, Entity entity) {
    return unsupported("getAcl");
  }

  @Override
  public boolean deleteAcl(BlobId blob, Entity entity) {
    return unsupported("deleteAcl");
  }

  @Override
  public Acl createAcl(BlobId blob, Acl acl) {
    return unsupported("createAcl");
  }

  @Override
  public Acl updateAcl(BlobId blob, Acl acl) {
    return unsupported("updateAcl");
  }

  @Override
  public List<Acl> listAcls(BlobId blob) {
    return unsupported("listAcls");
  }

  @Override
  public HmacKey createHmacKey(ServiceAccount serviceAccount, CreateHmacKeyOption... options) {
    return unsupported("createHmacKey");
  }

  @Override
  public Page<HmacKeyMetadata> listHmacKeys(ListHmacKeysOption... options) {
    return unsupported("listHmacKeys");
  }

  @Override
  public HmacKeyMetadata getHmacKey(String accessId, GetHmacKeyOption... options) {
    return unsupported("getHmacKey");
  }

  @Override
  public void deleteHmacKey(HmacKeyMetadata hmacKeyMetadata, DeleteHmacKeyOption... options) {
    unsupported("deleteHmacKey");
  }

  @Override
  public HmacKeyMetadata updateHmacKeyState(
      HmacKeyMetadata hmacKeyMetadata,
      HmacKey.HmacKeyState state,
      UpdateHmacKeyOption... options) {
    return unsupported("updateHmacKeyState");
  }

  @Override
  public Policy getIamPolicy(String bucket, BucketSourceOption... options) {
    return unsupported("getIamPolicy");
  }

  @Override
  public Policy setIamPolicy(String bucket, Policy policy, BucketSourceOption... options) {
    return unsupported("setIamPolicy");
  }

  @Override
  public List<Boolean> testIamPermissions(
      String bucket, List<String> permissions, BucketSourceOption... options) {
    return unsupported("testIamPermissions");
  }

  @Override
  public ServiceAccount getServiceAccount(String projectId) {
    return unsupported("getServiceAccount");
  }

  @Override
  public Notification createNotification(String bucket, NotificationInfo notificationInfo) {
    return unsupported("createNotification");
  }

  @Override
  public Notification getNotification(String bucket, String notificationId) {
    return unsupported("getNotification");
  }

  @Override
  public List<Notification> listNotifications(String bucket) {
    return unsupported("listNotifications");
  }

  @Override
  public boolean deleteNotification(String bucket, String notificationId) {
    return unsupported("deleteNotification");
  }

  @Override
  public BlobWriteSession blobWriteSession(BlobInfo blobInfo, BlobWriteOption... options) {
    return unsupported("blobWriteSession");
  }

  @Override
  public Blob moveBlob(MoveBlobRequest request) {
    return unsupported("moveBlob");
  }
}
