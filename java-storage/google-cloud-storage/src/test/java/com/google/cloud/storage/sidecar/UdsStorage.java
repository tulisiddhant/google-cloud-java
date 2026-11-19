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

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.paging.Page;
import com.google.cloud.NoCredentials;
import com.google.cloud.PageImpl;
import com.google.cloud.ReadChannel;
import com.google.cloud.RestorableState;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.GrpcStorageOptions;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadAsChannel;
import com.google.cloud.storage.ReadAsFutureBytes;
import com.google.cloud.storage.ReadProjectionConfig;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.UdsVeneerBridge;
import com.google.cloud.storage.UdsVeneerBridge.SourcePreconditions;
import com.google.cloud.storage.UdsVeneerBridge.TargetPreconditions;
import com.google.protobuf.ByteString;
import gcscore.v1.AppendRequest;
import gcscore.v1.AppendResponse;
import gcscore.v1.CloseObjectRequest;
import gcscore.v1.DeleteObjectRequest;
import gcscore.v1.Empty;
import gcscore.v1.GcsCoreGrpc;
import gcscore.v1.GetObjectRequest;
import gcscore.v1.ListBucketsRequest;
import gcscore.v1.ListBucketsResponse;
import gcscore.v1.ListObjectsRequest;
import gcscore.v1.ListObjectsResponse;
import gcscore.v1.ListedBucket;
import gcscore.v1.ListedObject;
import gcscore.v1.ObjectMeta;
import gcscore.v1.OpenObjectRequest;
import gcscore.v1.OpenObjectResponse;
import gcscore.v1.PingRequest;
import gcscore.v1.PingResponse;
import gcscore.v1.ReadChunk;
import gcscore.v1.ReadObjectRequest;
import gcscore.v1.ReadRangeRequest;
import gcscore.v1.StatsRequest;
import gcscore.v1.StatsResponse;
import gcscore.v1.Transport;
import gcscore.v1.WriteFinish;
import gcscore.v1.WriteHeader;
import gcscore.v1.WriteObjectResponse;
import gcscore.v1.WriteRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ScatteringByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * High-level {@link Storage} Veneer Client implementation backed by the Rust GCS core over Unix
 * Domain Sockets ({@code gcscore.v1.GcsCore}), supporting both out-of-process ({@code uds-sidecar})
 * and in-process JNI ({@code uds-inproc}) deployment streams across {@link Transport#GRPC} and
 * {@link Transport#JSON}.
 */
public final class UdsStorage implements InvocationHandler, AutoCloseable {

  private static final int CHUNK_SIZE = 2 * 1024 * 1024; // 2 MiB streaming frame size
  private static final boolean DEBUG_LOG = Boolean.getBoolean("gcs.uds.debug");

  /** Deployment topology when building a {@link Storage} client via {@link #newBuilder}. */
  public enum DriverMode {
    SIDECAR,
    INPROC
  }

  /**
   * Uniform builder wrapping a standard {@link StorageOptions} configuration (POC Step 1) so
   * callers instantiate {@code uds-sidecar} or {@code uds-inproc} with the same configuration
   * surface as Native Java {@link StorageOptions#getService()}.
   */
  public static final class Builder {
    private final StorageOptions baseOptions;
    private DriverMode driverMode = DriverMode.SIDECAR;
    private Transport transport;
    private ManagedChannel explicitChannel;
    private String socketPath;
    private boolean requireSession =
        Boolean.parseBoolean(
            System.getProperty(
                "gcs.sidecar.require.session",
                System.getProperty("gcs.live.benchmark", System.getProperty("gcs.bench.live", "false"))));

    private Builder(StorageOptions baseOptions) {
      this.baseOptions =
          baseOptions != null
              ? baseOptions
              : StorageOptions.http().setCredentials(NoCredentials.getInstance()).build();
      this.transport =
          (this.baseOptions instanceof GrpcStorageOptions) ? Transport.GRPC : Transport.JSON;
    }

    public Builder setDriverMode(DriverMode driverMode) {
      if (driverMode != null) {
        this.driverMode = driverMode;
      }
      return this;
    }

    public Builder setTransport(Transport transport) {
      if (transport != null && transport != Transport.UNRECOGNIZED) {
        this.transport = transport;
      }
      return this;
    }

    public Builder setChannel(ManagedChannel channel) {
      this.explicitChannel = channel;
      return this;
    }

    public Builder setSocketPath(String socketPath) {
      this.socketPath = socketPath;
      return this;
    }

    public Builder setRequireSession(boolean requireSession) {
      this.requireSession = requireSession;
      return this;
    }

    public Storage build() {
      AutoCloseable ownedServer = null;
      ManagedChannel ch = this.explicitChannel;
      boolean ownsChannel = false;
      try {
        if (ch == null) {
          String resolvedSock = this.socketPath;
          if (resolvedSock == null || resolvedSock.isEmpty()) {
            if (driverMode == DriverMode.INPROC) {
              ownedServer = startInprocReflectively();
              Method getSock = ownedServer.getClass().getMethod("getSocketPath");
              resolvedSock = ((Path) getSock.invoke(ownedServer)).toAbsolutePath().toString();
            } else {
              RustSidecarProcessManager mgr = RustSidecarProcessManager.createDefault();
              mgr.start(Duration.ofSeconds(15));
              ownedServer = mgr;
              resolvedSock = mgr.getSocketSpec();
            }
          }
          ch = UdsChannelFactory.createChannel(resolvedSock);
          ownsChannel = true;
        }

        GcsCoreGrpc.GcsCoreBlockingStub rawStub = GcsCoreGrpc.newBlockingStub(ch);
        GcsSessionManager sm =
            new GcsSessionManager(
                rawStub, baseOptions.getCredentials(), /* allowDummyFallback= */ !requireSession);
        try {
          sm.initSession(null, baseOptions.getQuotaProjectId());
        } catch (RuntimeException e) {
          if (requireSession) {
            throw e;
          }
          sm = null;
        }

        UdsStorage handler =
            new UdsStorage(ch, ownsChannel, ownedServer, null, sm, baseOptions, transport);
        Storage proxy =
            (Storage)
                Proxy.newProxyInstance(
                    Storage.class.getClassLoader(),
                    new Class<?>[] {Storage.class, AutoCloseable.class},
                    handler);
        handler.proxyInstance = proxy;
        return proxy;
      } catch (Exception e) {
        if (ownsChannel && ch != null) {
          UdsChannelFactory.shutdownChannel(ch, 1, TimeUnit.SECONDS);
        }
        if (ownedServer != null) {
          try {
            ownedServer.close();
          } catch (Exception ignored) {
          }
        }
        if (e instanceof RuntimeException) {
          throw (RuntimeException) e;
        }
        throw new IllegalStateException("Failed to initialize UdsStorage client", e);
      }
    }

    private static AutoCloseable startInprocReflectively() throws Exception {
      Class<?> cls =
          Class.forName("com.google.cloud.storage.sidecar.RustInprocSidecarManager");
      AutoCloseable mgr = (AutoCloseable) cls.getMethod("createDefault").invoke(null);
      cls.getMethod("start", Duration.class).invoke(mgr, Duration.ofSeconds(15));
      return mgr;
    }
  }

  public static Builder newBuilder(StorageOptions baseOptions) {
    return new Builder(baseOptions);
  }

  private final ManagedChannel channel;
  private final boolean ownsChannel;
  private final AutoCloseable ownedServer;
  private final Channel callChannel;
  private final GcsSessionManager sessionManager;
  private final GcsCoreGrpc.GcsCoreBlockingStub blockingStub;
  private final GcsCoreGrpc.GcsCoreStub asyncStub;
  private final Storage delegate;
  private final StorageOptions options;
  private final Transport clientTransport;
  private Storage proxyInstance;

  private UdsStorage(
      ManagedChannel channel,
      boolean ownsChannel,
      AutoCloseable ownedServer,
      Storage delegate,
      GcsSessionManager sessionManager,
      StorageOptions options,
      Transport clientTransport) {
    this.channel = channel;
    this.ownsChannel = ownsChannel;
    this.ownedServer = ownedServer;
    this.delegate = delegate;
    this.sessionManager = sessionManager;
    this.options = options != null ? options : buildDefaultStorageOptions(clientTransport);
    this.clientTransport =
        (clientTransport != null
                && clientTransport != Transport.TRANSPORT_UNSPECIFIED
                && clientTransport != Transport.UNRECOGNIZED)
            ? clientTransport
            : resolveConfiguredTransport();

    Channel intercepted = channel;
    if (sessionManager != null) {
      intercepted = ClientInterceptors.intercept(channel, sessionManager.createClientInterceptor());
    }
    this.callChannel = intercepted;
    this.blockingStub = GcsCoreGrpc.newBlockingStub(intercepted);
    this.asyncStub = GcsCoreGrpc.newStub(intercepted);
  }

  private static StorageOptions buildDefaultStorageOptions(Transport transport) {
    String project =
        System.getProperty(
            "gcs.project.id",
            System.getProperty(
                "gcs.sidecar.project",
                System.getenv("GOOGLE_CLOUD_PROJECT") != null
                    ? System.getenv("GOOGLE_CLOUD_PROJECT")
                    : "vaibhavpratap-sdk-test"));
    StorageOptions.Builder b =
        (transport == Transport.GRPC) ? StorageOptions.grpc() : StorageOptions.http();
    b.setProjectId(project);
    b.setCredentials(NoCredentials.getInstance());
    return b.build();
  }

  private static Transport resolveConfiguredTransport() {
    String explicit = System.getProperty("gcs.sidecar.transport");
    if ("json".equalsIgnoreCase(explicit)) {
      return Transport.JSON;
    }
    if ("grpc".equalsIgnoreCase(explicit)) {
      return Transport.GRPC;
    }
    String def = System.getProperty("gcs.sidecar.default.transport", "grpc");
    return "json".equalsIgnoreCase(def) ? Transport.JSON : Transport.GRPC;
  }

  public static Storage create(ManagedChannel channel) {
    return create(channel, resolveConfiguredTransport(), /* ownsChannel= */ false);
  }

  public static Storage create(ManagedChannel channel, Transport transport) {
    return create(channel, transport, /* ownsChannel= */ false);
  }

  private static Storage create(
      ManagedChannel channel, Transport transport, boolean ownsChannel) {
    boolean isLive =
        Boolean.parseBoolean(
            System.getProperty(
                "gcs.live.benchmark", System.getProperty("gcs.bench.live", "false")));
    boolean requireSession =
        Boolean.parseBoolean(
            System.getProperty("gcs.sidecar.require.session", String.valueOf(isLive)));

    GcsCoreGrpc.GcsCoreBlockingStub rawStub = GcsCoreGrpc.newBlockingStub(channel);
    GcsSessionManager sm =
        new GcsSessionManager(rawStub, null, /* allowDummyFallback= */ !requireSession);
    try {
      sm.initSession();
    } catch (RuntimeException e) {
      if (requireSession) {
        throw e;
      }
      System.out.println(
          "[UdsStorage] Session initialization skipped (" + e.getMessage() + ").");
      sm = null;
    }
    return createInternal(channel, ownsChannel, null, null, sm, null, transport);
  }

  public static Storage create(ManagedChannel channel, GcsSessionManager sessionManager) {
    return createInternal(
        channel,
        /* ownsChannel= */ false,
        null,
        null,
        sessionManager,
        null,
        resolveConfiguredTransport());
  }

  public static Storage create(Path socketPath) {
    ManagedChannel ch = UdsChannelFactory.createChannel(socketPath);
    return create(ch, resolveConfiguredTransport(), /* ownsChannel= */ true);
  }

  public static Storage create(String socketPathStr) {
    ManagedChannel ch = UdsChannelFactory.createChannel(socketPathStr);
    return create(ch, resolveConfiguredTransport(), /* ownsChannel= */ true);
  }

  public static Storage createWithDelegate(
      ManagedChannel channel, Storage delegate, GcsSessionManager sessionManager) {
    return createInternal(
        channel,
        /* ownsChannel= */ false,
        null,
        delegate,
        sessionManager,
        delegate != null ? delegate.getOptions() : null,
        resolveConfiguredTransport());
  }

  private static Storage createInternal(
      ManagedChannel channel,
      boolean ownsChannel,
      AutoCloseable ownedServer,
      Storage delegate,
      GcsSessionManager sessionManager,
      StorageOptions options,
      Transport transport) {
    UdsStorage handler =
        new UdsStorage(
            channel, ownsChannel, ownedServer, delegate, sessionManager, options, transport);
    Storage proxy =
        (Storage)
            Proxy.newProxyInstance(
                Storage.class.getClassLoader(),
                new Class<?>[] {Storage.class, AutoCloseable.class},
                handler);
    handler.proxyInstance = proxy;
    System.out.println(
        "[UdsStorage] Initialized Storage veneer client over gRPC UDS channel (transport="
            + handler.clientTransport
            + ").");
    return proxy;
  }

  public GcsSessionManager getSessionManager() {
    return sessionManager;
  }

  public ManagedChannel getChannel() {
    return channel;
  }

  public Transport getClientTransport() {
    return clientTransport;
  }

  private Storage selfStorage() {
    if (proxyInstance != null) {
      return proxyInstance;
    }
    return (Storage)
        Proxy.newProxyInstance(
            Storage.class.getClassLoader(),
            new Class<?>[] {Storage.class, AutoCloseable.class},
            this);
  }

  // --- Error Mapping (gRPC Status -> HTTP code for StorageException) ---

  static int statusToHttpCode(Status status) {
    if (status == null) {
      return 500;
    }
    switch (status.getCode()) {
      case INVALID_ARGUMENT:
        return 400;
      case UNAUTHENTICATED:
        return 401;
      case PERMISSION_DENIED:
        return 403;
      case NOT_FOUND:
        return 404;
      case DEADLINE_EXCEEDED:
        return 504;
      case ALREADY_EXISTS:
      case ABORTED:
        return 409;
      case FAILED_PRECONDITION:
        return 412;
      case OUT_OF_RANGE:
        return 416;
      case RESOURCE_EXHAUSTED:
        return 429;
      case CANCELLED:
        return 499;
      case UNIMPLEMENTED:
        return 501;
      case UNAVAILABLE:
        return 503;
      default:
        return 500;
    }
  }

  static StorageException toStorageException(Throwable t) {
    if (t instanceof StorageException) {
      return (StorageException) t;
    }
    Status status = Status.fromThrowable(t);
    int code = statusToHttpCode(status);
    String msg = status.getDescription() != null ? status.getDescription() : t.getMessage();
    return new StorageException(code, msg != null ? msg : status.getCode().name(), t);
  }

  // --- Dynamic Proxy Dispatcher ---

  @Override
  public java.lang.Object invoke(
      java.lang.Object proxy, Method method, java.lang.Object[] args) throws Throwable {
    String name = method.getName();
    Class<?>[] paramTypes = method.getParameterTypes();

    if ("blobReadSession".equals(name)
        && paramTypes.length >= 1
        && paramTypes[0] == BlobId.class) {
      BlobSourceOption[] opts = extractVarargs(args, 1, new BlobSourceOption[0]);
      return blobReadSession((BlobId) args[0], opts);
    } else if ("blobAppendableUpload".equals(name)
        && paramTypes.length >= 2
        && paramTypes[0] == BlobInfo.class
        && paramTypes[1] == BlobAppendableUploadConfig.class) {
      BlobWriteOption[] opts = extractVarargs(args, 2, new BlobWriteOption[0]);
      return blobAppendableUpload(
          (BlobInfo) args[0], (BlobAppendableUploadConfig) args[1], opts);
    } else if ("reader".equals(name)) {
      if (paramTypes.length >= 1 && paramTypes[0] == BlobId.class) {
        BlobSourceOption[] opts = extractVarargs(args, 1, new BlobSourceOption[0]);
        return reader((BlobId) args[0], opts);
      } else if (paramTypes.length >= 2
          && paramTypes[0] == String.class
          && paramTypes[1] == String.class) {
        BlobSourceOption[] opts = extractVarargs(args, 2, new BlobSourceOption[0]);
        return reader((String) args[0], (String) args[1], opts);
      }
    } else if ("writer".equals(name)
        && paramTypes.length >= 1
        && paramTypes[0] == BlobInfo.class) {
      BlobWriteOption[] opts = extractVarargs(args, 1, new BlobWriteOption[0]);
      return writer((BlobInfo) args[0], opts);
    } else if ("list".equals(name)) {
      if (paramTypes.length >= 1 && paramTypes[0] == String.class) {
        BlobListOption[] opts = extractVarargs(args, 1, new BlobListOption[0]);
        return list((String) args[0], opts);
      } else if (paramTypes.length == 0
          || (paramTypes.length == 1 && paramTypes[0] == BucketListOption[].class)) {
        BucketListOption[] opts = extractVarargs(args, 0, new BucketListOption[0]);
        return listBuckets(opts);
      }
    } else if ("get".equals(name)) {
      if (paramTypes.length >= 1 && paramTypes[0] == BlobId.class) {
        BlobGetOption[] opts = extractVarargs(args, 1, new BlobGetOption[0]);
        return get((BlobId) args[0], opts);
      } else if (paramTypes.length >= 2
          && paramTypes[0] == String.class
          && paramTypes[1] == String.class) {
        BlobGetOption[] opts = extractVarargs(args, 2, new BlobGetOption[0]);
        return get((String) args[0], (String) args[1], opts);
      }
    } else if ("delete".equals(name)) {
      if (paramTypes.length >= 1 && paramTypes[0] == BlobId.class) {
        BlobSourceOption[] opts = extractVarargs(args, 1, new BlobSourceOption[0]);
        return delete((BlobId) args[0], opts);
      } else if (paramTypes.length >= 2
          && paramTypes[0] == String.class
          && paramTypes[1] == String.class) {
        BlobSourceOption[] opts = extractVarargs(args, 2, new BlobSourceOption[0]);
        return delete((String) args[0], (String) args[1], opts);
      }
    } else if ("create".equals(name) && paramTypes.length >= 1 && paramTypes[0] == BlobInfo.class) {
      if (paramTypes.length == 1 || paramTypes[1] == BlobTargetOption[].class) {
        BlobTargetOption[] opts = extractVarargs(args, 1, new BlobTargetOption[0]);
        return create((BlobInfo) args[0], new byte[0], opts);
      } else if (paramTypes[1] == byte[].class) {
        BlobTargetOption[] opts = extractVarargs(args, 2, new BlobTargetOption[0]);
        byte[] content = args[1] != null ? (byte[]) args[1] : new byte[0];
        return create((BlobInfo) args[0], content, opts);
      } else if (InputStream.class.isAssignableFrom(paramTypes[1])) {
        BlobWriteOption[] opts = extractVarargs(args, 2, new BlobWriteOption[0]);
        return createFrom((BlobInfo) args[0], (InputStream) args[1], opts);
      }
    } else if ("createFrom".equals(name) && paramTypes.length >= 2 && paramTypes[0] == BlobInfo.class) {
      if (paramTypes[1] == Path.class) {
        int optIdx = (paramTypes.length >= 3 && paramTypes[2] == int.class) ? 3 : 2;
        BlobWriteOption[] opts = extractVarargs(args, optIdx, new BlobWriteOption[0]);
        return createFrom((BlobInfo) args[0], (Path) args[1], opts);
      } else if (InputStream.class.isAssignableFrom(paramTypes[1])) {
        int optIdx = (paramTypes.length >= 3 && paramTypes[2] == int.class) ? 3 : 2;
        BlobWriteOption[] opts = extractVarargs(args, optIdx, new BlobWriteOption[0]);
        return createFrom((BlobInfo) args[0], (InputStream) args[1], opts);
      }
    } else if ("downloadTo".equals(name) && paramTypes.length >= 2) {
      if (paramTypes[0] == BlobId.class && paramTypes[1] == Path.class) {
        BlobSourceOption[] opts = extractVarargs(args, 2, new BlobSourceOption[0]);
        downloadTo((BlobId) args[0], (Path) args[1], opts);
        return null;
      } else if (paramTypes[0] == BlobId.class
          && OutputStream.class.isAssignableFrom(paramTypes[1])) {
        BlobSourceOption[] opts = extractVarargs(args, 2, new BlobSourceOption[0]);
        downloadTo((BlobId) args[0], (OutputStream) args[1], opts);
        return null;
      }
    } else if ("readAllBytes".equals(name)) {
      if (paramTypes.length >= 1 && paramTypes[0] == BlobId.class) {
        BlobSourceOption[] opts = extractVarargs(args, 1, new BlobSourceOption[0]);
        return readAllBytes((BlobId) args[0], opts);
      } else if (paramTypes.length >= 2
          && paramTypes[0] == String.class
          && paramTypes[1] == String.class) {
        BlobSourceOption[] opts = extractVarargs(args, 2, new BlobSourceOption[0]);
        return readAllBytes((String) args[0], (String) args[1], opts);
      }
    } else if ("ping".equals(name) && paramTypes.length == 1 && paramTypes[0] == byte[].class) {
      return ping((byte[]) args[0]);
    } else if ("getStats".equals(name)) {
      boolean reset = paramTypes.length == 1 && Boolean.TRUE.equals(args[0]);
      return getStats(reset);
    } else if ("getSessionManager".equals(name)) {
      return sessionManager;
    } else if ("getChannel".equals(name)) {
      return channel;
    } else if ("close".equals(name)) {
      close();
      return null;
    } else if ("getOptions".equals(name)) {
      return options;
    } else if ("toString".equals(name)) {
      return "UdsStorage[gcscore.v1, transport="
          + clientTransport
          + ", session="
          + (sessionManager != null ? sessionManager.getSessionId() : "none")
          + "]";
    } else if ("hashCode".equals(name)) {
      return System.identityHashCode(this);
    } else if ("equals".equals(name)) {
      return proxy == args[0];
    }

    if (delegate != null) {
      return method.invoke(delegate, args);
    }
    throw new UnsupportedOperationException(
        "[UdsStorage] Method "
            + method.getName()
            + " is outside the 6-API scope of the GCS UDS POC.");
  }

  @SuppressWarnings("unchecked")
  private static <T> T[] extractVarargs(java.lang.Object[] args, int index, T[] empty) {
    if (args == null || index >= args.length || args[index] == null) {
      return empty;
    }
    return (T[]) args[index];
  }

  // =========================================================================
  // 1. BidiReadObject — Storage.blobReadSession(BlobId, BlobSourceOption...)
  // =========================================================================

  public ApiFuture<BlobReadSession> blobReadSession(BlobId id, BlobSourceOption... options) {
    final SettableApiFuture<BlobReadSession> future = SettableApiFuture.create();
    try {
      SourcePreconditions pre = UdsVeneerBridge.extractSourceOptions(id, options);
      OpenObjectRequest.Builder req =
          OpenObjectRequest.newBuilder().setBucket(id.getBucket()).setObject(id.getName());
      if (pre.generation != null && pre.generation > 0) {
        req.setGeneration(pre.generation);
      }
      if (pre.ifGenerationMatch != null && pre.ifGenerationMatch >= 0) {
        req.setIfGenerationMatch(pre.ifGenerationMatch);
      }

      asyncStub.openObject(
          req.build(),
          new StreamObserver<OpenObjectResponse>() {
            private OpenObjectResponse opened;

            @Override
            public void onNext(OpenObjectResponse value) {
              this.opened = value;
            }

            @Override
            public void onError(Throwable t) {
              future.setException(toStorageException(t));
            }

            @Override
            public void onCompleted() {
              if (opened == null) {
                future.setException(
                    new StorageException(500, "OpenObject completed without response"));
                return;
              }
              ObjectMeta meta =
                  opened.hasMeta() ? opened.getMeta() : ObjectMeta.getDefaultInstance();
              BlobInfo info =
                  UdsVeneerBridge.blobInfoFromMeta(id.getBucket(), id.getName(), meta);
              future.set(
                  new UdsBlobReadSession(
                      callChannel, blockingStub, asyncStub, info, opened.getHandle()));
            }
          });
    } catch (Throwable t) {
      future.setException(toStorageException(t));
    }
    return future;
  }

  private static final class UdsBlobReadSession implements BlobReadSession {
    private final Channel callChannel;
    private final GcsCoreGrpc.GcsCoreBlockingStub blockingStub;
    private final GcsCoreGrpc.GcsCoreStub asyncStub;
    private final BlobInfo blobInfo;
    private final long handle;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Set<AutoCloseable> activeProjections =
        Collections.newSetFromMap(new ConcurrentHashMap<AutoCloseable, Boolean>());

    UdsBlobReadSession(
        Channel callChannel,
        GcsCoreGrpc.GcsCoreBlockingStub blockingStub,
        GcsCoreGrpc.GcsCoreStub asyncStub,
        BlobInfo blobInfo,
        long handle) {
      this.callChannel = callChannel;
      this.blockingStub = blockingStub;
      this.asyncStub = asyncStub;
      this.blobInfo = blobInfo;
      this.handle = handle;
    }

    @Override
    public BlobInfo getBlobInfo() {
      return blobInfo;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Projection> Projection readAs(ReadProjectionConfig<Projection> config) {
      if (closed.get()) {
        throw new IllegalStateException("BlobReadSession is already closed");
      }
      if (config instanceof ReadAsFutureBytes) {
        ReadAsFutureBytes fBytes = (ReadAsFutureBytes) config;
        return (Projection) readRangeAsFutureBytes(fBytes.getRange());
      } else if (config instanceof ReadAsChannel) {
        ReadAsChannel chConfig = (ReadAsChannel) config;
        return (Projection) readRangeAsChannel(chConfig.getRange());
      }
      throw new UnsupportedOperationException(
          "Unsupported ReadProjectionConfig: " + config.getClass().getName());
    }

    private ApiFuture<byte[]> readRangeAsFutureBytes(RangeSpec range) {
      final SettableApiFuture<byte[]> future = SettableApiFuture.create();
      final long offset = range.begin();
      final long requestedLen =
          range.maxLength().isPresent() ? range.maxLength().getAsLong() : -1L;

      long expectedSize = -1L;
      Long knownObjSize = blobInfo.getSize();
      if (knownObjSize != null && knownObjSize >= 0 && offset >= 0) {
        long available = Math.max(0L, knownObjSize - offset);
        expectedSize = requestedLen >= 0 ? Math.min(requestedLen, available) : available;
      } else if (requestedLen >= 0) {
        expectedSize = requestedLen;
      }

      ReadRangeRequest.Builder req =
          ReadRangeRequest.newBuilder().setHandle(handle).setOffset(offset);
      if (requestedLen > 0) {
        req.setLength(requestedLen);
      } else {
        req.setLength(-1L);
      }

      final int preallocLen =
          (expectedSize >= 0 && expectedSize <= Integer.MAX_VALUE - 8) ? (int) expectedSize : -1;
      final AtomicReference<ClientCallStreamObserver<ReadRangeRequest>> callRef =
          new AtomicReference<ClientCallStreamObserver<ReadRangeRequest>>();
      final AutoCloseable canceller =
          new AutoCloseable() {
            @Override
            public void close() {
              ClientCallStreamObserver<ReadRangeRequest> obs = callRef.get();
              if (obs != null) {
                obs.cancel("BlobReadSession closed", null);
              }
              future.setException(
                  new StorageException(499, "BlobReadSession closed before range read completed"));
            }
          };
      activeProjections.add(canceller);

      asyncStub.readRange(
          req.build(),
          new ClientResponseObserver<ReadRangeRequest, ReadChunk>() {
            private byte[] directBuf = preallocLen >= 0 ? new byte[preallocLen] : null;
            private int cursor = 0;
            private ByteArrayOutputStream fallbackOut = null;

            @Override
            public void beforeStart(final ClientCallStreamObserver<ReadRangeRequest> requestStream) {
              callRef.set(requestStream);
              if (closed.get()) {
                requestStream.cancel("BlobReadSession already closed", null);
              }
            }

            @Override
            public void onNext(ReadChunk chunk) {
              for (ByteString bs : chunk.getDataList()) {
                int len = bs.size();
                if (len == 0) {
                  continue;
                }
                if (directBuf != null && cursor + len <= directBuf.length) {
                  bs.copyTo(directBuf, cursor);
                  cursor += len;
                } else {
                  if (fallbackOut == null) {
                    fallbackOut =
                        new ByteArrayOutputStream(Math.max(64 * 1024, cursor + len));
                    if (directBuf != null && cursor > 0) {
                      fallbackOut.write(directBuf, 0, cursor);
                      directBuf = null;
                    }
                  }
                  try {
                    bs.writeTo(fallbackOut);
                  } catch (IOException e) {
                    // ByteArrayOutputStream does not throw IOException on write
                  }
                }
              }
            }

            @Override
            public void onError(Throwable t) {
              activeProjections.remove(canceller);
              future.setException(toStorageException(t));
            }

            @Override
            public void onCompleted() {
              activeProjections.remove(canceller);
              if (fallbackOut != null) {
                future.set(fallbackOut.toByteArray());
              } else if (directBuf != null) {
                if (cursor == directBuf.length) {
                  future.set(directBuf);
                } else {
                  byte[] trimmed = new byte[cursor];
                  System.arraycopy(directBuf, 0, trimmed, 0, cursor);
                  future.set(trimmed);
                }
              } else {
                future.set(new byte[0]);
              }
            }
          });

      if (closed.get()) {
        try {
          canceller.close();
        } catch (Exception ignored) {
        }
      }
      return future;
    }

    private ScatteringByteChannel readRangeAsChannel(RangeSpec range) {
      long offset = range.begin();
      long requestedLen = range.maxLength().isPresent() ? range.maxLength().getAsLong() : -1L;
      ReadRangeRequest req =
          ReadRangeRequest.newBuilder()
              .setHandle(handle)
              .setOffset(offset)
              .setLength(requestedLen > 0 ? requestedLen : -1L)
              .build();
      final UdsRangeScatteringChannel sc =
          new UdsRangeScatteringChannel(callChannel, req, activeProjections);
      activeProjections.add(sc);
      sc.start();
      if (closed.get()) {
        try {
          sc.close();
        } catch (IOException ignored) {
        }
      }
      return sc;
    }

    @Override
    public void close() throws IOException {
      if (closed.compareAndSet(false, true)) {
        for (AutoCloseable proj : activeProjections) {
          try {
            proj.close();
          } catch (Exception ignored) {
          }
        }
        activeProjections.clear();
        try {
          blockingStub
              .withDeadlineAfter(10, TimeUnit.SECONDS)
              .closeObject(CloseObjectRequest.newBuilder().setHandle(handle).build());
        } catch (Throwable t) {
          throw new IOException(
              "Failed to close BidiRead session on handle " + handle + ": " + t.getMessage(),
              toStorageException(t));
        }
      }
    }
  }

  /** Non-blocking {@link ScatteringByteChannel} for {@link ReadAsChannel} with explicit cancel. */
  private static final class UdsRangeScatteringChannel
      implements ScatteringByteChannel, AutoCloseable {
    private final Channel callChannel;
    private final ReadRangeRequest request;
    private final Set<AutoCloseable> registry;
    private final Queue<ByteBuffer> sliceQueue = new ConcurrentLinkedQueue<ByteBuffer>();
    private final AtomicReference<Throwable> terminalError = new AtomicReference<Throwable>();
    private volatile boolean streamDone = false;
    private volatile boolean open = true;
    private ClientCall<ReadRangeRequest, ReadChunk> call;
    private ByteBuffer current;

    UdsRangeScatteringChannel(
        Channel callChannel, ReadRangeRequest request, Set<AutoCloseable> registry) {
      this.callChannel = callChannel;
      this.request = request;
      this.registry = registry;
    }

    void start() {
      this.call = callChannel.newCall(GcsCoreGrpc.getReadRangeMethod(), CallOptions.DEFAULT);
      this.call.start(
          new ClientCall.Listener<ReadChunk>() {
            @Override
            public void onMessage(ReadChunk message) {
              for (ByteString bs : message.getDataList()) {
                if (!bs.isEmpty()) {
                  sliceQueue.offer(bs.asReadOnlyByteBuffer());
                }
              }
              if (open && sliceQueue.size() < 8) {
                call.request(1);
              }
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              if (!status.isOk() && status.getCode() != Status.Code.CANCELLED) {
                terminalError.compareAndSet(null, status.asRuntimeException(trailers));
              }
              streamDone = true;
              registry.remove(UdsRangeScatteringChannel.this);
            }
          },
          new Metadata());
      this.call.sendMessage(request);
      this.call.halfClose();
      this.call.request(4);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      long n = read(new ByteBuffer[] {dst}, 0, 1);
      return n == -1L ? -1 : (int) n;
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
      return read(dsts, 0, dsts.length);
    }

    @Override
    public synchronized long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
      if (!open) {
        throw new ClosedChannelException();
      }
      Throwable err = terminalError.get();
      if (err != null) {
        throw new IOException("ReadAsChannel stream failed", toStorageException(err));
      }

      long totalCopied = 0;
      for (int i = offset; i < offset + length; i++) {
        ByteBuffer dst = dsts[i];
        while (dst.hasRemaining()) {
          if (current == null || !current.hasRemaining()) {
            current = sliceQueue.poll();
            if (current != null && open && sliceQueue.size() < 4 && !streamDone && call != null) {
              call.request(1);
            }
          }
          if (current == null) {
            break;
          }
          int toCopy = Math.min(dst.remaining(), current.remaining());
          int prevLimit = current.limit();
          current.limit(current.position() + toCopy);
          dst.put(current);
          current.limit(prevLimit);
          totalCopied += toCopy;
        }
      }

      if (totalCopied > 0) {
        return totalCopied;
      }
      err = terminalError.get();
      if (err != null) {
        throw new IOException("ReadAsChannel stream failed", toStorageException(err));
      }
      if (streamDone && sliceQueue.isEmpty() && (current == null || !current.hasRemaining())) {
        return -1L;
      }
      return 0L; // Non-blocking contract per ReadAsChannel Javadoc
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public synchronized void close() throws IOException {
      if (!open) {
        return;
      }
      open = false;
      registry.remove(this);
      sliceQueue.clear();
      current = null;
      if (call != null && !streamDone) {
        call.cancel("ReadAsChannel closed by caller", null);
      }
    }
  }

  // =========================================================================
  // 2. BidiWriteObject — Storage.blobAppendableUpload(...)
  // =========================================================================

  public BlobAppendableUpload blobAppendableUpload(
      BlobInfo blobInfo, BlobAppendableUploadConfig uploadConfig, BlobWriteOption... options) {
    TargetPreconditions pre = UdsVeneerBridge.extractWriteOptions(blobInfo, options);
    return new UdsBlobAppendableUpload(asyncStub, pre.resolvedInfo, uploadConfig, pre);
  }

  private static final class UdsBlobAppendableUpload implements BlobAppendableUpload {
    private final GcsCoreGrpc.GcsCoreStub asyncStub;
    private final BlobInfo blobInfo;
    private final BlobAppendableUploadConfig config;
    private final TargetPreconditions preconditions;
    private final AtomicBoolean opened = new AtomicBoolean(false);
    private final SettableApiFuture<BlobInfo> resultFuture = SettableApiFuture.create();

    UdsBlobAppendableUpload(
        GcsCoreGrpc.GcsCoreStub asyncStub,
        BlobInfo blobInfo,
        BlobAppendableUploadConfig config,
        TargetPreconditions preconditions) {
      this.asyncStub = asyncStub;
      this.blobInfo = blobInfo;
      this.config = config != null ? config : BlobAppendableUploadConfig.of();
      this.preconditions = preconditions;
    }

    @Override
    public AppendableUploadWriteableByteChannel open() throws IOException {
      if (!opened.compareAndSet(false, true)) {
        throw new IllegalStateException("BlobAppendableUpload session may only be opened once");
      }
      UdsAppendableUploadChannel ch =
          new UdsAppendableUploadChannel(asyncStub, blobInfo, config, preconditions, resultFuture);
      ch.start();
      return ch;
    }

    @Override
    public ApiFuture<BlobInfo> getResult() {
      return resultFuture;
    }
  }

  private static final class UdsAppendableUploadChannel
      implements BlobAppendableUpload.AppendableUploadWriteableByteChannel,
          ClientResponseObserver<AppendRequest, AppendResponse> {
    private final GcsCoreGrpc.GcsCoreStub asyncStub;
    private final BlobInfo blobInfo;
    private final BlobAppendableUploadConfig config;
    private final TargetPreconditions preconditions;
    private final SettableApiFuture<BlobInfo> resultFuture;
    private final long maxPendingBytes;
    private final Object stateLock = new Object();
    private final CountDownLatch closeLatch = new CountDownLatch(1);
    private final AtomicLong bytesSent = new AtomicLong(0);
    private final AtomicLong persistedBytes = new AtomicLong(0);

    private ClientCallStreamObserver<AppendRequest> requestStream;
    private volatile Throwable terminalError;
    private volatile AppendResponse lastResponse;
    private volatile boolean open = true;
    private volatile boolean closingInitiated = false;

    UdsAppendableUploadChannel(
        GcsCoreGrpc.GcsCoreStub asyncStub,
        BlobInfo blobInfo,
        BlobAppendableUploadConfig config,
        TargetPreconditions preconditions,
        SettableApiFuture<BlobInfo> resultFuture) {
      this.asyncStub = asyncStub;
      this.blobInfo = blobInfo;
      this.config = config;
      this.preconditions = preconditions;
      this.resultFuture = resultFuture;
      this.maxPendingBytes =
          Math.max(64L * 1024L, UdsVeneerBridge.maxPendingBytes(config));
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<AppendRequest> requestStream) {
      this.requestStream = requestStream;
      requestStream.setOnReadyHandler(
          new Runnable() {
            @Override
            public void run() {
              synchronized (stateLock) {
                stateLock.notifyAll();
              }
            }
          });
    }

    @Override
    public void onNext(AppendResponse value) {
      synchronized (stateLock) {
        this.lastResponse = value;
        if (value.getPersistedSize() > persistedBytes.get()) {
          persistedBytes.set(value.getPersistedSize());
        }
        stateLock.notifyAll();
      }
    }

    @Override
    public void onError(Throwable t) {
      StorageException se = toStorageException(t);
      synchronized (stateLock) {
        this.terminalError = se;
        stateLock.notifyAll();
      }
      resultFuture.setException(se);
      closeLatch.countDown();
    }

    @Override
    public void onCompleted() {
      AppendResponse resp = this.lastResponse;
      if (resp != null) {
        BlobInfo resolved;
        if (resp.hasMeta()) {
          resolved =
              UdsVeneerBridge.blobInfoFromMeta(
                  blobInfo.getBucket(), blobInfo.getName(), resp.getMeta());
        } else {
          resolved =
              UdsVeneerBridge.blobInfoFromWriteResult(
                  blobInfo,
                  resp.getPersistedSize(),
                  resp.getGeneration(),
                  resp.getCrc32C());
        }
        resultFuture.set(resolved);
      } else if (!resultFuture.isDone()) {
        resultFuture.set(blobInfo);
      }
      synchronized (stateLock) {
        stateLock.notifyAll();
      }
      closeLatch.countDown();
    }

    void start() {
      asyncStub.appendObject(this);
      WriteHeader.Builder hb =
          WriteHeader.newBuilder()
              .setBucket(blobInfo.getBucket())
              .setObject(blobInfo.getName())
              .setTransport(Transport.GRPC)
              .setAppendable(true)
              .setFinalizeOnClose(config.getCloseAction() == CloseAction.FINALIZE_WHEN_CLOSING);
      if (blobInfo.getContentType() != null && !blobInfo.getContentType().isEmpty()) {
        hb.setContentType(blobInfo.getContentType());
      }
      if (preconditions.ifGenerationMatch != null && preconditions.ifGenerationMatch >= 0) {
        hb.setIfGenerationMatch(preconditions.ifGenerationMatch);
      }
      if (blobInfo.getGeneration() != null && blobInfo.getGeneration() > 0) {
        hb.setGeneration(blobInfo.getGeneration());
      }
      requestStream.onNext(AppendRequest.newBuilder().setHeader(hb.build()).build());
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      if (!open) {
        throw new ClosedChannelException();
      }
      checkError();
      if (!src.hasRemaining()) {
        return 0;
      }

      // Non-blocking write contract: respect outbound queue window and gRPC stream readiness.
      long unacked = bytesSent.get() - persistedBytes.get();
      if (!requestStream.isReady() && unacked >= maxPendingBytes) {
        return 0;
      }

      int totalWritten = 0;
      while (src.hasRemaining()) {
        checkError();
        if (!requestStream.isReady() && totalWritten > 0) {
          break;
        }
        int step = Math.min(src.remaining(), CHUNK_SIZE);
        ByteString slice = ByteString.copyFrom(src, step);
        synchronized (stateLock) {
          requestStream.onNext(AppendRequest.newBuilder().setData(slice).build());
          bytesSent.addAndGet(step);
        }
        totalWritten += step;
      }
      return totalWritten;
    }

    @Override
    public void flush() throws IOException {
      if (!open) {
        throw new ClosedChannelException();
      }
      checkError();
      final long target = bytesSent.get();
      synchronized (stateLock) {
        requestStream.onNext(
            AppendRequest.newBuilder().setFlush(Empty.getDefaultInstance()).build());
        while (persistedBytes.get() < target && terminalError == null) {
          try {
            stateLock.wait(200);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            InterruptedIOException iio = new InterruptedIOException("Interrupted awaiting flush");
            iio.initCause(ie);
            throw iio;
          }
        }
      }
      checkError();
    }

    @Override
    public void finalizeAndClose() throws IOException {
      finalizeAndClose(null);
    }

    @Override
    public void finalizeAndClose(String expectedCrc32c) throws IOException {
      finishInternal(/* finalizeUpload= */ true, expectedCrc32c);
    }

    @Override
    public void closeWithoutFinalizing() throws IOException {
      finishInternal(/* finalizeUpload= */ false, null);
    }

    @Override
    public void close() throws IOException {
      close(null);
    }

    @Override
    public void close(String expectedCrc32c) throws IOException {
      boolean shouldFinalize = (config.getCloseAction() == CloseAction.FINALIZE_WHEN_CLOSING);
      if (expectedCrc32c != null && !shouldFinalize) {
        throw new IllegalArgumentException(
            "expectedCrc32c requires CloseAction.FINALIZE_WHEN_CLOSING");
      }
      finishInternal(shouldFinalize, expectedCrc32c);
    }

    private void finishInternal(boolean finalizeUpload, String expectedCrc32c) throws IOException {
      synchronized (stateLock) {
        if (closingInitiated) {
          return; // Mutually exclusive / idempotent per BlobAppendableUpload Javadoc
        }
        closingInitiated = true;
        open = false;
      }

      WriteFinish.Builder fb = WriteFinish.newBuilder().setFinalize(finalizeUpload);
      if (expectedCrc32c != null) {
        int crcBits = UdsVeneerBridge.decodeCrc32cBase64(expectedCrc32c);
        fb.setExpectedCrc32C(crcBits);
      }
      synchronized (stateLock) {
        if (terminalError == null) {
          requestStream.onNext(AppendRequest.newBuilder().setFinish(fb.build()).build());
          requestStream.onCompleted();
        }
      }

      try {
        if (!closeLatch.await(300, TimeUnit.SECONDS)) {
          requestStream.cancel("Appendable upload close timed out", null);
          throw new IOException("Timed out awaiting appendable upload completion over UDS");
        }
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        InterruptedIOException iio =
            new InterruptedIOException("Interrupted awaiting appendable upload close");
        iio.initCause(ie);
        throw iio;
      }
      checkError();
    }

    private void checkError() throws IOException {
      Throwable err = terminalError;
      if (err != null) {
        StorageException se = toStorageException(err);
        throw new IOException("Appendable upload failed over UDS: " + se.getMessage(), se);
      }
    }
  }

  // =========================================================================
  // 3. ReadObject — Storage.reader(...) with Explicit Stream Cancellation
  // =========================================================================

  public ReadChannel reader(BlobId blob, BlobSourceOption... options) {
    SourcePreconditions pre = UdsVeneerBridge.extractSourceOptions(blob, options);
    return new UdsReadChannel(
        callChannel, blob.getBucket(), blob.getName(), clientTransport, pre);
  }

  public ReadChannel reader(String bucket, String blob, BlobSourceOption... options) {
    return reader(BlobId.of(bucket, blob), options);
  }

  public static final class UdsReadChannel implements ReadChannel {
    private static final java.lang.Object EOF_MARKER = new java.lang.Object();

    private final Channel callChannel;
    private final String bucket;
    private final String object;
    private final Transport transport;
    private final SourcePreconditions preconditions;

    private LinkedBlockingQueue<java.lang.Object> chunkQueue;
    private ClientCall<ReadObjectRequest, ReadChunk> activeCall;
    private ReadChunk currentChunk;
    private int sliceIndex = 0;
    private ByteBuffer currentSliceBuf;
    private boolean open = true;
    private boolean streamFinished = false;
    private long position = 0;
    private long limit = Long.MAX_VALUE;
    private int chunkSize = CHUNK_SIZE;

    public UdsReadChannel(
        Channel callChannel,
        String bucket,
        String object,
        Transport transport,
        SourcePreconditions preconditions) {
      this.callChannel = callChannel;
      this.bucket = bucket;
      this.object = object;
      this.transport = transport;
      this.preconditions = preconditions;
    }

    private void startStreamIfNeeded() {
      if (activeCall != null) {
        return;
      }
      long reqLen = (limit == Long.MAX_VALUE) ? -1L : Math.max(0L, limit - position);
      ReadObjectRequest.Builder rb =
          ReadObjectRequest.newBuilder()
              .setBucket(bucket)
              .setObject(object)
              .setOffset(position)
              .setLength(reqLen)
              .setTransport(transport)
              .setChunkSize(chunkSize);
      if (preconditions != null) {
        if (preconditions.generation != null && preconditions.generation > 0) {
          rb.setGeneration(preconditions.generation);
        }
        if (preconditions.ifGenerationMatch != null && preconditions.ifGenerationMatch >= 0) {
          rb.setIfGenerationMatch(preconditions.ifGenerationMatch);
        }
      }

      final LinkedBlockingQueue<java.lang.Object> queue =
          new LinkedBlockingQueue<java.lang.Object>();
      final ClientCall<ReadObjectRequest, ReadChunk> call =
          callChannel.newCall(GcsCoreGrpc.getReadObjectMethod(), CallOptions.DEFAULT);
      this.chunkQueue = queue;
      this.activeCall = call;
      this.streamFinished = false;

      call.start(
          new ClientCall.Listener<ReadChunk>() {
            @Override
            public void onMessage(ReadChunk message) {
              queue.offer(message);
            }

            @Override
            public void onClose(Status status, Metadata trailers) {
              if (status.isOk() || status.getCode() == Status.Code.CANCELLED) {
                queue.offer(EOF_MARKER);
              } else {
                queue.offer(status.asRuntimeException(trailers));
              }
            }
          },
          new Metadata());
      call.sendMessage(rb.build());
      call.halfClose();
      call.request(2);
    }

    private void cancelActiveStream() {
      ClientCall<ReadObjectRequest, ReadChunk> call = this.activeCall;
      this.activeCall = null;
      this.chunkQueue = null;
      this.currentChunk = null;
      this.currentSliceBuf = null;
      this.sliceIndex = 0;
      this.streamFinished = false;
      if (call != null) {
        try {
          call.cancel("ReadChannel closed or repositioned", null);
        } catch (Throwable ignored) {
        }
      }
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
      if (!open) {
        throw new ClosedChannelException();
      }
      if (!dst.hasRemaining()) {
        return 0;
      }
      if (position >= limit) {
        cancelActiveStream();
        return -1;
      }
      startStreamIfNeeded();

      int totalRead = 0;
      while (dst.hasRemaining() && position < limit) {
        if (currentSliceBuf != null && currentSliceBuf.hasRemaining()) {
          int maxAllowed = (int) Math.min((long) dst.remaining(), limit - position);
          int count = Math.min(maxAllowed, currentSliceBuf.remaining());
          int oldLimit = currentSliceBuf.limit();
          currentSliceBuf.limit(currentSliceBuf.position() + count);
          dst.put(currentSliceBuf);
          currentSliceBuf.limit(oldLimit);
          totalRead += count;
          position += count;
        } else if (currentChunk != null && sliceIndex < currentChunk.getDataCount()) {
          ByteString slice = currentChunk.getData(sliceIndex++);
          currentSliceBuf = slice.asReadOnlyByteBuffer();
        } else if (!streamFinished) {
          java.lang.Object item;
          try {
            item =
                totalRead > 0
                    ? chunkQueue.poll()
                    : chunkQueue.poll(120, TimeUnit.SECONDS);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            InterruptedIOException iio = new InterruptedIOException("Interrupted during UDS read");
            iio.initCause(ie);
            throw iio;
          }
          if (item == null) {
            if (totalRead > 0) {
              break;
            }
            cancelActiveStream();
            throw new IOException("Timed out waiting for ReadChunk over UDS");
          }
          if (item == EOF_MARKER) {
            streamFinished = true;
            break;
          }
          if (item instanceof Throwable) {
            Throwable t = (Throwable) item;
            cancelActiveStream();
            throw new IOException("UDS read failed: " + t.getMessage(), toStorageException(t));
          }
          currentChunk = (ReadChunk) item;
          sliceIndex = 0;
          if (activeCall != null) {
            activeCall.request(1);
          }
          if (currentChunk.getDataCount() > 0) {
            currentSliceBuf = currentChunk.getData(sliceIndex++).asReadOnlyByteBuffer();
          }
        } else {
          break;
        }
      }

      if (position >= limit) {
        cancelActiveStream();
      }
      if (totalRead == 0 && (streamFinished || position >= limit)) {
        return -1;
      }
      return totalRead;
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() {
      if (!open) {
        return;
      }
      open = false;
      cancelActiveStream();
    }

    @Override
    public void seek(long newPosition) throws IOException {
      if (!open) {
        throw new ClosedChannelException();
      }
      if (newPosition < 0) {
        throw new IllegalArgumentException("seek position must be >= 0");
      }
      if (this.position != newPosition) {
        cancelActiveStream();
        this.position = newPosition;
      }
    }

    @Override
    public void setChunkSize(int chunkSize) {
      if (chunkSize > 0) {
        this.chunkSize = Math.max(64 * 1024, chunkSize);
      }
    }

    @Override
    public ReadChannel limit(long newLimit) {
      if (newLimit < 0) {
        throw new IllegalArgumentException("limit must be >= 0");
      }
      if (this.limit != newLimit) {
        cancelActiveStream();
        this.limit = newLimit;
      }
      return this;
    }

    @Override
    public long limit() {
      return limit;
    }

    @Override
    public RestorableState<ReadChannel> capture() {
      throw new UnsupportedOperationException("capture not supported in UDS POC");
    }
  }

  // =========================================================================
  // 4. WriteObject — Storage.writer(...) with Zero Intermediate byte[] Copy
  // =========================================================================

  public WriteChannel writer(BlobInfo blobInfo, BlobWriteOption... options) {
    TargetPreconditions pre = UdsVeneerBridge.extractWriteOptions(blobInfo, options);
    return new UdsWriteChannel(asyncStub, pre.resolvedInfo, clientTransport, pre);
  }

  public static final class UdsWriteChannel
      implements WriteChannel, ClientResponseObserver<WriteRequest, WriteObjectResponse> {
    private final GcsCoreGrpc.GcsCoreStub asyncStub;
    private final BlobInfo blobInfo;
    private final Transport transport;
    private final TargetPreconditions preconditions;
    private final CountDownLatch latch = new CountDownLatch(1);
    private final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
    private final AtomicReference<WriteObjectResponse> responseRef =
        new AtomicReference<WriteObjectResponse>();
    private final Object readyLock = new Object();

    private ClientCallStreamObserver<WriteRequest> requestStream;
    private boolean headerSent = false;
    private boolean open = true;
    private int chunkSize = CHUNK_SIZE;

    public UdsWriteChannel(
        GcsCoreGrpc.GcsCoreStub asyncStub,
        BlobInfo blobInfo,
        Transport transport,
        TargetPreconditions preconditions) {
      this.asyncStub = asyncStub;
      this.blobInfo = blobInfo;
      this.transport = transport;
      this.preconditions = preconditions;
    }

    @Override
    public void beforeStart(ClientCallStreamObserver<WriteRequest> requestStream) {
      this.requestStream = requestStream;
      requestStream.setOnReadyHandler(
          new Runnable() {
            @Override
            public void run() {
              synchronized (readyLock) {
                readyLock.notifyAll();
              }
            }
          });
    }

    @Override
    public void onNext(WriteObjectResponse resp) {
      responseRef.set(resp);
    }

    @Override
    public void onError(Throwable t) {
      error.set(t);
      synchronized (readyLock) {
        readyLock.notifyAll();
      }
      latch.countDown();
    }

    @Override
    public void onCompleted() {
      synchronized (readyLock) {
        readyLock.notifyAll();
      }
      latch.countDown();
    }

    private void ensureInitialized() {
      if (requestStream == null) {
        asyncStub.writeObject(this);
      }
      if (!headerSent) {
        WriteHeader.Builder hb =
            WriteHeader.newBuilder()
                .setBucket(blobInfo.getBucket())
                .setObject(blobInfo.getName())
                .setChunkSize(chunkSize)
                .setTransport(transport);
        if (blobInfo.getContentType() != null && !blobInfo.getContentType().isEmpty()) {
          hb.setContentType(blobInfo.getContentType());
        }
        if (preconditions != null) {
          if (preconditions.ifGenerationMatch != null && preconditions.ifGenerationMatch >= 0) {
            hb.setIfGenerationMatch(preconditions.ifGenerationMatch);
          }
          if (preconditions.expectedObjectSize != null && preconditions.expectedObjectSize >= 0) {
            hb.setKnownSize(preconditions.expectedObjectSize);
          }
        }
        requestStream.onNext(WriteRequest.newBuilder().setHeader(hb.build()).build());
        headerSent = true;
      }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
      if (!open) {
        throw new ClosedChannelException();
      }
      if (!src.hasRemaining()) {
        return 0;
      }
      ensureInitialized();
      checkWriteError();

      int totalWritten = 0;
      while (src.hasRemaining()) {
        checkWriteError();
        if (requestStream != null) {
          synchronized (readyLock) {
            while (!requestStream.isReady() && error.get() == null && open) {
              try {
                readyLock.wait(10);
              } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                InterruptedIOException iio =
                    new InterruptedIOException("Interrupted waiting for UDS flow control");
                iio.initCause(ie);
                throw iio;
              }
            }
          }
        }
        checkWriteError();
        int toRead = Math.min(src.remaining(), chunkSize);
        ByteString slice = ByteString.copyFrom(src, toRead);
        requestStream.onNext(WriteRequest.newBuilder().setData(slice).build());
        totalWritten += toRead;
      }
      return totalWritten;
    }

    private void checkWriteError() throws IOException {
      Throwable t = error.get();
      if (t != null) {
        throw new IOException("UDS write stream failed: " + t.getMessage(), toStorageException(t));
      }
    }

    @Override
    public boolean isOpen() {
      return open;
    }

    @Override
    public void close() throws IOException {
      if (!open) {
        return;
      }
      open = false;
      ensureInitialized();
      requestStream.onCompleted();
      try {
        if (!latch.await(300, TimeUnit.SECONDS)) {
          requestStream.cancel("WriteChannel close timed out", null);
          throw new IOException(
              "Upload timed out over UDS for " + blobInfo.getBucket() + "/" + blobInfo.getName());
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        InterruptedIOException iio =
            new InterruptedIOException("Interrupted waiting for upload completion");
        iio.initCause(e);
        throw iio;
      }
      checkWriteError();
    }

    @Override
    public void setChunkSize(int chunkSize) {
      this.chunkSize = Math.max(chunkSize, 64 * 1024);
    }

    @Override
    public RestorableState<WriteChannel> capture() {
      throw new UnsupportedOperationException("capture not supported in UDS POC");
    }
  }

  // =========================================================================
  // 5 & 6. Control-Plane Listing — ListObjects & ListBuckets (Real Pagination)
  // =========================================================================

  public Page<Blob> list(final String bucket, final BlobListOption... listOptions) {
    return fetchObjectsPage(bucket, /* overridePageToken= */ null, listOptions);
  }

  private Page<Blob> fetchObjectsPage(
      final String bucket, String overridePageToken, final BlobListOption... listOptions) {
    final Storage st = selfStorage();
    try {
      ListObjectsRequest req =
          UdsVeneerBridge.buildListObjectsRequest(bucket, overridePageToken, listOptions);
      ListObjectsResponse resp =
          blockingStub.withDeadlineAfter(60, TimeUnit.SECONDS).listObjects(req);

      List<Blob> blobs =
          new ArrayList<Blob>(resp.getItemsCount() + resp.getPrefixesCount());
      for (String prefix : resp.getPrefixesList()) {
        blobs.add(
            UdsVeneerBridge.asBlob(
                UdsVeneerBridge.directoryPrefixBlobInfo(bucket, prefix), st));
      }
      for (ListedObject obj : resp.getItemsList()) {
        blobs.add(
            UdsVeneerBridge.asBlob(UdsVeneerBridge.blobInfoFromListed(bucket, obj), st));
      }

      final String nextCursor =
          resp.getNextPageToken().isEmpty() ? null : resp.getNextPageToken();
      PageImpl.NextPageFetcher<Blob> fetcher =
          new PageImpl.NextPageFetcher<Blob>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Page<Blob> getNextPage() {
              if (nextCursor == null) {
                return null;
              }
              return fetchObjectsPage(bucket, nextCursor, listOptions);
            }
          };
      if (DEBUG_LOG) {
        System.out.printf(
            "[UdsStorage] list(%s) returned %d items (hasNext=%b)\n",
            bucket, blobs.size(), nextCursor != null);
      }
      return new PageImpl<Blob>(fetcher, nextCursor, blobs);
    } catch (Throwable t) {
      throw toStorageException(t);
    }
  }

  public Page<Bucket> listBuckets(final BucketListOption... listOptions) {
    final String project = resolveProjectId();
    return fetchBucketsPage(project, /* overridePageToken= */ null, listOptions);
  }

  private String resolveProjectId() {
    if (options != null
        && options.getProjectId() != null
        && !options.getProjectId().isEmpty()
        && !options.getProjectId().startsWith("no-project")) {
      return options.getProjectId();
    }
    String p = System.getProperty("gcs.project.id", System.getProperty("gcs.sidecar.project"));
    if (p == null || p.isEmpty()) {
      p = System.getenv("GOOGLE_CLOUD_PROJECT");
    }
    return (p != null && !p.isEmpty()) ? p : "vaibhavpratap-sdk-test";
  }

  private Page<Bucket> fetchBucketsPage(
      final String project, String overridePageToken, final BucketListOption... listOptions) {
    final Storage st = selfStorage();
    try {
      ListBucketsRequest req =
          UdsVeneerBridge.buildListBucketsRequest(project, overridePageToken, listOptions);
      ListBucketsResponse resp =
          blockingStub.withDeadlineAfter(60, TimeUnit.SECONDS).listBuckets(req);

      List<Bucket> buckets = new ArrayList<Bucket>(resp.getItemsCount());
      for (ListedBucket b : resp.getItemsList()) {
        buckets.add(UdsVeneerBridge.bucketFromListed(b, st));
      }

      final String nextCursor =
          resp.getNextPageToken().isEmpty() ? null : resp.getNextPageToken();
      PageImpl.NextPageFetcher<Bucket> fetcher =
          new PageImpl.NextPageFetcher<Bucket>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Page<Bucket> getNextPage() {
              if (nextCursor == null) {
                return null;
              }
              return fetchBucketsPage(project, nextCursor, listOptions);
            }
          };
      return new PageImpl<Bucket>(fetcher, nextCursor, buckets);
    } catch (Throwable t) {
      throw toStorageException(t);
    }
  }

  // =========================================================================
  // Control-Plane GetObject & DeleteObject + Convenience Helpers
  // =========================================================================

  public Blob get(BlobId blob, BlobGetOption... options) {
    SourcePreconditions pre = UdsVeneerBridge.extractGetOptions(blob, options);
    GetObjectRequest.Builder rb =
        GetObjectRequest.newBuilder().setBucket(blob.getBucket()).setObject(blob.getName());
    if (pre.generation != null && pre.generation > 0) {
      rb.setGeneration(pre.generation);
    }
    if (pre.ifGenerationMatch != null && pre.ifGenerationMatch >= 0) {
      rb.setIfGenerationMatch(pre.ifGenerationMatch);
    }
    try {
      ListedObject obj =
          blockingStub.withDeadlineAfter(30, TimeUnit.SECONDS).getObject(rb.build());
      BlobInfo info = UdsVeneerBridge.blobInfoFromListed(blob.getBucket(), obj);
      return UdsVeneerBridge.asBlob(info, selfStorage());
    } catch (StatusRuntimeException sre) {
      if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return null;
      }
      throw toStorageException(sre);
    } catch (Throwable t) {
      throw toStorageException(t);
    }
  }

  public Blob get(String bucket, String blob, BlobGetOption... options) {
    return get(BlobId.of(bucket, blob), options);
  }

  public boolean delete(BlobId blob, BlobSourceOption... options) {
    SourcePreconditions pre = UdsVeneerBridge.extractSourceOptions(blob, options);
    DeleteObjectRequest.Builder rb =
        DeleteObjectRequest.newBuilder().setBucket(blob.getBucket()).setObject(blob.getName());
    if (pre.generation != null && pre.generation > 0) {
      rb.setGeneration(pre.generation);
    }
    if (pre.ifGenerationMatch != null && pre.ifGenerationMatch >= 0) {
      rb.setIfGenerationMatch(pre.ifGenerationMatch);
    }
    try {
      blockingStub.withDeadlineAfter(30, TimeUnit.SECONDS).deleteObject(rb.build());
      return true;
    } catch (StatusRuntimeException sre) {
      if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
        return false;
      }
      throw toStorageException(sre);
    } catch (Throwable t) {
      throw toStorageException(t);
    }
  }

  public boolean delete(String bucket, String blob, BlobSourceOption... options) {
    return delete(BlobId.of(bucket, blob), options);
  }

  public Blob create(BlobInfo blobInfo, byte[] content, BlobTargetOption... options) {
    TargetPreconditions pre = UdsVeneerBridge.extractTargetOptions(blobInfo, options);
    WriteObjectResponse resp =
        uploadStream(
            pre.resolvedInfo,
            new ByteArrayInputStream(content),
            content.length,
            pre.ifGenerationMatch);
    BlobInfo resolved =
        UdsVeneerBridge.blobInfoFromWriteResult(
            pre.resolvedInfo, resp.getSize(), resp.getGeneration(), resp.getCrc32C());
    return UdsVeneerBridge.asBlob(resolved, selfStorage());
  }

  public Blob createFrom(BlobInfo blobInfo, Path path, BlobWriteOption... options)
      throws IOException {
    long size = Files.size(path);
    TargetPreconditions pre = UdsVeneerBridge.extractWriteOptions(blobInfo, options);
    try (InputStream in = Files.newInputStream(path)) {
      WriteObjectResponse resp =
          uploadStream(pre.resolvedInfo, in, size, pre.ifGenerationMatch);
      BlobInfo resolved =
          UdsVeneerBridge.blobInfoFromWriteResult(
              pre.resolvedInfo, resp.getSize(), resp.getGeneration(), resp.getCrc32C());
      return UdsVeneerBridge.asBlob(resolved, selfStorage());
    }
  }

  public Blob createFrom(BlobInfo blobInfo, InputStream content, BlobWriteOption... options) {
    TargetPreconditions pre = UdsVeneerBridge.extractWriteOptions(blobInfo, options);
    long knownSize = pre.expectedObjectSize != null ? pre.expectedObjectSize : -1L;
    WriteObjectResponse resp =
        uploadStream(pre.resolvedInfo, content, knownSize, pre.ifGenerationMatch);
    BlobInfo resolved =
        UdsVeneerBridge.blobInfoFromWriteResult(
            pre.resolvedInfo, resp.getSize(), resp.getGeneration(), resp.getCrc32C());
    return UdsVeneerBridge.asBlob(resolved, selfStorage());
  }

  public void downloadTo(BlobId blob, Path path, BlobSourceOption... options) {
    try (OutputStream out = Files.newOutputStream(path)) {
      downloadTo(blob, out, options);
    } catch (IOException e) {
      throw new StorageException(500, e.getMessage(), e);
    }
  }

  public void downloadTo(BlobId blob, OutputStream outputStream, BlobSourceOption... options) {
    SourcePreconditions pre = UdsVeneerBridge.extractSourceOptions(blob, options);
    downloadStream(blob.getBucket(), blob.getName(), pre, outputStream);
  }

  public byte[] readAllBytes(BlobId blob, BlobSourceOption... options) {
    SourcePreconditions pre = UdsVeneerBridge.extractSourceOptions(blob, options);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    downloadStream(blob.getBucket(), blob.getName(), pre, baos);
    return baos.toByteArray();
  }

  public byte[] readAllBytes(String bucket, String blob, BlobSourceOption... options) {
    return readAllBytes(BlobId.of(bucket, blob), options);
  }

  public byte[] ping(byte[] payload) {
    PingRequest req = PingRequest.newBuilder().setPayload(ByteString.copyFrom(payload)).build();
    PingResponse resp = blockingStub.ping(req);
    return resp.getPayload().toByteArray();
  }

  public StatsResponse getStats(boolean resetPeakRss) {
    StatsRequest req = StatsRequest.newBuilder().setResetPeakRss(resetPeakRss).build();
    return blockingStub.stats(req);
  }

  @Override
  public void close() throws Exception {
    if (sessionManager != null) {
      try {
        sessionManager.close();
      } catch (Exception e) {
        if (DEBUG_LOG) {
          System.out.printf("[UdsStorage] Warning: Failed to close session: %s\n", e.getMessage());
        }
      }
    }
    if (ownsChannel) {
      UdsChannelFactory.shutdownChannel(channel, 3, TimeUnit.SECONDS);
    }
    if (ownedServer != null) {
      ownedServer.close();
    }
  }

  // --- Low-level Streaming Helpers ---

  private WriteObjectResponse uploadStream(
      BlobInfo info, InputStream in, long totalSize, Long ifGenerationMatch) {
    final CountDownLatch latch = new CountDownLatch(1);
    final AtomicReference<Throwable> error = new AtomicReference<Throwable>();
    final AtomicReference<WriteObjectResponse> responseRef =
        new AtomicReference<WriteObjectResponse>();
    final Object readyLock = new Object();
    final AtomicReference<ClientCallStreamObserver<WriteRequest>> clientCallRef =
        new AtomicReference<ClientCallStreamObserver<WriteRequest>>();

    ClientResponseObserver<WriteRequest, WriteObjectResponse> responseObserver =
        new ClientResponseObserver<WriteRequest, WriteObjectResponse>() {
          @Override
          public void beforeStart(ClientCallStreamObserver<WriteRequest> requestStream) {
            clientCallRef.set(requestStream);
            requestStream.setOnReadyHandler(
                new Runnable() {
                  @Override
                  public void run() {
                    synchronized (readyLock) {
                      readyLock.notifyAll();
                    }
                  }
                });
          }

          @Override
          public void onNext(WriteObjectResponse resp) {
            responseRef.set(resp);
          }

          @Override
          public void onError(Throwable t) {
            error.set(t);
            synchronized (readyLock) {
              readyLock.notifyAll();
            }
            latch.countDown();
          }

          @Override
          public void onCompleted() {
            synchronized (readyLock) {
              readyLock.notifyAll();
            }
            latch.countDown();
          }
        };

    StreamObserver<WriteRequest> requestObserver = asyncStub.writeObject(responseObserver);
    ClientCallStreamObserver<WriteRequest> clientCallObserver = clientCallRef.get();

    try {
      WriteHeader.Builder hb =
          WriteHeader.newBuilder()
              .setBucket(info.getBucket())
              .setObject(info.getName())
              .setTransport(clientTransport);
      if (info.getContentType() != null && !info.getContentType().isEmpty()) {
        hb.setContentType(info.getContentType());
      }
      if (ifGenerationMatch != null && ifGenerationMatch >= 0) {
        hb.setIfGenerationMatch(ifGenerationMatch);
      }
      if (totalSize >= 0) {
        hb.setKnownSize(totalSize);
      }

      requestObserver.onNext(WriteRequest.newBuilder().setHeader(hb.build()).build());

      byte[] buffer = new byte[CHUNK_SIZE];
      int read;
      while ((read = in.read(buffer)) > 0) {
        if (error.get() != null) {
          break;
        }
        if (clientCallObserver != null) {
          synchronized (readyLock) {
            while (!clientCallObserver.isReady() && error.get() == null) {
              readyLock.wait(10);
            }
          }
        }
        requestObserver.onNext(
            WriteRequest.newBuilder().setData(ByteString.copyFrom(buffer, 0, read)).build());
      }

      requestObserver.onCompleted();

      if (!latch.await(300, TimeUnit.SECONDS)) {
        if (clientCallObserver != null) {
          clientCallObserver.cancel("Upload timed out over UDS", null);
        }
        throw new StorageException(504, "Upload timed out over UDS");
      }
      if (error.get() != null) {
        throw toStorageException(error.get());
      }
      WriteObjectResponse resp = responseRef.get();
      return resp != null ? resp : WriteObjectResponse.getDefaultInstance();
    } catch (Exception e) {
      if (error.get() == null && latch.getCount() > 0 && clientCallObserver != null) {
        clientCallObserver.cancel("Upload aborted by client exception", e);
      }
      throw toStorageException(e);
    }
  }

  private long downloadStream(
      String bucket, String name, SourcePreconditions pre, OutputStream out) {
    ReadObjectRequest.Builder rb =
        ReadObjectRequest.newBuilder()
            .setBucket(bucket)
            .setObject(name)
            .setOffset(0)
            .setLength(-1)
            .setTransport(clientTransport);
    if (pre != null) {
      if (pre.generation != null && pre.generation > 0) {
        rb.setGeneration(pre.generation);
      }
      if (pre.ifGenerationMatch != null && pre.ifGenerationMatch >= 0) {
        rb.setIfGenerationMatch(pre.ifGenerationMatch);
      }
    }

    final LinkedBlockingQueue<java.lang.Object> queue =
        new LinkedBlockingQueue<java.lang.Object>();
    final ClientCall<ReadObjectRequest, ReadChunk> call =
        callChannel.newCall(GcsCoreGrpc.getReadObjectMethod(), CallOptions.DEFAULT);
    call.start(
        new ClientCall.Listener<ReadChunk>() {
          @Override
          public void onMessage(ReadChunk message) {
            queue.offer(message);
          }

          @Override
          public void onClose(Status status, Metadata trailers) {
            if (status.isOk()) {
              queue.offer(UdsReadChannel.EOF_MARKER);
            } else {
              queue.offer(status.asRuntimeException(trailers));
            }
          }
        },
        new Metadata());
    call.sendMessage(rb.build());
    call.halfClose();
    call.request(2);

    long totalBytes = 0;
    try {
      while (true) {
        java.lang.Object item = queue.poll(120, TimeUnit.SECONDS);
        if (item == null) {
          call.cancel("Download timed out over UDS", null);
          throw new StorageException(504, "Download timed out over UDS");
        }
        if (item == UdsReadChannel.EOF_MARKER) {
          break;
        }
        if (item instanceof Throwable) {
          throw toStorageException((Throwable) item);
        }
        ReadChunk chunk = (ReadChunk) item;
        call.request(1);
        for (ByteString slice : chunk.getDataList()) {
          slice.writeTo(out);
          totalBytes += slice.size();
        }
      }
      out.flush();
      return totalBytes;
    } catch (Exception e) {
      call.cancel("Download failed", e);
      throw toStorageException(e);
    }
  }
}
