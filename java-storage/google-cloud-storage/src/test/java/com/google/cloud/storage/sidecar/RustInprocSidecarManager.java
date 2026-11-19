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

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages the in-process GCS Rust Core gRPC-over-UDS server lifecycle via direct JNI bindings
 * ({@code libstorage_grpc_uds.so}) without spawning a separate sidecar OS process.
 *
 * <p>Uses {@link UdsChannelFactory#probeReadiness(String, long)} for full JDK 8+ runtime
 * compatibility (including Linux {@code @abstract} domain sockets).
 */
public final class RustInprocSidecarManager implements AutoCloseable {

  private static final String LIBRARY_PROPERTY = "gcs.inproc.library.path";
  private static final String LIBRARY_ENV = "GCS_INPROC_LIBRARY_PATH";
  private static final String LIBRARY_FILENAME = "libstorage_grpc_uds.so";
  private static final AtomicInteger INSTANCE_SEQ = new AtomicInteger(0);

  private static Path loadedLibraryPath;

  private final Path libraryPath;
  private final Path socketPath;
  private final String socketSpec;
  private boolean started = false;
  private Thread shutdownHook;

  public RustInprocSidecarManager(Path libraryPath, Path socketPath) {
    this(libraryPath, socketPath, socketPath.toAbsolutePath().toString());
  }

  public RustInprocSidecarManager(Path libraryPath, String socketSpec) {
    this(
        libraryPath,
        socketSpec.startsWith("@") ? Paths.get(socketSpec) : Paths.get(socketSpec).toAbsolutePath(),
        socketSpec.startsWith("@")
            ? socketSpec
            : Paths.get(socketSpec).toAbsolutePath().toString());
  }

  private RustInprocSidecarManager(Path libraryPath, Path socketPath, String socketSpec) {
    this.libraryPath = libraryPath;
    this.socketPath = socketPath;
    this.socketSpec = socketSpec;
  }

  private static void log(String message) {
    System.out.println("[InprocSidecarManager] " + message);
  }

  private static String currentPidString() {
    String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
    int atIdx = runtimeName.indexOf('@');
    if (atIdx > 0) {
      return runtimeName.substring(0, atIdx);
    }
    return String.valueOf(System.currentTimeMillis());
  }

  public static RustInprocSidecarManager createDefault() {
    String customLib = System.getProperty(LIBRARY_PROPERTY);
    String libSource = "System property (" + LIBRARY_PROPERTY + ")";
    if (customLib == null || customLib.isEmpty()) {
      customLib = System.getenv(LIBRARY_ENV);
      libSource = "Environment variable (" + LIBRARY_ENV + ")";
    }
    if (customLib == null || customLib.isEmpty()) {
      String home = System.getProperty("user.home");
      Path[] candidates =
          new Path[] {
            Paths.get(home, "gits/java-uds-benchmark/google-cloud-rust/target/release", LIBRARY_FILENAME),
            Paths.get(home, "bin", LIBRARY_FILENAME),
            Paths.get(home, "google-cloud-rust/target/release", LIBRARY_FILENAME),
            Paths.get(home, "gits/java-uds-benchmark/google-cloud-rust/target/debug", LIBRARY_FILENAME),
          };
      for (Path candidate : candidates) {
        if (Files.exists(candidate)) {
          customLib = candidate.toString();
          libSource = "Auto-detected (" + candidate + ")";
          break;
        }
      }
      if (customLib == null || customLib.isEmpty()) {
        customLib = Paths.get(home, "bin", LIBRARY_FILENAME).toString();
        libSource = "Default path (~/" + "bin/" + LIBRARY_FILENAME + ")";
      }
    }
    Path library = Paths.get(customLib);
    log("Resolved in-process JNI library path: " + library.toAbsolutePath() + " [" + libSource + "]");

    String customSocket = System.getProperty("gcs.sidecar.socket.path");
    String socketSource = "System property (gcs.sidecar.socket.path)";
    if (customSocket == null || customSocket.isEmpty()) {
      customSocket = System.getenv("GCS_SIDECAR_SOCKET_PATH");
      socketSource = "Environment variable (GCS_SIDECAR_SOCKET_PATH)";
    }
    if (customSocket == null || customSocket.isEmpty()) {
      int seq = INSTANCE_SEQ.incrementAndGet();
      customSocket =
          seq == 1
              ? ("/tmp/gcs-core-inproc-" + currentPidString() + ".sock")
              : ("/tmp/gcs-core-inproc-" + currentPidString() + "-" + seq + ".sock");
      socketSource = "Default per-PID path (" + customSocket + ")";
    }
    log("Resolved in-process UDS socket spec: " + customSocket + " [" + socketSource + "]");

    return new RustInprocSidecarManager(library, customSocket);
  }

  private static synchronized void ensureLibraryLoaded(Path libraryPath) {
    Path normalized = libraryPath.toAbsolutePath().normalize();
    if (loadedLibraryPath != null) {
      if (!loadedLibraryPath.equals(normalized)) {
        throw new IllegalStateException(
            "Already loaded " + loadedLibraryPath + "; cannot also load " + normalized);
      }
      return;
    }
    if (!Files.exists(normalized)) {
      String err =
          "In-process JNI library not found at: "
              + normalized
              + ". Build with `RUSTFLAGS=\"--cfg google_cloud_unstable_storage_bidi\" cargo build --release -p storage-grpc-uds --features jni`.";
      log("ERROR: " + err);
      throw new IllegalStateException(err);
    }
    System.load(normalized.toString());
    loadedLibraryPath = normalized;
    log("Loaded native JNI library via System.load: " + normalized);
  }

  private static native int startInprocNative(
      String socketPath, String defaultTransport, int grpcSubchannels, String endpoint);

  private static native int startInprocWithOptionsNative(
      String socketPath,
      String defaultTransport,
      int grpcSubchannels,
      String endpoint,
      String controlEndpoint,
      boolean requireSession);

  private static native int stopInprocNative(String socketPath);

  public synchronized void start(Duration timeout)
      throws IOException, TimeoutException, InterruptedException {
    log("======================================================================");
    log("STARTING IN-PROCESS RUST CORE (JNI + UDS)");
    log("======================================================================");
    log("Target UDS socket spec : " + socketSpec);
    log("Target JNI library     : " + libraryPath.toAbsolutePath());

    boolean isAbstract = socketSpec.startsWith("@");
    if (isAbstract || Files.exists(socketPath)) {
      if (UdsChannelFactory.probeReadiness(socketSpec, 250)) {
        throw new IllegalStateException(
            "An active server is already listening on in-process socket: " + socketSpec);
      }
      if (!isAbstract && Files.exists(socketPath)) {
        log("Removing stale socket file: " + socketPath);
        Files.deleteIfExists(socketPath);
        Files.deleteIfExists(Paths.get(socketSpec + ".shm"));
      }
    }

    ensureLibraryLoaded(libraryPath);

    String defaultTransport = System.getProperty("gcs.sidecar.default.transport", "grpc");
    int grpcSubchannels = Integer.getInteger("gcs.sidecar.grpc.subchannels", 0);
    String endpoint = System.getProperty("gcs.sidecar.endpoint");
    String controlEndpoint = System.getProperty("gcs.sidecar.control.endpoint");
    boolean isLive =
        Boolean.parseBoolean(
            System.getProperty("gcs.live.benchmark", System.getProperty("gcs.bench.live", "false")));
    boolean requireSession =
        Boolean.parseBoolean(
            System.getProperty("gcs.sidecar.require.session", String.valueOf(isLive)));

    log(
        String.format(
            "Calling startInprocWithOptionsNative(socket=%s, transport=%s, subchannels=%d, endpoint=%s, controlEndpoint=%s, requireSession=%b)...",
            socketSpec,
            defaultTransport,
            grpcSubchannels,
            endpoint,
            controlEndpoint,
            requireSession));

    Instant startTime = Instant.now();
    int rc;
    try {
      rc =
          startInprocWithOptionsNative(
              socketSpec,
              defaultTransport,
              grpcSubchannels,
              endpoint,
              controlEndpoint,
              requireSession);
    } catch (UnsatisfiedLinkError legacyLib) {
      rc = startInprocNative(socketSpec, defaultTransport, grpcSubchannels, endpoint);
    }
    if (rc != 0) {
      throw new IllegalStateException(
          "startInproc returned non-zero status code " + rc + " for socket " + socketSpec);
    }
    this.started = true;

    this.shutdownHook =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                log("JVM Shutdown Hook executing: stopping in-process Rust core...");
                stopInternal();
              }
            },
            "inproc-sidecar-shutdown-hook");
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    Instant deadline = Instant.now().plus(timeout);
    boolean ready = false;
    int probeCount = 0;
    while (Instant.now().isBefore(deadline)) {
      probeCount++;
      if (UdsChannelFactory.probeReadiness(socketSpec, 250)) {
        ready = true;
        break;
      }
      Thread.sleep(25);
    }

    long elapsedMs = Duration.between(startTime, Instant.now()).toMillis();
    if (!ready) {
      stop();
      throw new TimeoutException(
          "Timed out after " + timeout.toMillis() + " ms waiting for in-process UDS socket " + socketSpec);
    }

    log(
        String.format(
            "Readiness verified! In-process GcsCore Ping succeeded after %d ms (%d probes).",
            elapsedMs, probeCount));
    log("======================================================================");
  }

  public Path getSocketPath() {
    return socketPath;
  }

  public String getSocketSpec() {
    return socketSpec;
  }

  public Path getLibraryPath() {
    return libraryPath;
  }

  public synchronized boolean isRunning() {
    return started && UdsChannelFactory.probeReadiness(socketSpec, 250);
  }

  public synchronized void stop() {
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException ignored) {
        // JVM shutdown already in progress
      }
      shutdownHook = null;
    }
    stopInternal();
  }

  private void stopInternal() {
    if (started) {
      log("Stopping in-process Rust core on socket: " + socketSpec);
      int rc = stopInprocNative(socketSpec);
      log("stopInprocNative returned: " + rc);
      started = false;
    }
    if (!socketSpec.startsWith("@")) {
      try {
        Files.deleteIfExists(socketPath);
        Files.deleteIfExists(Paths.get(socketSpec + ".shm"));
      } catch (IOException e) {
        log("Warning: Failed to delete socket file " + socketPath + ": " + e.getMessage());
      }
    }
  }

  @Override
  public void close() {
    stop();
  }
}
