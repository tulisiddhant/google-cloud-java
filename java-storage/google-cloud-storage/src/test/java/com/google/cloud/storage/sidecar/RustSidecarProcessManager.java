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
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Manages the lifecycle of the GCS Rust Core sidecar subprocess ({@code gcs-core-server}) over Unix
 * Domain Sockets using a JDK 8-compatible Netty UDS {@code Ping} readiness probe.
 */
public final class RustSidecarProcessManager implements AutoCloseable {

  private final Path binaryPath;
  private final Path socketPath;
  private final String socketSpec;
  private Process process;
  private Thread shutdownHook;
  private boolean ownsProcess = false;

  public RustSidecarProcessManager(Path binaryPath, Path socketPath) {
    this(binaryPath, socketPath, socketPath.toAbsolutePath().toString());
  }

  public RustSidecarProcessManager(Path binaryPath, String socketSpec) {
    this(
        binaryPath,
        socketSpec.startsWith("@") ? Paths.get(socketSpec) : Paths.get(socketSpec).toAbsolutePath(),
        socketSpec.startsWith("@")
            ? socketSpec
            : Paths.get(socketSpec).toAbsolutePath().toString());
  }

  private RustSidecarProcessManager(Path binaryPath, Path socketPath, String socketSpec) {
    this.binaryPath = binaryPath;
    this.socketPath = socketPath;
    this.socketSpec = socketSpec;
  }

  private static void log(String message) {
    System.out.println("[SidecarProcessManager] " + message);
  }

  private static String currentPidString() {
    String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
    int atIdx = runtimeName.indexOf('@');
    if (atIdx > 0) {
      return runtimeName.substring(0, atIdx);
    }
    return String.valueOf(System.currentTimeMillis());
  }

  public static RustSidecarProcessManager createDefault() {
    String customBinary = System.getProperty("gcs.sidecar.binary.path");
    String binarySource = "System property (gcs.sidecar.binary.path)";
    if (customBinary == null || customBinary.isEmpty()) {
      customBinary = System.getenv("GCS_SIDECAR_BINARY_PATH");
      binarySource = "Environment variable (GCS_SIDECAR_BINARY_PATH)";
    }
    if (customBinary == null || customBinary.isEmpty()) {
      String home = System.getProperty("user.home");
      Path[] candidates =
          new Path[] {
            Paths.get(home, "gits/java-uds-benchmark/google-cloud-rust/target/release/gcs-core-server"),
            Paths.get(home, "bin/gcs-core-server"),
            Paths.get(home, "google-cloud-rust/target/release/gcs-core-server"),
            Paths.get(home, "google-cloud-rust-cpriti/target/release/gcs-core-server"),
            Paths.get(home, "gits/java-uds-benchmark/google-cloud-rust/target/debug/gcs-core-server"),
            Paths.get(home, "google-cloud-rust-cpriti/target/debug/gcs-core-server"),
          };
      for (Path candidate : candidates) {
        if (Files.exists(candidate)) {
          customBinary = candidate.toString();
          binarySource = "Auto-detected (" + candidate + ")";
          break;
        }
      }
      if (customBinary == null || customBinary.isEmpty()) {
        customBinary = Paths.get(home, "bin/gcs-core-server").toString();
        binarySource = "Default path (~/bin/gcs-core-server)";
      }
    }
    Path binary = Paths.get(customBinary);
    log("Resolved sidecar binary path: " + binary.toAbsolutePath() + " [" + binarySource + "]");

    String customSocket = System.getProperty("gcs.sidecar.socket.path");
    String socketSource = "System property (gcs.sidecar.socket.path)";
    if (customSocket == null || customSocket.isEmpty()) {
      customSocket = System.getenv("GCS_SIDECAR_SOCKET_PATH");
      socketSource = "Environment variable (GCS_SIDECAR_SOCKET_PATH)";
    }
    if (customSocket == null || customSocket.isEmpty()) {
      customSocket = "/tmp/gcs-core-" + currentPidString() + ".sock";
      socketSource = "Default per-PID path (" + customSocket + ")";
    }
    log("Resolved UDS socket spec: " + customSocket + " [" + socketSource + "]");

    return new RustSidecarProcessManager(binary, customSocket);
  }

  /**
   * Starts the sidecar process if not already running and waits for the UDS socket to respond to
   * gRPC {@code Ping}.
   */
  public synchronized void start(Duration timeout)
      throws IOException, TimeoutException, InterruptedException {
    log("======================================================================");
    log("STARTING SIDECAR SUBPROCESS LIFECYCLE MANAGEMENT");
    log("======================================================================");
    log("Target UDS socket spec : " + socketSpec);
    log("Target binary path     : " + binaryPath.toAbsolutePath());

    boolean isAbstract = socketSpec.startsWith("@");
    if (isAbstract || Files.exists(socketPath)) {
      log("Checking existing UDS endpoint at " + socketSpec + "...");
      if (probeSocket(socketSpec)) {
        log("Active sidecar already detected and serving Ping RPCs on " + socketSpec);
        log("Reusing existing sidecar instance (ownsProcess=false).");
        this.ownsProcess = false;
        log("======================================================================");
        return;
      } else if (!isAbstract && Files.exists(socketPath)) {
        log("Socket file exists but is inactive/stale. Removing stale socket: " + socketPath);
        Files.deleteIfExists(socketPath);
        Files.deleteIfExists(Paths.get(socketSpec + ".shm"));
      }
    }

    if (!Files.exists(binaryPath)) {
      String err =
          "Sidecar binary not found at: "
              + binaryPath.toAbsolutePath()
              + ". Build with `RUSTFLAGS=\"--cfg google_cloud_unstable_storage_bidi\" cargo build --release -p storage-grpc-uds --bin gcs-core-server`.";
      log("ERROR: " + err);
      throw new IllegalStateException(err);
    }

    long binarySizeBytes = Files.size(binaryPath);
    log(
        String.format(
            "Binary verified (size: %,d bytes). Preparing ProcessBuilder...", binarySizeBytes));

    List<String> command = new ArrayList<String>();
    command.add(binaryPath.toAbsolutePath().toString());
    boolean isGcsCoreServer = binaryPath.getFileName().toString().contains("gcs-core-server");
    if (isGcsCoreServer) {
      command.add("--socket");
      command.add(socketSpec);
      String transport = System.getProperty("gcs.sidecar.default.transport", "grpc");
      command.add("--default-transport");
      command.add(transport);
      command.add("--grpc-subchannels");
      String subchannels = System.getProperty("gcs.sidecar.grpc.subchannels", "0");
      command.add(subchannels);
      command.add("--pdeathsig");
      command.add("true");
      command.add("--idle-secs");
      command.add("0");

      boolean isLive =
          Boolean.parseBoolean(
              System.getProperty(
                  "gcs.live.benchmark", System.getProperty("gcs.bench.live", "false")));
      boolean requireSession =
          Boolean.parseBoolean(
              System.getProperty("gcs.sidecar.require.session", String.valueOf(isLive)));
      if (requireSession) {
        command.add("--require-session");
      }

      String endpoint = System.getProperty("gcs.sidecar.endpoint");
      if (endpoint != null && !endpoint.isEmpty()) {
        command.add("--endpoint");
        command.add(endpoint);
      }
      String controlEndpoint = System.getProperty("gcs.sidecar.control.endpoint");
      if (controlEndpoint != null && !controlEndpoint.isEmpty()) {
        command.add("--control-endpoint");
        command.add(controlEndpoint);
      }
    } else {
      command.add("--socket-path");
      command.add(socketSpec);
    }

    ProcessBuilder pb = new ProcessBuilder(command);
    pb.inheritIO();
    pb.environment().put("NO_PROXY", "localhost,127.0.0.1,::1");
    pb.environment().put("no_proxy", "localhost,127.0.0.1,::1");

    log("Spawning process: " + String.join(" ", command));
    Instant spawnStart = Instant.now();
    this.process = pb.start();
    this.ownsProcess = true;

    final long pid = getProcessPid(process);
    log(
        String.format(
            "Subprocess spawned successfully! PID: %d (in %d ms)",
            pid, Duration.between(spawnStart, Instant.now()).toMillis()));

    this.shutdownHook =
        new Thread(
            new Runnable() {
              @Override
              public void run() {
                log("JVM Shutdown Hook executing: stopping Rust sidecar (PID " + pid + ")...");
                stopInternal();
              }
            },
            "sidecar-shutdown-hook");
    Runtime.getRuntime().addShutdownHook(shutdownHook);

    log(String.format("Polling UDS Ping readiness for up to %d ms...", timeout.toMillis()));
    Instant deadline = Instant.now().plus(timeout);
    boolean ready = false;
    int probeCount = 0;

    while (Instant.now().isBefore(deadline)) {
      probeCount++;
      if (!process.isAlive()) {
        int exitCode = process.exitValue();
        String err =
            "Sidecar process terminated unexpectedly during startup with exit code: " + exitCode;
        log("FATAL: " + err);
        throw new IllegalStateException(err);
      }
      if (probeSocket(socketSpec)) {
        ready = true;
        break;
      }
      Thread.sleep(50);
    }

    long elapsedMs = Duration.between(spawnStart, Instant.now()).toMillis();
    if (!ready) {
      log("TIMEOUT: Sidecar failed readiness probe within " + timeout.toMillis() + " ms.");
      stop();
      throw new TimeoutException(
          "Timed out after " + timeout.toMillis() + " ms waiting for sidecar socket " + socketSpec);
    }

    log(
        String.format(
            "Readiness verified! GcsCore Ping succeeded after %d ms (%d probes).",
            elapsedMs, probeCount));
    log("======================================================================");
  }

  public Path getSocketPath() {
    return socketPath;
  }

  public String getSocketSpec() {
    return socketSpec;
  }

  public Path getBinaryPath() {
    return binaryPath;
  }

  public synchronized boolean isRunning() {
    if (process != null && process.isAlive()) {
      return true;
    }
    return probeSocket(socketSpec);
  }

  static boolean probeSocket(Path socket) {
    return UdsChannelFactory.probeReadiness(socket);
  }

  static boolean probeSocket(String socketSpec) {
    return UdsChannelFactory.probeReadiness(socketSpec, 250);
  }

  private static long getProcessPid(Process p) {
    try {
      Method pidMethod = Process.class.getMethod("pid");
      return (Long) pidMethod.invoke(p);
    } catch (Throwable ignored) {
      return -1;
    }
  }

  public synchronized void stop() {
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException ignored) {
        // Already in progress of shutting down
      }
      shutdownHook = null;
    }
    stopInternal();
  }

  private void stopInternal() {
    if (ownsProcess && process != null) {
      long pid = getProcessPid(process);
      log("----------------------------------------------------------------------");
      log(String.format("Initiating sidecar process termination (PID: %d)...", pid));
      process.destroy();
      try {
        if (!process.waitFor(3, TimeUnit.SECONDS)) {
          log("Process did not terminate within 3s grace period; sending SIGKILL...");
          process.destroyForcibly();
          process.waitFor(1, TimeUnit.SECONDS);
        } else {
          log(
              String.format(
                  "Process (PID: %d) exited cleanly with status code: %d",
                  pid, process.exitValue()));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
      process = null;
    }

    if (ownsProcess && !socketSpec.startsWith("@")) {
      try {
        Files.deleteIfExists(socketPath);
        Files.deleteIfExists(Paths.get(socketSpec + ".shm"));
      } catch (IOException e) {
        log("Warning: Failed to delete socket file " + socketPath + ": " + e.getMessage());
      }
    }
    log("Sidecar process manager stopped.");
    log("----------------------------------------------------------------------");
  }

  @Override
  public void close() {
    stop();
  }
}
