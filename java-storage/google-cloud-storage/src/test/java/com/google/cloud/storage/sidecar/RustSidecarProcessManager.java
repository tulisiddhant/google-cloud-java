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
import java.lang.reflect.Method;
import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages the lifecycle of the GCS Rust Core sidecar subprocess over Unix Domain Sockets.
 * Supports both gcs-core-server (PR #7350) and gcs-rust-sidecar (V1 prototype).
 * Logs extensively to stdout to assist developers in tracking sidecar lifecycle events.
 */
public final class RustSidecarProcessManager implements AutoCloseable {

  private static final Logger LOGGER =
      Logger.getLogger(RustSidecarProcessManager.class.getName());

  private static final String DEFAULT_SOCKET_PATH = "/tmp/gcs-core.sock";
  private static final String DEFAULT_BINARY_RELATIVE_PATH =
      "google-cloud-rust-cpriti/target/debug/gcs-core-server";
  private static final String FALLBACK_BINARY_RELATIVE_PATH =
      "google-cloud-rust/target/debug/gcs-rust-sidecar";

  private final Path binaryPath;
  private final Path socketPath;
  private Process process;
  private Thread shutdownHook;
  private boolean ownsProcess = false;

  public RustSidecarProcessManager(Path binaryPath, Path socketPath) {
    this.binaryPath = binaryPath;
    this.socketPath = socketPath;
  }

  private static void log(String message) {
    System.out.println("[SidecarProcessManager] " + message);
  }

  public static RustSidecarProcessManager createDefault() {
    String customBinary = System.getProperty("gcs.sidecar.binary.path");
    String binarySource = "System property (gcs.sidecar.binary.path)";
    if (customBinary == null) {
      customBinary = System.getenv("GCS_SIDECAR_BINARY_PATH");
      binarySource = "Environment variable (GCS_SIDECAR_BINARY_PATH)";
    }
    if (customBinary == null) {
      Path primary = Paths.get(System.getProperty("user.home"), DEFAULT_BINARY_RELATIVE_PATH);
      Path binPath = Paths.get(System.getProperty("user.home"), "bin/gcs-core-server");
      Path fallback = Paths.get(System.getProperty("user.home"), FALLBACK_BINARY_RELATIVE_PATH);
      if (Files.exists(primary)) {
        customBinary = primary.toString();
        binarySource = "Primary path (~/" + DEFAULT_BINARY_RELATIVE_PATH + ")";
      } else if (Files.exists(binPath)) {
        customBinary = binPath.toString();
        binarySource = "User bin path (~/bin/gcs-core-server)";
      } else {
        customBinary = fallback.toString();
        binarySource = "Fallback path (~/" + FALLBACK_BINARY_RELATIVE_PATH + ")";
      }
    }
    Path binary = Paths.get(customBinary);
    log("Resolved sidecar binary path: " + binary.toAbsolutePath() + " [" + binarySource + "]");

    String customSocket = System.getProperty("gcs.sidecar.socket.path");
    String socketSource = "System property (gcs.sidecar.socket.path)";
    if (customSocket == null) {
      customSocket = System.getenv("GCS_SIDECAR_SOCKET_PATH");
      socketSource = "Environment variable (GCS_SIDECAR_SOCKET_PATH)";
    }
    if (customSocket == null) {
      customSocket = DEFAULT_SOCKET_PATH;
      socketSource = "Default path (" + DEFAULT_SOCKET_PATH + ")";
    }
    Path socket = Paths.get(customSocket);
    log("Resolved UDS socket path: " + socket.toAbsolutePath() + " [" + socketSource + "]");

    return new RustSidecarProcessManager(binary, socket);
  }

  /**
   * Starts the sidecar process if not already running and waits for the UDS socket to become ready.
   */
  public synchronized void start(Duration timeout) throws IOException, TimeoutException, InterruptedException {
    log("======================================================================");
    log("STARTING SIDECAR SUBPROCESS LIFECYCLE MANAGEMENT");
    log("======================================================================");
    log("Target UDS socket path : " + socketPath.toAbsolutePath());
    log("Target binary path     : " + binaryPath.toAbsolutePath());

    // If the socket file exists, check if another process is already listening
    if (Files.exists(socketPath)) {
      log("Checking existing socket at " + socketPath + "...");
      if (probeSocket(socketPath)) {
        log("Active sidecar already detected and accepting connections on " + socketPath);
        log("Reusing existing sidecar instance (ownsProcess=false).");
        this.ownsProcess = false;
        log("======================================================================");
        return;
      } else {
        log("Socket file exists but is inactive/stale (connection refused).");
        log("Removing stale socket file: " + socketPath);
        Files.deleteIfExists(socketPath);
      }
    }

    if (!Files.exists(binaryPath)) {
      String err =
          "Sidecar binary not found at: "
              + binaryPath.toAbsolutePath()
              + ". Please build the binary first.";
      log("ERROR: " + err);
      throw new IllegalStateException(err);
    }

    long binarySizeBytes = Files.size(binaryPath);
    log(String.format("Binary verified (size: %,d bytes). Preparing ProcessBuilder...", binarySizeBytes));

    List<String> command = new ArrayList<>();
    command.add(binaryPath.toAbsolutePath().toString());
    boolean isGcsCoreServer = binaryPath.getFileName().toString().contains("gcs-core-server");
    if (isGcsCoreServer) {
      command.add("--socket");
      command.add(socketPath.toAbsolutePath().toString());
      String transport = System.getProperty("gcs.sidecar.default.transport", "grpc");
      command.add("--default-transport");
      command.add(transport);
      command.add("--grpc-subchannels");
      String subchannels = System.getProperty("gcs.sidecar.grpc.subchannels", "16");
      command.add(subchannels);
      command.add("--pdeathsig");
      command.add("true");
      command.add("--idle-secs");
      command.add("0");

      String endpoint = System.getProperty("gcs.sidecar.endpoint");
      if (endpoint != null && !endpoint.isEmpty()) {
        command.add("--endpoint");
        command.add(endpoint);
      }
    } else {
      command.add("--socket-path");
      command.add(socketPath.toAbsolutePath().toString());
    }

    ProcessBuilder pb = new ProcessBuilder(command);
    pb.inheritIO();
    pb.environment().put("NO_PROXY", "localhost,127.0.0.1,::1");
    pb.environment().put("no_proxy", "localhost,127.0.0.1,::1");

    log("Spawning process: " + String.join(" ", command));
    Instant spawnStart = Instant.now();
    this.process = pb.start();
    this.ownsProcess = true;

    long pid = getProcessPid(process);
    log(String.format("Subprocess spawned successfully! PID: %d (in %d ms)", pid, Duration.between(spawnStart, Instant.now()).toMillis()));

    // Register JVM shutdown hook
    this.shutdownHook =
        new Thread(
            () -> {
              log("JVM Shutdown Hook executing: stopping Rust sidecar (PID " + pid + ")...");
              stopInternal();
            },
            "sidecar-shutdown-hook");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    log("Registered JVM shutdown hook to clean up PID " + pid + " on JVM exit.");

    // Wait for the Unix Domain Socket to accept connections
    log(String.format("Polling UDS readiness for up to %d ms...", timeout.toMillis()));
    Instant deadline = Instant.now().plus(timeout);
    boolean ready = false;
    int probeCount = 0;

    while (Instant.now().isBefore(deadline)) {
      probeCount++;
      if (!process.isAlive()) {
        int exitCode = process.exitValue();
        String err = "Sidecar process terminated unexpectedly during startup with exit code: " + exitCode;
        log("FATAL: " + err);
        throw new IllegalStateException(err);
      }
      if (Files.exists(socketPath) && probeSocket(socketPath)) {
        ready = true;
        break;
      }
      Thread.sleep(50);
    }

    long elapsedMs = Duration.between(spawnStart, Instant.now()).toMillis();
    if (!ready) {
      log("TIMEOUT: Sidecar socket failed to accept connections within " + timeout.toMillis() + " ms.");
      stop();
      throw new TimeoutException(
          "Timed out after " + timeout.toMillis() + " ms waiting for sidecar socket at " + socketPath);
    }

    log(String.format("Readiness verified! Socket accepted connection after %d ms (%d probes).", elapsedMs, probeCount));
    log("GCS Rust Sidecar is READY and accepting gRPC connections.");
    log("======================================================================");
  }

  public Path getSocketPath() {
    return socketPath;
  }

  public Path getBinaryPath() {
    return binaryPath;
  }

  public synchronized boolean isRunning() {
    if (process != null && process.isAlive()) {
      return true;
    }
    return probeSocket(socketPath);
  }

  private static boolean probeSocket(Path socket) {
    if (!Files.exists(socket)) {
      return false;
    }
    try {
      Class<?> addrClass = Class.forName("java.net.UnixDomainSocketAddress");
      Method ofMethod = addrClass.getMethod("of", Path.class);
      SocketAddress addr = (SocketAddress) ofMethod.invoke(null, socket);
      try (java.nio.channels.SocketChannel channel = java.nio.channels.SocketChannel.open(addr)) {
        return channel.isConnected();
      } catch (IOException e) {
        return false;
      }
    } catch (Throwable t) {
      return false;
    }
  }

  private static long getProcessPid(Process p) {
    try {
      Method pidMethod = Process.class.getMethod("pid");
      return (long) pidMethod.invoke(p);
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
          log("Process did not terminate within 3s grace period; sending SIGKILL (destroyForcibly())...");
          process.destroyForcibly();
          process.waitFor(1, TimeUnit.SECONDS);
        } else {
          log(String.format("Process (PID: %d) exited cleanly with status code: %d", pid, process.exitValue()));
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        process.destroyForcibly();
      }
      process = null;
    }

    try {
      if (Files.exists(socketPath)) {
        Files.deleteIfExists(socketPath);
        log("Cleaned up Unix Domain Socket file: " + socketPath);
      }
    } catch (IOException e) {
      log("Warning: Failed to delete socket file " + socketPath + ": " + e.getMessage());
    }
    log("Sidecar process manager stopped.");
    log("----------------------------------------------------------------------");
  }

  @Override
  public void close() {
    stop();
  }
}
