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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Multi-process sidecar runtime ({@code uds-sidecar}) that spawns an external {@code
 * gcs-core-server} child process bound to a dedicated per-run Unix Domain Socket.
 */
public final class SidecarServerRuntime implements UdsServerRuntime {

  public static final int DEFAULT_CHUNK_SIZE_BYTES = 2 * 1024 * 1024;

  private final Path binaryPath;
  private final String socketPath;
  private final int grpcSubchannels;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final StringBuilder startupLogBuffer = new StringBuilder();

  private volatile Process process;
  private volatile long resolvedPid = -1L;

  public SidecarServerRuntime(String binaryPath, String socketPath) {
    this(binaryPath, socketPath, 0);
  }

  public SidecarServerRuntime(String binaryPath, String socketPath, int grpcSubchannels) {
    this.binaryPath = Paths.get(requireNonNull(binaryPath, "binaryPath"));
    this.socketPath = requireNonNull(socketPath, "socketPath");
    this.grpcSubchannels = Math.max(0, grpcSubchannels);
  }

  @Override
  public synchronized void start() throws IOException {
    if (process != null) {
      throw new IllegalStateException("SidecarServerRuntime already started");
    }
    if (!Files.isRegularFile(binaryPath) || !Files.isExecutable(binaryPath)) {
      throw new IOException("gcs-core-server binary is missing or not executable: " + binaryPath);
    }
    UdsServerRuntime.unlinkSocketFiles(socketPath);

    List<String> cmd = new ArrayList<String>();
    cmd.add(binaryPath.toAbsolutePath().toString());
    cmd.add("--socket");
    cmd.add(socketPath);
    cmd.add("--grpc-subchannels");
    cmd.add(Integer.toString(grpcSubchannels));
    cmd.add("--chunk-size");
    cmd.add(Integer.toString(DEFAULT_CHUNK_SIZE_BYTES));
    cmd.add("--idle-secs");
    cmd.add("0");
    cmd.add("--pdeathsig=true");
    cmd.add("--require-session");

    ProcessBuilder pb = new ProcessBuilder(cmd);
    pb.redirectErrorStream(true);
    Process started = pb.start();
    this.process = started;

    Thread logDrainer =
        new Thread(() -> drainOutput(started), "gcs-core-sidecar-log-drainer");
    logDrainer.setDaemon(true);
    logDrainer.start();
  }

  private void drainOutput(Process proc) {
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(proc.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        parsePidIfPresent(line);
        synchronized (startupLogBuffer) {
          if (startupLogBuffer.length() < 16384) {
            startupLogBuffer.append(line).append('\n');
          }
        }
      }
    } catch (IOException ignored) {
      // Child process stream closed during teardown.
    }
  }

  private void parsePidIfPresent(String line) {
    // Matches "gcs-core-server pid=<N> listening on <sock>" printed by lib.rs on startup.
    int idx = line.indexOf("pid=");
    if (idx >= 0) {
      int start = idx + 4;
      int end = start;
      while (end < line.length() && Character.isDigit(line.charAt(end))) {
        end++;
      }
      if (end > start) {
        try {
          resolvedPid = Long.parseLong(line.substring(start, end));
        } catch (NumberFormatException ignored) {
          // Ignore malformed pid token.
        }
      }
    }
  }

  public void recordServerPid(long pid) {
    if (pid > 0) {
      this.resolvedPid = pid;
    }
  }

  public String capturedLogs() {
    synchronized (startupLogBuffer) {
      return startupLogBuffer.toString();
    }
  }

  @Override
  public String socketPath() {
    return socketPath;
  }

  @Override
  public boolean isAlive() {
    Process p = this.process;
    return p != null && p.isAlive();
  }

  @Override
  public OptionalLong serverPid() {
    long pid = this.resolvedPid;
    return pid > 0 ? OptionalLong.of(pid) : OptionalLong.empty();
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    Process p = this.process;
    if (p != null) {
      p.destroy();
      try {
        if (!p.waitFor(5, TimeUnit.SECONDS)) {
          p.destroyForcibly();
          p.waitFor(2, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        p.destroyForcibly();
        Thread.currentThread().interrupt();
      }
    }
    UdsServerRuntime.unlinkSocketFiles(socketPath);
  }
}
