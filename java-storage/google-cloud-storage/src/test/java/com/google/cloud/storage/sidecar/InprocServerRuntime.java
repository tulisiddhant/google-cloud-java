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

import com.sun.jna.Library;
import com.sun.jna.Native;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Single-process UDS runtime ({@code uds-inproc}) that loads unmodified {@code
 * libstorage_grpc_uds.so} via JNA and hosts {@code gcscore.v1.GcsCore} on an in-process background
 * Tokio runtime thread bound to a dedicated {@code AF_UNIX} socket path.
 */
public final class InprocServerRuntime implements UdsServerRuntime {

  private static final ConcurrentHashMap<String, GcsCoreNativeLibrary> LOADED_LIBRARIES =
      new ConcurrentHashMap<String, GcsCoreNativeLibrary>();
  private static final long CURRENT_JVM_PID = resolveCurrentProcessPid();

  /** JNA C-ABI binding matching {@code include/gcs_core_uds.h}. */
  public interface GcsCoreNativeLibrary extends Library {
    int gcs_core_start_inproc(String socketPath);

    int gcs_core_start_inproc_with_subchannels(String socketPath, int grpcSubchannels);

    int gcs_core_stop_inproc(String socketPath);
  }

  private final Path libraryPath;
  private final String socketPath;
  private final int grpcSubchannels;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private volatile GcsCoreNativeLibrary nativeLib;
  private volatile boolean running = false;

  public InprocServerRuntime(String libraryPath, String socketPath) {
    this(libraryPath, socketPath, 0);
  }

  public InprocServerRuntime(String libraryPath, String socketPath, int grpcSubchannels) {
    this.libraryPath = Paths.get(requireNonNull(libraryPath, "libraryPath"));
    this.socketPath = requireNonNull(socketPath, "socketPath");
    this.grpcSubchannels = Math.max(0, grpcSubchannels);
  }

  @Override
  public synchronized void start() throws IOException {
    if (running) {
      throw new IllegalStateException("InprocServerRuntime already started");
    }
    if (!Files.isRegularFile(libraryPath)) {
      throw new IOException("libstorage_grpc_uds.so does not exist: " + libraryPath);
    }
    UdsServerRuntime.unlinkSocketFiles(socketPath);

    String absPath = libraryPath.toRealPath().toString();
    GcsCoreNativeLibrary lib = LOADED_LIBRARIES.get(absPath);
    if (lib == null) {
      GcsCoreNativeLibrary loaded = Native.load(absPath, GcsCoreNativeLibrary.class);
      GcsCoreNativeLibrary existing = LOADED_LIBRARIES.putIfAbsent(absPath, loaded);
      lib = existing != null ? existing : loaded;
    }
    this.nativeLib = lib;

    int rc =
        grpcSubchannels > 0
            ? lib.gcs_core_start_inproc_with_subchannels(socketPath, grpcSubchannels)
            : lib.gcs_core_start_inproc(socketPath);
    if (rc != 0) {
      UdsServerRuntime.unlinkSocketFiles(socketPath);
      throw new IOException(
          "gcs_core_start_inproc returned error rc=" + rc + " for socket " + socketPath);
    }
    this.running = true;
  }

  @Override
  public String socketPath() {
    return socketPath;
  }

  @Override
  public boolean isAlive() {
    return running && !closed.get();
  }

  @Override
  public OptionalLong serverPid() {
    return CURRENT_JVM_PID > 0 ? OptionalLong.of(CURRENT_JVM_PID) : OptionalLong.empty();
  }

  @Override
  public synchronized void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    try {
      if (running && nativeLib != null) {
        running = false;
        nativeLib.gcs_core_stop_inproc(socketPath);
      }
    } finally {
      UdsServerRuntime.unlinkSocketFiles(socketPath);
    }
  }

  static long resolveCurrentProcessPid() {
    try {
      String jvmName = ManagementFactory.getRuntimeMXBean().getName();
      int at = jvmName.indexOf('@');
      if (at > 0) {
        return Long.parseLong(jvmName.substring(0, at));
      }
    } catch (RuntimeException ignored) {
      // Fall back to empty OptionalLong if non-standard JVM name format.
    }
    return -1L;
  }
}
