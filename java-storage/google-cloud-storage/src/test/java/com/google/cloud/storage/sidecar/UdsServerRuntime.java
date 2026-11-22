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
import java.nio.file.Paths;
import java.util.OptionalLong;
import java.util.UUID;

/** Lifecycle abstraction for starting and stopping a local {@code GcsCore} UDS server. */
public interface UdsServerRuntime extends AutoCloseable {

  /** Starts the UDS server runtime and prepares its socket listener. */
  void start() throws IOException;

  /** Returns the filesystem Unix Domain Socket path bound by this runtime. */
  String socketPath();

  /** Returns {@code true} if the underlying server runtime is currently alive. */
  boolean isAlive();

  /** Returns the OS process ID of the server when available. */
  OptionalLong serverPid();

  @Override
  void close();

  /** Generates a unique per-process filesystem socket path {@code /tmp/gcs-uds-<pid>-<uuid>.sock}. */
  static String newUniqueSocketPath() {
    String jvmName = ManagementFactory.getRuntimeMXBean().getName();
    int atIdx = jvmName.indexOf('@');
    String pidPart = atIdx > 0 ? jvmName.substring(0, atIdx) : "jvm";
    String shortUuid = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    return "/tmp/gcs-uds-" + pidPart + "-" + shortUuid + ".sock";
  }

  /** Best-effort removal of both {@code <socketPath>} and its companion {@code <socketPath>.shm}. */
  static void unlinkSocketFiles(String socketPath) {
    if (socketPath == null || socketPath.isEmpty() || socketPath.startsWith("@")) {
      return;
    }
    try {
      Files.deleteIfExists(Paths.get(socketPath));
    } catch (IOException ignored) {
      // Best-effort socket cleanup.
    }
    try {
      Files.deleteIfExists(Paths.get(socketPath + ".shm"));
    } catch (IOException ignored) {
      // Best-effort companion shared-memory socket cleanup.
    }
  }
}
