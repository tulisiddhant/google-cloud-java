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

package com.google.cloud.storage.sidecar.benchmark;

import static java.util.Objects.requireNonNull;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.DriverMode;
import com.google.cloud.storage.sidecar.UdsStorage;
import com.google.cloud.storage.sidecar.UdsTransport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Symmetric {@link Storage} client pool manager across {@code native} ({@code 16}-client Cloudpath
 * pool) and {@code uds-sidecar} ({@code 1} shared {@link UdsStorage} proxy over {@code AF_UNIX}).
 */
public final class ClientProvider implements AutoCloseable {

  private final BenchmarkConfig config;
  private final List<Storage> clients;
  private final UdsStorage udsStorage;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private volatile Storage adminClient;

  private ClientProvider(
      BenchmarkConfig config, List<Storage> clients, UdsStorage udsStorage) {
    this.config = config;
    this.clients = Collections.unmodifiableList(clients);
    this.udsStorage = udsStorage;
  }

  /** Wraps an existing client instance for hermetic workload unit tests. */
  public static ClientProvider forSingleClient(BenchmarkConfig config, Storage client) {
    return forSingleClientWithAdmin(config, client, null);
  }

  /** Wraps an existing client and out-of-band admin client for hermetic cleanup unit tests. */
  public static ClientProvider forSingleClientWithAdmin(
      BenchmarkConfig config, Storage client, Storage adminClient) {
    requireNonNull(config, "config");
    requireNonNull(client, "client");
    UdsStorage uds = (client instanceof UdsStorage) ? (UdsStorage) client : null;
    ClientProvider provider = new ClientProvider(config, Collections.singletonList(client), uds);
    provider.adminClient = adminClient;
    return provider;
  }

  /** Wraps an explicit client pool for hermetic multi-client workload unit tests. */
  public static ClientProvider forClientPool(BenchmarkConfig config, List<Storage> pool) {
    requireNonNull(config, "config");
    requireNonNull(pool, "pool");
    if (pool.isEmpty()) {
      throw new IllegalArgumentException("pool must be non-empty");
    }
    UdsStorage uds =
        (pool.size() == 1 && pool.get(0) instanceof UdsStorage)
            ? (UdsStorage) pool.get(0)
            : null;
    return new ClientProvider(config, new ArrayList<Storage>(pool), uds);
  }

  /** Instantiates and pre-warms the driver clients specified by {@code config}. */
  public static ClientProvider create(BenchmarkConfig config) throws IOException {
    requireNonNull(config, "config");
    if (config.driver() == BenchmarkConfig.DriverType.NATIVE) {
      List<Storage> pool = new ArrayList<Storage>(config.nativePoolSize());
      boolean ok = false;
      try {
        for (int i = 0; i < config.nativePoolSize(); i++) {
          pool.add(buildNativeClient(config.projectId(), config.protocol()));
        }
        ok = true;
        return new ClientProvider(config, pool, null);
      } finally {
        if (!ok) {
          closeAllQuietly(pool);
        }
      }
    }

    if (config.driver() == BenchmarkConfig.DriverType.UDS_SIDECAR) {
      StorageOptions baseOptions = buildBaseOptions(config.projectId(), config.protocol());
      UdsStorage uds =
          UdsStorage.newBuilder(baseOptions)
              .setDriverMode(DriverMode.SIDECAR)
              .setTransport(config.protocol())
              .setSidecarBinaryPath(config.sidecarBinaryPath())
              .setSocketPath(config.socketPath())
              .build();
      return new ClientProvider(config, Collections.<Storage>singletonList(uds), uds);
    }

    if (config.driver() == BenchmarkConfig.DriverType.UDS_INPROC) {
      StorageOptions baseOptions = buildBaseOptions(config.projectId(), config.protocol());
      UdsStorage uds =
          UdsStorage.newBuilder(baseOptions)
              .setDriverMode(DriverMode.INPROC)
              .setTransport(config.protocol())
              .setInprocLibraryPath(config.inprocLibraryPath())
              .setSocketPath(config.socketPath())
              .build();
      return new ClientProvider(config, Collections.<Storage>singletonList(uds), uds);
    }

    throw new UnsupportedOperationException("Unsupported driver: " + config.driver());
  }

  private static StorageOptions buildBaseOptions(String projectId, UdsTransport protocol) {
    if (protocol == UdsTransport.GRPC) {
      return StorageOptions.grpc()
          .setProjectId(projectId)
          .setAttemptDirectPath(false)
          .build();
    }
    return StorageOptions.http().setProjectId(projectId).build();
  }

  private static Storage buildNativeClient(String projectId, UdsTransport protocol) {
    return buildBaseOptions(projectId, protocol).getService();
  }

  /** Returns the primary client for single-threaded workloads. */
  public Storage primaryClient() {
    return clients.get(0);
  }

  /**
   * Returns the symmetric worker client ({@code nativeClientPool.get(workerIdx % 16)} on Native
   * Java, or the shared {@link UdsStorage} instance on UDS).
   */
  public Storage clientForWorker(int workerIndex) {
    int idx = Math.floorMod(workerIndex, clients.size());
    return clients.get(idx);
  }

  public Optional<UdsStorage> udsClient() {
    return Optional.ofNullable(udsStorage);
  }

  /**
   * Returns an out-of-band Native {@link Storage} client strictly for untimed pre-flight checks and
   * post-window {@code WL6} slot cleanup.
   */
  public synchronized Storage adminNativeClient() {
    if (adminClient == null) {
      UdsTransport adminTransport =
          config.bucketType() == BenchmarkConfig.BucketType.ZONAL
              ? UdsTransport.GRPC
              : UdsTransport.JSON;
      adminClient = buildNativeClient(config.projectId(), adminTransport);
    }
    return adminClient;
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    Storage admin = this.adminClient;
    if (admin != null) {
      try {
        admin.close();
      } catch (Exception ignored) {
        // Best-effort cleanup.
      }
    }
    closeAllQuietly(clients);
  }

  private static void closeAllQuietly(List<Storage> list) {
    for (Storage s : list) {
      try {
        s.close();
      } catch (Exception ignored) {
        // Best-effort cleanup.
      }
    }
  }
}
