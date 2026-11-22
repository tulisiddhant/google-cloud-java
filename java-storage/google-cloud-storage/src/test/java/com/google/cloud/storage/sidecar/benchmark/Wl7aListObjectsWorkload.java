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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.sidecar.UdsTransport;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Workload 7a ({@code WL7a}): {@code ListObjects} — Single-Page Control-Plane Object Listing
 * ({@code 100} Objects Under {@code list-bench-100/}).
 *
 * <p>Applicable to {@code gRPC} only (Regional and Zonal). Runs {@code 5} untimed warmup calls,
 * then loops until {@code opCount >= 100} and {@code elapsed >= 120s}, timing single-page {@code
 * storage.list(bucket, prefix("list-bench-100/"), pageSize(100))} + {@code page.getValues()}
 * iteration and asserting {@code count == 100} on every call.
 */
public final class Wl7aListObjectsWorkload implements Workload {

  public static final String WORKLOAD_ID = "WL7a";
  public static final String DISPLAY_NAME = "ListObjects (100 Objects Under Prefix, Single Page)";
  public static final String DEFAULT_PREFIX = "list-bench-100/";
  public static final long DEFAULT_PAGE_SIZE = 100L;
  public static final int EXPECTED_DEFAULT_COUNT = 100;

  public static final int DEFAULT_WARMUP_OPS = 5;
  public static final int DEFAULT_MIN_OPS = 100;
  public static final long DEFAULT_MIN_DURATION_SEC = 120L;

  private final int warmupOps;
  private final int minOps;
  private final long minDurationNs;
  private final String prefix;
  private final long pageSize;
  private final int expectedCount;

  public Wl7aListObjectsWorkload() {
    this(
        DEFAULT_WARMUP_OPS,
        DEFAULT_MIN_OPS,
        DEFAULT_MIN_DURATION_SEC,
        DEFAULT_PREFIX,
        DEFAULT_PAGE_SIZE,
        EXPECTED_DEFAULT_COUNT);
  }

  public Wl7aListObjectsWorkload(int warmupOps, int minOps, long minDurationSec) {
    this(
        warmupOps,
        minOps,
        minDurationSec,
        DEFAULT_PREFIX,
        DEFAULT_PAGE_SIZE,
        EXPECTED_DEFAULT_COUNT);
  }

  public Wl7aListObjectsWorkload(
      int warmupOps,
      int minOps,
      long minDurationSec,
      String prefix,
      long pageSize,
      int expectedCount) {
    if (warmupOps < 0
        || minOps <= 0
        || minDurationSec < 0
        || prefix == null
        || pageSize <= 0
        || expectedCount <= 0) {
      throw new IllegalArgumentException("Invalid WL7a parameters");
    }
    this.warmupOps = warmupOps;
    this.minOps = minOps;
    this.minDurationNs = TimeUnit.SECONDS.toNanos(minDurationSec);
    this.prefix = prefix;
    this.pageSize = pageSize;
    this.expectedCount = expectedCount;
  }

  @Override
  public String id() {
    return WORKLOAD_ID;
  }

  @Override
  public String displayName() {
    return DISPLAY_NAME;
  }

  @Override
  public boolean isApplicable(UdsTransport protocol, BenchmarkConfig.BucketType bucketType) {
    return protocol == UdsTransport.GRPC;
  }

  @Override
  public WorkloadResult execute(ClientProvider clients, BenchmarkConfig config) throws Exception {
    if (!isApplicable(config.protocol(), config.bucketType())) {
      throw new IllegalArgumentException(
          "WL7a is gRPC-only; N/A for protocol=" + config.protocol());
    }

    Storage storage = clients.primaryClient();
    String bucket = config.bucketName();

    for (int w = 0; w < warmupOps; w++) {
      runSingleListObjects(storage, bucket);
    }

    WorkloadResult.ProcessSnapshot beforeSnap =
        WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), true);
    Instant wlStartUtc = Instant.now();
    long windowStartNs = System.nanoTime();

    long[] latenciesNs = new long[Math.max(256, minOps * 2)];
    int count = 0;

    while (count < minOps || (System.nanoTime() - windowStartNs) < minDurationNs) {
      long elapsedNs = runSingleListObjects(storage, bucket);
      if (count == latenciesNs.length) {
        latenciesNs = Arrays.copyOf(latenciesNs, latenciesNs.length * 2);
      }
      latenciesNs[count++] = elapsedNs;
    }

    Instant wlEndUtc = Instant.now();
    WorkloadResult.ProcessSnapshot afterSnap =
        WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), false);

    return WorkloadResult.fromSequentialSamples(
        WORKLOAD_ID,
        DISPLAY_NAME,
        wlStartUtc,
        wlEndUtc,
        Arrays.copyOf(latenciesNs, count),
        0L,
        WorkloadResult.DiagnosticDelta.between(beforeSnap, afterSnap));
  }

  private long runSingleListObjects(Storage storage, String bucket) {
    long startNs = System.nanoTime();
    Page<Blob> page =
        storage.list(
            bucket,
            Storage.BlobListOption.prefix(prefix),
            Storage.BlobListOption.pageSize(pageSize));
    int itemCount = 0;
    for (Blob blob : page.getValues()) {
      if (blob != null) {
        itemCount++;
      }
    }
    long elapsedNs = System.nanoTime() - startNs;
    if (itemCount != expectedCount) {
      throw new IllegalStateException(
          "WL7a ListObjects under prefix '"
              + prefix
              + "' expected "
              + expectedCount
              + " items, got "
              + itemCount);
    }
    return elapsedNs;
  }
}
