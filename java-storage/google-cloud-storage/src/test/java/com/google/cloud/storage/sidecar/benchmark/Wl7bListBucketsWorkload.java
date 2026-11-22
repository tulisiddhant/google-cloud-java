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
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.sidecar.UdsTransport;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Workload 7b ({@code WL7b}): {@code ListBuckets} — Single-Page Control-Plane Project Bucket
 * Listing ({@code pageSize = 100}).
 *
 * <p>Applicable to {@code gRPC} only (executed during both Regional and Zonal gRPC suite runs).
 * Runs {@code 5} untimed warmup calls, then loops until {@code opCount >= 50} and {@code elapsed >=
 * 120s}, timing single-page {@code storage.list(pageSize(100))} + {@code page.getValues()}
 * iteration and asserting {@code count > 0} and invariant across all calls.
 */
public final class Wl7bListBucketsWorkload implements Workload {

  public static final String WORKLOAD_ID = "WL7b";
  public static final String DISPLAY_NAME = "ListBuckets (Project Buckets, pageSize=100, Single Page)";
  public static final long DEFAULT_PAGE_SIZE = 100L;

  public static final int DEFAULT_WARMUP_OPS = 5;
  public static final int DEFAULT_MIN_OPS = 50;
  public static final long DEFAULT_MIN_DURATION_SEC = 120L;

  private final int warmupOps;
  private final int minOps;
  private final long minDurationNs;
  private final long pageSize;

  public Wl7bListBucketsWorkload() {
    this(DEFAULT_WARMUP_OPS, DEFAULT_MIN_OPS, DEFAULT_MIN_DURATION_SEC, DEFAULT_PAGE_SIZE);
  }

  public Wl7bListBucketsWorkload(int warmupOps, int minOps, long minDurationSec) {
    this(warmupOps, minOps, minDurationSec, DEFAULT_PAGE_SIZE);
  }

  public Wl7bListBucketsWorkload(int warmupOps, int minOps, long minDurationSec, long pageSize) {
    if (warmupOps < 0 || minOps <= 0 || minDurationSec < 0 || pageSize <= 0) {
      throw new IllegalArgumentException("Invalid WL7b parameters");
    }
    this.warmupOps = warmupOps;
    this.minOps = minOps;
    this.minDurationNs = TimeUnit.SECONDS.toNanos(minDurationSec);
    this.pageSize = pageSize;
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
          "WL7b is gRPC-only; N/A for protocol=" + config.protocol());
    }

    Storage storage = clients.primaryClient();
    int expectedBucketCount = -1;

    for (int w = 0; w < warmupOps; w++) {
      ListCallSample sample = runSingleListBuckets(storage, expectedBucketCount);
      expectedBucketCount = sample.itemCount;
    }

    WorkloadResult.ProcessSnapshot beforeSnap =
        WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), true);
    Instant wlStartUtc = Instant.now();
    long windowStartNs = System.nanoTime();

    long[] latenciesNs = new long[Math.max(128, minOps * 2)];
    int count = 0;

    while (count < minOps || (System.nanoTime() - windowStartNs) < minDurationNs) {
      ListCallSample sample = runSingleListBuckets(storage, expectedBucketCount);
      expectedBucketCount = sample.itemCount;
      if (count == latenciesNs.length) {
        latenciesNs = Arrays.copyOf(latenciesNs, latenciesNs.length * 2);
      }
      latenciesNs[count++] = sample.elapsedNs;
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

  private ListCallSample runSingleListBuckets(Storage storage, int expectedCount) {
    long startNs = System.nanoTime();
    Page<Bucket> page = storage.list(Storage.BucketListOption.pageSize(pageSize));
    int itemCount = 0;
    for (Bucket bucket : page.getValues()) {
      if (bucket != null) {
        itemCount++;
      }
    }
    long elapsedNs = System.nanoTime() - startNs;
    if (itemCount <= 0) {
      throw new IllegalStateException("WL7b ListBuckets expected > 0 buckets, got 0");
    }
    if (expectedCount > 0 && itemCount != expectedCount) {
      throw new IllegalStateException(
          "WL7b ListBuckets count changed mid-run: expected "
              + expectedCount
              + ", got "
              + itemCount);
    }
    return new ListCallSample(elapsedNs, itemCount);
  }

  private static final class ListCallSample {
    final long elapsedNs;
    final int itemCount;

    ListCallSample(long elapsedNs, int itemCount) {
      this.elapsedNs = elapsedNs;
      this.itemCount = itemCount;
    }
  }
}
