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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.sidecar.UdsTransport;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Workload 1 ({@code WL1}): {@code Open} — Bidirectional Read Session Handshake.
 *
 * <p>Measures pure {@code BidiReadObject} session-open handshake latency against {@code
 * bench-open-200mb.bin} ({@code 200 MiB = 209,715,200} bytes) without reading payload bytes,
 * keeping {@code session.close()} strictly outside {@code [startNs, endNs]}.
 */
public final class Wl1OpenWorkload implements Workload {

  public static final String WORKLOAD_ID = "WL1";
  public static final String DISPLAY_NAME = "Open (200 MiB BidiReadObject Session Handshake)";
  public static final String OBJECT_NAME = "bench-open-200mb.bin";
  public static final long EXPECTED_OBJECT_SIZE_BYTES = 200L * 1024L * 1024L;

  public static final int DEFAULT_WARMUP_ITERATIONS = 10;
  public static final int DEFAULT_MIN_ITERATIONS = 100;
  public static final long DEFAULT_MIN_DURATION_SEC = 120L;
  private static final long OPEN_TIMEOUT_SEC = 120L;

  private final int warmupIterations;
  private final int minIterations;
  private final long minDurationNs;

  public Wl1OpenWorkload() {
    this(DEFAULT_WARMUP_ITERATIONS, DEFAULT_MIN_ITERATIONS, DEFAULT_MIN_DURATION_SEC);
  }

  public Wl1OpenWorkload(int warmupIterations, int minIterations, long minDurationSec) {
    if (warmupIterations < 0 || minIterations <= 0 || minDurationSec < 0) {
      throw new IllegalArgumentException(
          "Invalid WL1 parameters: warmup="
              + warmupIterations
              + ", minIterations="
              + minIterations
              + ", minDurationSec="
              + minDurationSec);
    }
    this.warmupIterations = warmupIterations;
    this.minIterations = minIterations;
    this.minDurationNs = TimeUnit.SECONDS.toNanos(minDurationSec);
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
      throw new IllegalArgumentException("WL1 Open is gRPC-only; got " + config.protocol());
    }

    Storage storage = clients.primaryClient();
    BlobId blobId = BlobId.of(config.bucketName(), OBJECT_NAME);

    // 1. Untimed warmup phase (10 session opens + closes by default)
    for (int i = 0; i < warmupIterations; i++) {
      try (BlobReadSession session =
          storage.blobReadSession(blobId).get(OPEN_TIMEOUT_SEC, TimeUnit.SECONDS)) {
        verifyFixtureMetadata(session.getBlobInfo());
      }
    }

    // 2. Timed steady-state loop (minIterations >= 100 AND elapsed >= 120s)
    WorkloadResult.ProcessSnapshot beforeSnap =
        WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), true);
    Instant wlStartUtc = Instant.now();
    long windowStartNs = System.nanoTime();

    long[] latenciesNs = new long[Math.max(256, minIterations * 2)];
    int count = 0;

    while (count < minIterations || (System.nanoTime() - windowStartNs) < minDurationNs) {
      BlobReadSession session = null;
      try {
        long startNs = System.nanoTime();
        session = storage.blobReadSession(blobId).get(OPEN_TIMEOUT_SEC, TimeUnit.SECONDS);
        long elapsedNs = System.nanoTime() - startNs;

        // Metadata assertion and session.close() execute strictly outside [startNs, endNs]
        verifyFixtureMetadata(session.getBlobInfo());
        if (count == latenciesNs.length) {
          latenciesNs = Arrays.copyOf(latenciesNs, latenciesNs.length * 2);
        }
        latenciesNs[count++] = elapsedNs;
      } finally {
        if (session != null) {
          session.close();
        }
      }
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

  private static void verifyFixtureMetadata(BlobInfo info) {
    if (info == null || info.getSize() == null || info.getSize() != EXPECTED_OBJECT_SIZE_BYTES) {
      throw new IllegalStateException(
          "WL1 expected "
              + EXPECTED_OBJECT_SIZE_BYTES
              + "-byte object metadata for "
              + OBJECT_NAME
              + ", got: "
              + (info == null ? "null" : info.getSize()));
    }
  }
}
