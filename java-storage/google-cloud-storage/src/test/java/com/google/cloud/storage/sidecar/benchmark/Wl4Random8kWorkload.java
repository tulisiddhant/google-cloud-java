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

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.sidecar.UdsTransport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Workload 4 ({@code WL4}): {@code Random8K} — Sequential Random {@code 8 KiB} Seeks on an Open
 * {@code 100 MiB} Object.
 *
 * <p>Against {@code bench-random-100mb.bin} ({@code 104,857,600} B), performs at least {@code
 * 2,000} sequential random {@code 8 KiB} ({@code 8,192} B) range reads at deterministic uniformly
 * distributed byte offsets in {@code [0, 100 MiB - 8,192]} ({@code [0, 104,849,408]}) generated
 * from fixed PRNG seed {@code 42L}.
 *
 * <ul>
 *   <li><b>Under {@code gRPC}:</b> Opens {@code 1} persistent {@link BlobReadSession} once before
 *       warmup, runs {@code 50} untimed warmup seeks on that open session, then loops until {@code
 *       seekCount >= 2,000} and {@code elapsed >= 120s} timing strictly {@code
 *       session.readAs(...RangeSpec.of(offset, 8192)).get(...)}, closing the session after the
 *       timed window completes.
 *   <li><b>Under {@code JSON}:</b> Runs {@code 5} untimed warmup seeks, then loops until {@code
 *       seekCount >= 2,000} and {@code elapsed >= 120s} timing {@code reader(...)} + {@code
 *       setChunkSize(0)} + {@code seek(offset)} + {@code limit(offset + 8192)} + drain {@code
 *       8,192} B + {@code close()}.
 * </ul>
 */
public final class Wl4Random8kWorkload implements Workload {

  public static final String WORKLOAD_ID = "WL4";
  public static final String DISPLAY_NAME_GRPC =
      "Random8K (2,000 Random 8 KiB Seeks on 1 Open 100 MiB Session)";
  public static final String DISPLAY_NAME_JSON =
      "Random8K (2,000 Random 8 KiB Seeks via ReadChannel)";
  public static final String OBJECT_NAME = "bench-random-100mb.bin";

  public static final long DEFAULT_OBJECT_SIZE_BYTES = 100L * 1024L * 1024L;
  public static final int DEFAULT_SEEK_BYTES = 8 * 1024;
  public static final long DEFAULT_MAX_OFFSET_INCLUSIVE =
      DEFAULT_OBJECT_SIZE_BYTES - DEFAULT_SEEK_BYTES;
  public static final long PRNG_SEED = 42L;
  private static final long WARMUP_PRNG_SEED = 42L ^ 0x9E3779B97F4A7C15L;

  public static final int DEFAULT_WARMUP_SEEKS_GRPC = 50;
  public static final int DEFAULT_WARMUP_SEEKS_JSON = 5;
  public static final int DEFAULT_MIN_SEEKS = 2_000;
  public static final long DEFAULT_MIN_DURATION_SEC = 120L;
  private static final long OP_TIMEOUT_SEC = 60L;

  private final int warmupSeeksGrpc;
  private final int warmupSeeksJson;
  private final int minSeeks;
  private final long minDurationNs;
  private final long objectSizeBytes;
  private final int seekBytes;
  private final long maxOffsetInclusive;

  public Wl4Random8kWorkload() {
    this(
        DEFAULT_WARMUP_SEEKS_GRPC,
        DEFAULT_WARMUP_SEEKS_JSON,
        DEFAULT_MIN_SEEKS,
        DEFAULT_MIN_DURATION_SEC,
        DEFAULT_OBJECT_SIZE_BYTES,
        DEFAULT_SEEK_BYTES);
  }

  public Wl4Random8kWorkload(
      int warmupSeeksGrpc, int warmupSeeksJson, int minSeeks, long minDurationSec) {
    this(
        warmupSeeksGrpc,
        warmupSeeksJson,
        minSeeks,
        minDurationSec,
        DEFAULT_OBJECT_SIZE_BYTES,
        DEFAULT_SEEK_BYTES);
  }

  public Wl4Random8kWorkload(
      int warmupSeeksGrpc,
      int warmupSeeksJson,
      int minSeeks,
      long minDurationSec,
      long objectSizeBytes,
      int seekBytes) {
    if (warmupSeeksGrpc < 0
        || warmupSeeksJson < 0
        || minSeeks <= 0
        || minDurationSec < 0
        || seekBytes <= 0
        || objectSizeBytes < seekBytes) {
      throw new IllegalArgumentException("Invalid WL4 parameters");
    }
    this.warmupSeeksGrpc = warmupSeeksGrpc;
    this.warmupSeeksJson = warmupSeeksJson;
    this.minSeeks = minSeeks;
    this.minDurationNs = TimeUnit.SECONDS.toNanos(minDurationSec);
    this.objectSizeBytes = objectSizeBytes;
    this.seekBytes = seekBytes;
    this.maxOffsetInclusive = objectSizeBytes - seekBytes;
  }

  @Override
  public String id() {
    return WORKLOAD_ID;
  }

  @Override
  public String displayName() {
    return DISPLAY_NAME_GRPC;
  }

  @Override
  public boolean isApplicable(UdsTransport protocol, BenchmarkConfig.BucketType bucketType) {
    if (protocol == UdsTransport.JSON && bucketType == BenchmarkConfig.BucketType.ZONAL) {
      return false;
    }
    return true;
  }

  @Override
  public WorkloadResult execute(ClientProvider clients, BenchmarkConfig config) throws Exception {
    if (!isApplicable(config.protocol(), config.bucketType())) {
      throw new IllegalArgumentException(
          "WL4 is N/A for protocol=" + config.protocol() + ", bucketType=" + config.bucketType());
    }

    Storage storage = clients.primaryClient();
    BlobId blobId = BlobId.of(config.bucketName(), OBJECT_NAME);
    if (config.protocol() == UdsTransport.GRPC) {
      return executeGrpc(clients, storage, blobId);
    }
    return executeJson(clients, storage, blobId);
  }

  private WorkloadResult executeGrpc(ClientProvider clients, Storage storage, BlobId blobId)
      throws Exception {
    try (BlobReadSession session =
        storage.blobReadSession(blobId).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS)) {
      Random warmupRng = new Random(WARMUP_PRNG_SEED);
      for (int w = 0; w < warmupSeeksGrpc; w++) {
        long offset = nextUniformOffset(warmupRng, maxOffsetInclusive);
        runSingleGrpcSeek(session, offset);
      }

      Random timedRng = new Random(PRNG_SEED);
      WorkloadResult.ProcessSnapshot beforeSnap =
          WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), true);
      Instant wlStartUtc = Instant.now();
      long windowStartNs = System.nanoTime();

      long[] latenciesNs = new long[Math.max(2048, minSeeks * 2)];
      int seekCount = 0;

      while (seekCount < minSeeks || (System.nanoTime() - windowStartNs) < minDurationNs) {
        long offset = nextUniformOffset(timedRng, maxOffsetInclusive);
        long elapsedNs = runSingleGrpcSeek(session, offset);
        if (seekCount == latenciesNs.length) {
          latenciesNs = Arrays.copyOf(latenciesNs, latenciesNs.length * 2);
        }
        latenciesNs[seekCount++] = elapsedNs;
      }

      Instant wlEndUtc = Instant.now();
      WorkloadResult.ProcessSnapshot afterSnap =
          WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), false);

      long totalTimedBytes = (long) seekCount * seekBytes;
      return WorkloadResult.fromSequentialSamples(
          WORKLOAD_ID,
          DISPLAY_NAME_GRPC,
          wlStartUtc,
          wlEndUtc,
          Arrays.copyOf(latenciesNs, seekCount),
          totalTimedBytes,
          WorkloadResult.DiagnosticDelta.between(beforeSnap, afterSnap));
    }
  }

  private WorkloadResult executeJson(ClientProvider clients, Storage storage, BlobId blobId)
      throws IOException {
    ByteBuffer reusableBuffer = ByteBuffer.allocate(seekBytes);

    Random warmupRng = new Random(WARMUP_PRNG_SEED);
    for (int w = 0; w < warmupSeeksJson; w++) {
      long offset = nextUniformOffset(warmupRng, maxOffsetInclusive);
      runSingleJsonSeek(storage, blobId, offset, reusableBuffer);
    }

    Random timedRng = new Random(PRNG_SEED);
    WorkloadResult.ProcessSnapshot beforeSnap =
        WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), true);
    Instant wlStartUtc = Instant.now();
    long windowStartNs = System.nanoTime();

    long[] latenciesNs = new long[Math.max(2048, minSeeks * 2)];
    int seekCount = 0;

    while (seekCount < minSeeks || (System.nanoTime() - windowStartNs) < minDurationNs) {
      long offset = nextUniformOffset(timedRng, maxOffsetInclusive);
      long elapsedNs = runSingleJsonSeek(storage, blobId, offset, reusableBuffer);
      if (seekCount == latenciesNs.length) {
        latenciesNs = Arrays.copyOf(latenciesNs, latenciesNs.length * 2);
      }
      latenciesNs[seekCount++] = elapsedNs;
    }

    Instant wlEndUtc = Instant.now();
    WorkloadResult.ProcessSnapshot afterSnap =
        WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), false);

    long totalTimedBytes = (long) seekCount * seekBytes;
    return WorkloadResult.fromSequentialSamples(
        WORKLOAD_ID,
        DISPLAY_NAME_JSON,
        wlStartUtc,
        wlEndUtc,
        Arrays.copyOf(latenciesNs, seekCount),
        totalTimedBytes,
        WorkloadResult.DiagnosticDelta.between(beforeSnap, afterSnap));
  }

  private long runSingleGrpcSeek(BlobReadSession session, long offset) throws Exception {
    long startNs = System.nanoTime();
    byte[] data =
        session
            .readAs(
                ReadProjectionConfigs.asFutureBytes()
                    .withRangeSpec(RangeSpec.of(offset, seekBytes)))
            .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
    long elapsedNs = System.nanoTime() - startNs;
    if (data == null || data.length != seekBytes) {
      throw new IllegalStateException(
          "WL4 gRPC seek at offset "
              + offset
              + " expected "
              + seekBytes
              + " bytes, got "
              + (data == null ? -1 : data.length));
    }
    return elapsedNs;
  }

  private long runSingleJsonSeek(
      Storage storage, BlobId blobId, long offset, ByteBuffer reusableBuffer) throws IOException {
    long startNs = System.nanoTime();
    long bytesRead;
    try (ReadChannel r =
        storage.reader(blobId, Storage.BlobSourceOption.shouldReturnRawInputStream(true))) {
      r.setChunkSize(0);
      r.seek(offset);
      r.limit(offset + seekBytes);
      bytesRead = drainChannel(r, reusableBuffer);
    }
    long elapsedNs = System.nanoTime() - startNs;
    if (bytesRead != seekBytes) {
      throw new IllegalStateException(
          "WL4 JSON seek at offset "
              + offset
              + " expected "
              + seekBytes
              + " bytes, got "
              + bytesRead);
    }
    return elapsedNs;
  }

  private static long drainChannel(ReadChannel channel, ByteBuffer buffer) throws IOException {
    long total = 0L;
    buffer.clear();
    int n;
    while ((n = channel.read(buffer)) != -1) {
      total += n;
      if (!buffer.hasRemaining()) {
        buffer.clear();
      }
    }
    return total;
  }

  /**
   * Returns a uniformly distributed byte offset in {@code [0, maxOffsetInclusive]} using standard
   * Java 8-compatible rejection sampling over {@link Random#nextLong()}.
   */
  public static long nextUniformOffset(Random rng, long maxOffsetInclusive) {
    if (maxOffsetInclusive < 0L) {
      throw new IllegalArgumentException("maxOffsetInclusive must be >= 0");
    }
    if (maxOffsetInclusive == 0L) {
      return 0L;
    }
    long bound = maxOffsetInclusive + 1L;
    long mask = bound - 1L;
    if ((bound & mask) == 0L) {
      return rng.nextLong() & mask;
    }
    long u = rng.nextLong() >>> 1;
    long r = u % bound;
    while (u + mask - r < 0L) {
      u = rng.nextLong() >>> 1;
      r = u % bound;
    }
    return r;
  }
}
