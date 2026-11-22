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
import java.util.concurrent.TimeUnit;

/**
 * Workload 2 ({@code WL2}): {@code Read50x2x4 Serial} — Sequential Strided Range Reads.
 *
 * <p>From {@code bench-open-200mb.bin} ({@code 200 MiB}), sequentially reads {@code 50}
 * non-contiguous {@code 2 MiB} ({@code 2,097,152} B) ranges at a {@code 4 MiB} ({@code 4,194,304}
 * B) stride ({@code 100 MiB = 104,857,600} B per pass), waiting for each range read to complete
 * before issuing the next.
 */
public final class Wl2ReadSerialWorkload implements Workload {

  public static final String WORKLOAD_ID = "WL2";
  public static final String DISPLAY_NAME =
      "Read50x2x4 Serial (50 \u00d7 2 MiB Ranges at 4 MiB Stride)";
  public static final String OBJECT_NAME = "bench-open-200mb.bin";

  public static final int DEFAULT_RANGE_COUNT = 50;
  public static final int DEFAULT_RANGE_BYTES = 2 * 1024 * 1024;
  public static final long DEFAULT_STRIDE_BYTES = 4L * 1024L * 1024L;
  public static final long EXPECTED_DEFAULT_PASS_BYTES =
      DEFAULT_RANGE_COUNT * (long) DEFAULT_RANGE_BYTES;

  public static final int DEFAULT_WARMUP_PASSES = 1;
  public static final int DEFAULT_MIN_PASSES = 20;
  public static final long DEFAULT_MIN_DURATION_SEC = 120L;
  private static final long OP_TIMEOUT_SEC = 120L;

  private final int warmupPasses;
  private final int minPasses;
  private final long minDurationNs;
  private final int rangeCount;
  private final int rangeBytes;
  private final long strideBytes;
  private final long expectedPassBytes;

  public Wl2ReadSerialWorkload() {
    this(DEFAULT_WARMUP_PASSES, DEFAULT_MIN_PASSES, DEFAULT_MIN_DURATION_SEC);
  }

  public Wl2ReadSerialWorkload(int warmupPasses, int minPasses, long minDurationSec) {
    this(
        warmupPasses,
        minPasses,
        minDurationSec,
        DEFAULT_RANGE_COUNT,
        DEFAULT_RANGE_BYTES,
        DEFAULT_STRIDE_BYTES);
  }

  public Wl2ReadSerialWorkload(
      int warmupPasses,
      int minPasses,
      long minDurationSec,
      int rangeCount,
      int rangeBytes,
      long strideBytes) {
    if (warmupPasses < 0
        || minPasses <= 0
        || minDurationSec < 0
        || rangeCount <= 0
        || rangeBytes <= 0
        || strideBytes <= 0) {
      throw new IllegalArgumentException("Invalid WL2 parameters");
    }
    this.warmupPasses = warmupPasses;
    this.minPasses = minPasses;
    this.minDurationNs = TimeUnit.SECONDS.toNanos(minDurationSec);
    this.rangeCount = rangeCount;
    this.rangeBytes = rangeBytes;
    this.strideBytes = strideBytes;
    this.expectedPassBytes = (long) rangeCount * rangeBytes;
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
    if (protocol == UdsTransport.JSON && bucketType == BenchmarkConfig.BucketType.ZONAL) {
      return false;
    }
    return true;
  }

  @Override
  public WorkloadResult execute(ClientProvider clients, BenchmarkConfig config) throws Exception {
    if (!isApplicable(config.protocol(), config.bucketType())) {
      throw new IllegalArgumentException(
          "WL2 is N/A for protocol=" + config.protocol() + ", bucketType=" + config.bucketType());
    }

    Storage storage = clients.primaryClient();
    BlobId blobId = BlobId.of(config.bucketName(), OBJECT_NAME);
    boolean useGrpc = (config.protocol() == UdsTransport.GRPC);
    ByteBuffer reusableJsonBuffer = useGrpc ? null : ByteBuffer.allocate(rangeBytes);

    // 1. Untimed warmup phase (1 full 100 MiB serial pass by default)
    for (int w = 0; w < warmupPasses; w++) {
      if (useGrpc) {
        runSingleGrpcSerialPass(storage, blobId);
      } else {
        runSingleJsonSerialPass(storage, blobId, reusableJsonBuffer);
      }
    }

    // 2. Timed steady-state loop (minPasses >= 20 AND elapsed >= 120s)
    WorkloadResult.ProcessSnapshot beforeSnap =
        WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), true);
    Instant wlStartUtc = Instant.now();
    long windowStartNs = System.nanoTime();

    long[] latenciesNs = new long[Math.max(64, minPasses * 2)];
    int count = 0;

    while (count < minPasses || (System.nanoTime() - windowStartNs) < minDurationNs) {
      long elapsedNs =
          useGrpc
              ? runSingleGrpcSerialPass(storage, blobId)
              : runSingleJsonSerialPass(storage, blobId, reusableJsonBuffer);
      if (count == latenciesNs.length) {
        latenciesNs = Arrays.copyOf(latenciesNs, latenciesNs.length * 2);
      }
      latenciesNs[count++] = elapsedNs;
    }

    Instant wlEndUtc = Instant.now();
    WorkloadResult.ProcessSnapshot afterSnap =
        WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), false);

    long totalTimedBytes = (long) count * expectedPassBytes;
    return WorkloadResult.fromSequentialSamples(
        WORKLOAD_ID,
        DISPLAY_NAME,
        wlStartUtc,
        wlEndUtc,
        Arrays.copyOf(latenciesNs, count),
        totalTimedBytes,
        WorkloadResult.DiagnosticDelta.between(beforeSnap, afterSnap));
  }

  /**
   * Opens 1 {@link BlobReadSession} outside {@code [startNs, endNs]}, times the 50 sequential
   * {@code 2 MiB} range reads, and closes the session outside {@code [startNs, endNs]}.
   */
  private long runSingleGrpcSerialPass(Storage storage, BlobId blobId) throws Exception {
    try (BlobReadSession session =
        storage.blobReadSession(blobId).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS)) {
      long startNs = System.nanoTime();
      long passBytes = 0L;
      for (int i = 0; i < rangeCount; i++) {
        long offset = i * strideBytes;
        byte[] data =
            session
                .readAs(
                    ReadProjectionConfigs.asFutureBytes()
                        .withRangeSpec(RangeSpec.of(offset, rangeBytes)))
                .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
        if (data == null || data.length != rangeBytes) {
          throw new IllegalStateException(
              "WL2 gRPC range "
                  + i
                  + " at offset "
                  + offset
                  + " expected "
                  + rangeBytes
                  + " bytes, got "
                  + (data == null ? -1 : data.length));
        }
        passBytes += data.length;
      }
      long elapsedNs = System.nanoTime() - startNs;
      if (passBytes != expectedPassBytes) {
        throw new IllegalStateException(
            "WL2 gRPC pass expected " + expectedPassBytes + " bytes, got " + passBytes);
      }
      return elapsedNs;
    }
  }

  /**
   * Times 50 sequential unidirectional {@link ReadChannel} range reads ({@code seek(offset)} +
   * {@code limit(offset + 2 MiB)} + {@code setChunkSize(0)}).
   */
  private long runSingleJsonSerialPass(
      Storage storage, BlobId blobId, ByteBuffer reusableBuffer) throws IOException {
    long startNs = System.nanoTime();
    long passBytes = 0L;
    for (int i = 0; i < rangeCount; i++) {
      long offset = i * strideBytes;
      try (ReadChannel r =
          storage.reader(blobId, Storage.BlobSourceOption.shouldReturnRawInputStream(true))) {
        r.setChunkSize(0);
        r.seek(offset);
        r.limit(offset + rangeBytes);
        long rangeRead = drainChannel(r, reusableBuffer);
        if (rangeRead != rangeBytes) {
          throw new IllegalStateException(
              "WL2 JSON range "
                  + i
                  + " at offset "
                  + offset
                  + " expected "
                  + rangeBytes
                  + " bytes, got "
                  + rangeRead);
        }
        passBytes += rangeRead;
      }
    }
    long elapsedNs = System.nanoTime() - startNs;
    if (passBytes != expectedPassBytes) {
      throw new IllegalStateException(
          "WL2 JSON pass expected " + expectedPassBytes + " bytes, got " + passBytes);
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
}
