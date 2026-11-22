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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Workload 3 ({@code WL3}): {@code Read50x2x4 Concurrent} — Concurrent Strided Range Reads on 1
 * Open Object.
 *
 * <p>From {@code bench-open-200mb.bin} ({@code 200 MiB}), dispatches all {@code 50} non-contiguous
 * {@code 2 MiB} ({@code 2,097,152} B) range reads at a {@code 4 MiB} ({@code 4,194,304} B) stride
 * concurrently and awaits completion of all {@code 50} ranges ({@code 100 MiB = 104,857,600} B per
 * pass).
 *
 * <ul>
 *   <li><b>Under {@code gRPC}:</b> Strict 1:1 single-session symmetry — opens {@code 1} {@link
 *       BlobReadSession} outside {@code [startNs, endNs]}, submits all {@code 50} {@code
 *       session.readAs(...)} futures without blocking, awaits {@link ApiFutures#allAsList}, stops
 *       the pass stopwatch, validates {@code 104,857,600} bytes, and closes the session outside
 *       {@code [startNs, endNs]}.
 *   <li><b>Under {@code JSON}:</b> Dispatches the {@code 50} unidirectional {@link ReadChannel}
 *       range reads ({@code seek(i * 4 MiB)} + {@code limit(i * 4 MiB + 2 MiB)} + {@code
 *       setChunkSize(0)}) concurrently onto a pre-warmed fixed {@code 16}-thread pool backed by
 *       {@link ClientProvider#clientForWorker(int)} and awaits all {@code 50} {@link Future}s.
 * </ul>
 */
public final class Wl3ReadConcurrentWorkload implements Workload {

  public static final String WORKLOAD_ID = "WL3";
  public static final String DISPLAY_NAME_GRPC =
      "Read50x2x4 Concurrent (50 \u00d7 2 MiB Ranges on 1 BlobReadSession)";
  public static final String DISPLAY_NAME_JSON =
      "Read50x2x4 Concurrent (50 \u00d7 2 MiB Ranges Across 16-Thread ReadChannel Pool)";
  public static final String OBJECT_NAME = "bench-open-200mb.bin";

  public static final int DEFAULT_RANGE_COUNT = 50;
  public static final int DEFAULT_RANGE_BYTES = 2 * 1024 * 1024;
  public static final long DEFAULT_STRIDE_BYTES = 4L * 1024L * 1024L;
  public static final long EXPECTED_DEFAULT_PASS_BYTES =
      DEFAULT_RANGE_COUNT * (long) DEFAULT_RANGE_BYTES;
  public static final int DEFAULT_JSON_POOL_THREADS = 16;

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
  private final int jsonPoolThreads;
  private final long expectedPassBytes;

  public Wl3ReadConcurrentWorkload() {
    this(DEFAULT_WARMUP_PASSES, DEFAULT_MIN_PASSES, DEFAULT_MIN_DURATION_SEC);
  }

  public Wl3ReadConcurrentWorkload(int warmupPasses, int minPasses, long minDurationSec) {
    this(
        warmupPasses,
        minPasses,
        minDurationSec,
        DEFAULT_RANGE_COUNT,
        DEFAULT_RANGE_BYTES,
        DEFAULT_STRIDE_BYTES,
        DEFAULT_JSON_POOL_THREADS);
  }

  public Wl3ReadConcurrentWorkload(
      int warmupPasses,
      int minPasses,
      long minDurationSec,
      int rangeCount,
      int rangeBytes,
      long strideBytes,
      int jsonPoolThreads) {
    if (warmupPasses < 0
        || minPasses <= 0
        || minDurationSec < 0
        || rangeCount <= 0
        || rangeBytes <= 0
        || strideBytes <= 0
        || jsonPoolThreads <= 0) {
      throw new IllegalArgumentException("Invalid WL3 parameters");
    }
    this.warmupPasses = warmupPasses;
    this.minPasses = minPasses;
    this.minDurationNs = TimeUnit.SECONDS.toNanos(minDurationSec);
    this.rangeCount = rangeCount;
    this.rangeBytes = rangeBytes;
    this.strideBytes = strideBytes;
    this.jsonPoolThreads = jsonPoolThreads;
    this.expectedPassBytes = (long) rangeCount * rangeBytes;
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
          "WL3 is N/A for protocol=" + config.protocol() + ", bucketType=" + config.bucketType());
    }

    BlobId blobId = BlobId.of(config.bucketName(), OBJECT_NAME);
    boolean useGrpc = (config.protocol() == UdsTransport.GRPC);
    String effectiveDisplayName = useGrpc ? DISPLAY_NAME_GRPC : DISPLAY_NAME_JSON;

    ThreadPoolExecutor jsonExecutor = null;
    try {
      if (!useGrpc) {
        jsonExecutor = createPrewarmedJsonPool(jsonPoolThreads, rangeBytes);
      }

      // 1. Untimed warmup phase (1 full 100 MiB concurrent pass by default)
      for (int w = 0; w < warmupPasses; w++) {
        if (useGrpc) {
          runSingleGrpcConcurrentPass(clients.primaryClient(), blobId);
        } else {
          runSingleJsonConcurrentPass(clients, blobId, jsonExecutor);
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
                ? runSingleGrpcConcurrentPass(clients.primaryClient(), blobId)
                : runSingleJsonConcurrentPass(clients, blobId, jsonExecutor);
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
          effectiveDisplayName,
          wlStartUtc,
          wlEndUtc,
          Arrays.copyOf(latenciesNs, count),
          totalTimedBytes,
          WorkloadResult.DiagnosticDelta.between(beforeSnap, afterSnap));
    } finally {
      if (jsonExecutor != null) {
        jsonExecutor.shutdownNow();
        jsonExecutor.awaitTermination(5, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Opens 1 {@link BlobReadSession} outside {@code [startNs, endNs]}, submits all 50 range futures
   * concurrently without blocking, awaits {@link ApiFutures#allAsList}, stops {@code startNs}, and
   * validates {@code 104,857,600} bytes before closing the session outside {@code [startNs,
   * endNs]}.
   */
  private long runSingleGrpcConcurrentPass(Storage storage, BlobId blobId) throws Exception {
    try (BlobReadSession session =
        storage.blobReadSession(blobId).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS)) {
      List<ApiFuture<byte[]>> futures = new ArrayList<ApiFuture<byte[]>>(rangeCount);

      long startNs = System.nanoTime();
      for (int i = 0; i < rangeCount; i++) {
        long offset = i * strideBytes;
        futures.add(
            session.readAs(
                ReadProjectionConfigs.asFutureBytes()
                    .withRangeSpec(RangeSpec.of(offset, rangeBytes))));
      }
      List<byte[]> results = ApiFutures.allAsList(futures).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
      long elapsedNs = System.nanoTime() - startNs;

      if (results.size() != rangeCount) {
        throw new IllegalStateException(
            "WL3 gRPC pass expected " + rangeCount + " results, got " + results.size());
      }
      long passBytes = 0L;
      for (int i = 0; i < results.size(); i++) {
        byte[] data = results.get(i);
        if (data == null || data.length != rangeBytes) {
          throw new IllegalStateException(
              "WL3 gRPC range "
                  + i
                  + " expected "
                  + rangeBytes
                  + " bytes, got "
                  + (data == null ? -1 : data.length));
        }
        passBytes += data.length;
      }
      if (passBytes != expectedPassBytes) {
        throw new IllegalStateException(
            "WL3 gRPC pass expected " + expectedPassBytes + " bytes, got " + passBytes);
      }
      return elapsedNs;
    }
  }

  /**
   * Dispatches all 50 unidirectional {@link ReadChannel} range reads onto the 16-thread {@link
   * ThreadPoolExecutor} and awaits completion of all 50 {@link Future}s.
   */
  private long runSingleJsonConcurrentPass(
      final ClientProvider clients, final BlobId blobId, ThreadPoolExecutor executor)
      throws Exception {
    List<Future<Long>> futures = new ArrayList<Future<Long>>(rangeCount);
    boolean ok = false;

    long startNs = System.nanoTime();
    try {
      for (int i = 0; i < rangeCount; i++) {
        final int rangeIdx = i;
        final long offset = i * strideBytes;
        futures.add(
            executor.submit(
                new Callable<Long>() {
                  @Override
                  public Long call() throws Exception {
                    JsonWorkerThread worker = (JsonWorkerThread) Thread.currentThread();
                    Storage client = clients.clientForWorker(worker.workerId);
                    try (ReadChannel r =
                        client.reader(
                            blobId, Storage.BlobSourceOption.shouldReturnRawInputStream(true))) {
                      r.setChunkSize(0);
                      r.seek(offset);
                      r.limit(offset + rangeBytes);
                      long rangeRead = drainChannel(r, worker.reusableBuffer);
                      if (rangeRead != rangeBytes) {
                        throw new IllegalStateException(
                            "WL3 JSON range "
                                + rangeIdx
                                + " at offset "
                                + offset
                                + " expected "
                                + rangeBytes
                                + " bytes, got "
                                + rangeRead);
                      }
                      return rangeRead;
                    }
                  }
                }));
      }

      long passBytes = 0L;
      for (int i = 0; i < futures.size(); i++) {
        passBytes += futures.get(i).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
      }
      long elapsedNs = System.nanoTime() - startNs;

      if (passBytes != expectedPassBytes) {
        throw new IllegalStateException(
            "WL3 JSON pass expected " + expectedPassBytes + " bytes, got " + passBytes);
      }
      ok = true;
      return elapsedNs;
    } finally {
      if (!ok) {
        for (int i = 0; i < futures.size(); i++) {
          futures.get(i).cancel(true);
        }
      }
    }
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

  private static ThreadPoolExecutor createPrewarmedJsonPool(int poolSize, final int bufferBytes) {
    final AtomicInteger nextWorkerId = new AtomicInteger(0);
    ThreadFactory factory =
        new ThreadFactory() {
          @Override
          public Thread newThread(Runnable r) {
            int id = nextWorkerId.getAndIncrement();
            JsonWorkerThread t = new JsonWorkerThread(r, id, bufferBytes);
            t.setDaemon(true);
            return t;
          }
        };
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(
            poolSize,
            poolSize,
            0L,
            TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(),
            factory);
    pool.prestartAllCoreThreads();
    return pool;
  }

  private static final class JsonWorkerThread extends Thread {
    final int workerId;
    final ByteBuffer reusableBuffer;

    JsonWorkerThread(Runnable target, int workerId, int bufferBytes) {
      super(target, "wl3-json-worker-" + workerId);
      this.workerId = workerId;
      this.reusableBuffer = ByteBuffer.allocate(bufferBytes);
    }
  }
}
