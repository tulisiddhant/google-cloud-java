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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Workload 5 ({@code WL5}): {@code Scale Read} — Read {@code 16 MiB} Chunks of {@code 200 MiB}
 * Objects on {@code 48} Parallel Threads for {@code 5} Minutes ({@code 300s}).
 *
 * <p>Worker {@code w} ({@code 0..47}) binds to {@code bench-200mb-<w>.bin} ({@code 209,715,200} B)
 * and {@link ClientProvider#clientForWorker(int)}, stepping sequentially across the {@code 12} full
 * {@code 16 MiB} ({@code 16,777,216} B) chunks ({@code offset = (chunkIdx % 12) * 16 MiB}). After a
 * barrier-synchronized {@code 15s} untimed warmup across all {@code 48} threads, all workers run
 * for a {@code 300s} timed steady-state window where {@code activeSec = (wlEndNs - wlStartNs) /
 * 1e9}.
 */
public final class Wl5ScaleReadWorkload implements Workload {

  public static final String WORKLOAD_ID = "WL5";
  public static final String DISPLAY_NAME_GRPC =
      "Scale Read (16 MiB Chunks of 200 MiB \u00d7 48 Threads, 5 Minutes)";
  public static final String DISPLAY_NAME_JSON =
      "Scale Read (16 MiB Chunks of 200 MiB \u00d7 48 Threads, 5 Minutes via ReadChannel)";

  public static final int DEFAULT_WORKER_THREADS = 48;
  public static final int DEFAULT_CHUNK_BYTES = 16 * 1024 * 1024;
  public static final int DEFAULT_CHUNKS_PER_OBJECT = 12;
  public static final long DEFAULT_WARMUP_SEC = 15L;
  public static final long DEFAULT_DURATION_SEC = 300L;

  private static final int MAX_JSON_DRAIN_BUFFER_BYTES = 2 * 1024 * 1024;
  private static final long OP_TIMEOUT_SEC = 180L;

  private final int workerThreads;
  private final int chunkBytes;
  private final int chunksPerObject;
  private final long warmupNs;
  private final long durationNs;
  private final int minOpsPerWorker;

  public Wl5ScaleReadWorkload() {
    this(
        DEFAULT_WORKER_THREADS,
        DEFAULT_CHUNK_BYTES,
        DEFAULT_CHUNKS_PER_OBJECT,
        DEFAULT_WARMUP_SEC,
        DEFAULT_DURATION_SEC,
        0);
  }

  public Wl5ScaleReadWorkload(
      int workerThreads,
      int chunkBytes,
      int chunksPerObject,
      long warmupSec,
      long durationSec,
      int minOpsPerWorker) {
    if (workerThreads <= 0
        || chunkBytes <= 0
        || chunksPerObject <= 0
        || warmupSec < 0
        || durationSec < 0
        || minOpsPerWorker < 0) {
      throw new IllegalArgumentException("Invalid WL5 parameters");
    }
    if (durationSec == 0L && minOpsPerWorker == 0) {
      throw new IllegalArgumentException("Either durationSec > 0 or minOpsPerWorker > 0 required");
    }
    this.workerThreads = workerThreads;
    this.chunkBytes = chunkBytes;
    this.chunksPerObject = chunksPerObject;
    this.warmupNs = TimeUnit.SECONDS.toNanos(warmupSec);
    this.durationNs = TimeUnit.SECONDS.toNanos(durationSec);
    this.minOpsPerWorker = minOpsPerWorker;
  }

  public static String objectNameForWorker(int workerIndex) {
    return "bench-200mb-" + workerIndex + ".bin";
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
  public WorkloadResult execute(final ClientProvider clients, final BenchmarkConfig config)
      throws Exception {
    if (!isApplicable(config.protocol(), config.bucketType())) {
      throw new IllegalArgumentException(
          "WL5 is N/A for protocol=" + config.protocol() + ", bucketType=" + config.bucketType());
    }

    final boolean useGrpc = (config.protocol() == UdsTransport.GRPC);
    final String effectiveDisplayName = useGrpc ? DISPLAY_NAME_GRPC : DISPLAY_NAME_JSON;

    final CountDownLatch setupReadyLatch = new CountDownLatch(workerThreads);
    final CountDownLatch startWarmupLatch = new CountDownLatch(1);
    final CountDownLatch warmupDoneLatch = new CountDownLatch(workerThreads);
    final CountDownLatch startTimedLatch = new CountDownLatch(1);
    final CountDownLatch timedDoneLatch = new CountDownLatch(workerThreads);
    final CountDownLatch allowTeardownLatch = new CountDownLatch(1);

    final AtomicBoolean abort = new AtomicBoolean(false);
    final AtomicReference<Throwable> firstError = new AtomicReference<Throwable>(null);
    final PhaseDeadlines deadlines = new PhaseDeadlines();

    List<WorkerState> states = new ArrayList<WorkerState>(workerThreads);
    List<Thread> threads = new ArrayList<Thread>(workerThreads);

    for (int w = 0; w < workerThreads; w++) {
      final int workerId = w;
      final WorkerState state = new WorkerState();
      states.add(state);
      Thread t =
          new Thread(
              new Runnable() {
                @Override
                public void run() {
                  runWorker(
                      workerId,
                      clients,
                      config.bucketName(),
                      useGrpc,
                      state,
                      deadlines,
                      setupReadyLatch,
                      startWarmupLatch,
                      warmupDoneLatch,
                      startTimedLatch,
                      timedDoneLatch,
                      allowTeardownLatch,
                      abort,
                      firstError);
                }
              },
              "wl5-scale-read-" + workerId);
      t.setDaemon(true);
      threads.add(t);
      t.start();
    }

    try {
      setupReadyLatch.await();
      rethrowIfFailed(firstError);

      // Phase 1: Untimed 15s warmup across all 48 threads
      deadlines.warmupDeadlineNs = System.nanoTime() + warmupNs;
      startWarmupLatch.countDown();
      warmupDoneLatch.await();
      rethrowIfFailed(firstError);

      // Phase 2: Barrier-aligned 300s timed steady-state window
      WorkloadResult.ProcessSnapshot beforeSnap =
          WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), true);
      Instant wlStartUtc = Instant.now();
      long wlStartNs = System.nanoTime();
      deadlines.timedDeadlineNs = wlStartNs + durationNs;
      startTimedLatch.countDown();

      timedDoneLatch.await();
      long wlEndNs = System.nanoTime();
      Instant wlEndUtc = Instant.now();
      WorkloadResult.ProcessSnapshot afterSnap =
          WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), false);

      allowTeardownLatch.countDown();
      for (int i = 0; i < threads.size(); i++) {
        threads.get(i).join(TimeUnit.SECONDS.toMillis(60));
      }
      rethrowIfFailed(firstError);

      double wallActiveSec = (wlEndNs - wlStartNs) / WorkloadResult.NANOS_PER_SEC;
      long[] mergedLatenciesNs = mergeWorkerSamples(states);
      long totalTimedBytes = (long) mergedLatenciesNs.length * chunkBytes;

      return WorkloadResult.fromParallelScaleSamples(
          WORKLOAD_ID,
          effectiveDisplayName,
          wlStartUtc,
          wlEndUtc,
          mergedLatenciesNs,
          totalTimedBytes,
          wallActiveSec,
          WorkloadResult.DiagnosticDelta.between(beforeSnap, afterSnap));
    } catch (Throwable t) {
      abort.set(true);
      startWarmupLatch.countDown();
      startTimedLatch.countDown();
      allowTeardownLatch.countDown();
      for (int i = 0; i < threads.size(); i++) {
        threads.get(i).interrupt();
      }
      for (int i = 0; i < threads.size(); i++) {
        threads.get(i).join(TimeUnit.SECONDS.toMillis(10));
      }
      if (t instanceof Exception) {
        throw (Exception) t;
      }
      throw new RuntimeException(t);
    }
  }

  private void runWorker(
      int workerId,
      ClientProvider clients,
      String bucketName,
      boolean useGrpc,
      WorkerState state,
      PhaseDeadlines deadlines,
      CountDownLatch setupReadyLatch,
      CountDownLatch startWarmupLatch,
      CountDownLatch warmupDoneLatch,
      CountDownLatch startTimedLatch,
      CountDownLatch timedDoneLatch,
      CountDownLatch allowTeardownLatch,
      AtomicBoolean abort,
      AtomicReference<Throwable> firstError) {
    Storage client = clients.clientForWorker(workerId);
    BlobId blobId = BlobId.of(bucketName, objectNameForWorker(workerId));
    BlobReadSession session = null;
    ByteBuffer jsonBuffer =
        useGrpc ? null : ByteBuffer.allocate(Math.min(chunkBytes, MAX_JSON_DRAIN_BUFFER_BYTES));

    boolean setupCountedDown = false;
    boolean warmupCountedDown = false;
    boolean timedCountedDown = false;

    try {
      if (useGrpc) {
        session = client.blobReadSession(blobId).get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
      }
      setupReadyLatch.countDown();
      setupCountedDown = true;

      startWarmupLatch.await();
      long chunkIdx = 0L;
      while (!abort.get() && System.nanoTime() < deadlines.warmupDeadlineNs) {
        long offset = (chunkIdx % chunksPerObject) * (long) chunkBytes;
        chunkIdx++;
        if (useGrpc) {
          readSingleGrpcChunk(session, offset);
        } else {
          readSingleJsonChunk(client, blobId, offset, jsonBuffer);
        }
      }
      warmupDoneLatch.countDown();
      warmupCountedDown = true;

      startTimedLatch.await();
      chunkIdx = 0L;
      while (!abort.get()
          && (state.count < minOpsPerWorker || System.nanoTime() < deadlines.timedDeadlineNs)) {
        long offset = (chunkIdx % chunksPerObject) * (long) chunkBytes;
        chunkIdx++;
        long elapsedNs =
            useGrpc
                ? readSingleGrpcChunk(session, offset)
                : readSingleJsonChunk(client, blobId, offset, jsonBuffer);
        state.record(elapsedNs);
      }
      timedDoneLatch.countDown();
      timedCountedDown = true;

      allowTeardownLatch.await();
    } catch (Throwable t) {
      abort.set(true);
      firstError.compareAndSet(null, t);
    } finally {
      if (!setupCountedDown) {
        setupReadyLatch.countDown();
      }
      if (!warmupCountedDown) {
        warmupDoneLatch.countDown();
      }
      if (!timedCountedDown) {
        timedDoneLatch.countDown();
      }
      if (session != null) {
        try {
          session.close();
        } catch (Throwable closeErr) {
          abort.set(true);
          firstError.compareAndSet(null, closeErr);
        }
      }
    }
  }

  private long readSingleGrpcChunk(BlobReadSession session, long offset) throws Exception {
    long startNs = System.nanoTime();
    byte[] data =
        session
            .readAs(
                ReadProjectionConfigs.asFutureBytes()
                    .withRangeSpec(RangeSpec.of(offset, chunkBytes)))
            .get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
    long elapsedNs = System.nanoTime() - startNs;
    if (data == null || data.length != chunkBytes) {
      throw new IllegalStateException(
          "WL5 gRPC chunk at offset "
              + offset
              + " expected "
              + chunkBytes
              + " bytes, got "
              + (data == null ? -1 : data.length));
    }
    return elapsedNs;
  }

  private long readSingleJsonChunk(
      Storage client, BlobId blobId, long offset, ByteBuffer reusableBuffer) throws IOException {
    long startNs = System.nanoTime();
    long bytesRead;
    try (ReadChannel r =
        client.reader(blobId, Storage.BlobSourceOption.shouldReturnRawInputStream(true))) {
      r.setChunkSize(0);
      r.seek(offset);
      r.limit(offset + chunkBytes);
      bytesRead = drainChannel(r, reusableBuffer);
    }
    long elapsedNs = System.nanoTime() - startNs;
    if (bytesRead != chunkBytes) {
      throw new IllegalStateException(
          "WL5 JSON chunk at offset "
              + offset
              + " expected "
              + chunkBytes
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

  private static long[] mergeWorkerSamples(List<WorkerState> states) {
    int total = 0;
    for (int i = 0; i < states.size(); i++) {
      total += states.get(i).count;
    }
    long[] merged = new long[total];
    int pos = 0;
    for (int i = 0; i < states.size(); i++) {
      WorkerState s = states.get(i);
      System.arraycopy(s.samplesNs, 0, merged, pos, s.count);
      pos += s.count;
    }
    return merged;
  }

  private static void rethrowIfFailed(AtomicReference<Throwable> firstError) throws Exception {
    Throwable err = firstError.get();
    if (err != null) {
      if (err instanceof Exception) {
        throw (Exception) err;
      }
      throw new RuntimeException(err);
    }
  }

  private static final class PhaseDeadlines {
    volatile long warmupDeadlineNs;
    volatile long timedDeadlineNs;
  }

  private static final class WorkerState {
    long[] samplesNs = new long[512];
    int count = 0;

    void record(long elapsedNs) {
      if (count == samplesNs.length) {
        samplesNs = Arrays.copyOf(samplesNs, samplesNs.length * 2);
      }
      samplesNs[count++] = elapsedNs;
    }
  }
}
