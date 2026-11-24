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

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageChannelUtils;
import com.google.cloud.storage.sidecar.UdsTransport;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Workload 6 ({@code WL6}): {@code Scale Write} — Write {@code 200 MiB} Objects on {@code 48}
 * Parallel Threads for {@code 5} Minutes ({@code 300s}).
 *
 * <ul>
 *   <li><b>Strict Section 3 Applicability:</b> Runs <b>only</b> on {@code GRPC + ZONAL} ({@code
 *       BidiWriteObject} via {@link Storage#blobAppendableUpload}) and {@code JSON + REGIONAL}
 *       (resumable upload via {@link Storage#writer(BlobInfo, Storage.BlobWriteOption...)}). {@code
 *       GRPC + REGIONAL} and {@code JSON + ZONAL} are {@code N/A}.
 *   <li><b>2-Slot Bounded Rotation:</b> Worker {@code w} ({@code 0..47}) writes to {@code
 *       bench-write-w<w>-slot<seq % 2>.bin} without {@code doesNotExist()}, capping live storage at
 *       {@code <= 96} objects ({@code 19.2 GiB}) so Zonal {@code rapid_zonal_bytes} quota is never
 *       breached.
 *   <li><b>Post-Window Cleanup:</b> Deletes all {@code <= 96} touched slot objects in parallel via
 *       {@link ClientProvider#adminNativeClient()} strictly inside a {@code finally} block
 *       <b>after</b> {@code wlEndUtc} and {@code afterSnap} are recorded.
 * </ul>
 */
public final class Wl6ScaleWriteWorkload implements Workload {

  public static final String WORKLOAD_ID = "WL6";
  public static final String DISPLAY_NAME_GRPC =
      "Scale Write (200 MiB Objects via BidiWriteObject \u00d7 48 Threads, 5 Minutes)";
  public static final String DISPLAY_NAME_JSON =
      "Scale Write (200 MiB Objects via WriteChannel \u00d7 48 Threads, 5 Minutes)";

  public static final int DEFAULT_WORKER_THREADS = 48;
  public static final int DEFAULT_BUFFER_BYTES = 4 * 1024 * 1024;
  public static final int DEFAULT_WRITES_PER_OBJECT = 50;
  public static final long EXPECTED_DEFAULT_OBJECT_BYTES =
      (long) DEFAULT_WRITES_PER_OBJECT * DEFAULT_BUFFER_BYTES;
  public static final long DEFAULT_WARMUP_SEC = 15L;
  public static final long DEFAULT_DURATION_SEC = 300L;

  private static final int CLEANUP_POOL_THREADS = 16;
  private static final long OP_TIMEOUT_SEC = 300L;
  private static final BlobAppendableUploadConfig APPENDABLE_FINALIZE_CONFIG =
      BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING);

  private final int workerThreads;
  private final int bufferBytes;
  private final int writesPerObject;
  private final long expectedObjectBytes;
  private final long warmupNs;
  private final long durationNs;
  private final int minOpsPerWorker;

  public Wl6ScaleWriteWorkload() {
    this(
        DEFAULT_WORKER_THREADS,
        DEFAULT_BUFFER_BYTES,
        DEFAULT_WRITES_PER_OBJECT,
        DEFAULT_WARMUP_SEC,
        DEFAULT_DURATION_SEC,
        0);
  }

  public Wl6ScaleWriteWorkload(
      int workerThreads,
      int bufferBytes,
      int writesPerObject,
      long warmupSec,
      long durationSec,
      int minOpsPerWorker) {
    if (workerThreads <= 0
        || bufferBytes <= 0
        || writesPerObject <= 0
        || warmupSec < 0
        || durationSec < 0
        || minOpsPerWorker < 0) {
      throw new IllegalArgumentException("Invalid WL6 parameters");
    }
    if (durationSec == 0L && minOpsPerWorker == 0) {
      throw new IllegalArgumentException("Either durationSec > 0 or minOpsPerWorker > 0 required");
    }
    this.workerThreads = workerThreads;
    this.bufferBytes = bufferBytes;
    this.writesPerObject = writesPerObject;
    this.expectedObjectBytes = (long) writesPerObject * bufferBytes;
    this.warmupNs = TimeUnit.SECONDS.toNanos(warmupSec);
    this.durationNs = TimeUnit.SECONDS.toNanos(durationSec);
    this.minOpsPerWorker = minOpsPerWorker;
  }

  public static String slotObjectName(int workerIndex, long opSequence) {
    long slot = opSequence & 1L;
    return "bench-write-w" + workerIndex + "-slot" + slot + ".bin";
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
    if (protocol == UdsTransport.GRPC && bucketType == BenchmarkConfig.BucketType.ZONAL) {
      return true;
    }
    if (protocol == UdsTransport.JSON && bucketType == BenchmarkConfig.BucketType.REGIONAL) {
      return true;
    }
    return false;
  }

  @Override
  public WorkloadResult execute(final ClientProvider clients, final BenchmarkConfig config)
      throws Exception {
    if (!isApplicable(config.protocol(), config.bucketType())) {
      throw new IllegalArgumentException(
          "WL6 is N/A for protocol=" + config.protocol() + ", bucketType=" + config.bucketType());
    }

    final boolean useGrpc = (config.protocol() == UdsTransport.GRPC);
    final String effectiveDisplayName = useGrpc ? DISPLAY_NAME_GRPC : DISPLAY_NAME_JSON;

    // Pre-initialize one incompressible 4 MiB byte[] shared read-only across all 48 workers
    final byte[] sharedPayload = new byte[bufferBytes];
    new Random(42L).nextBytes(sharedPayload);

    final Set<String> touchedObjects = ConcurrentHashMap.newKeySet();
    final CountDownLatch setupReadyLatch = new CountDownLatch(workerThreads);
    final CountDownLatch startWarmupLatch = new CountDownLatch(1);
    final CountDownLatch warmupDoneLatch = new CountDownLatch(workerThreads);
    final CountDownLatch startTimedLatch = new CountDownLatch(1);
    final CountDownLatch timedDoneLatch = new CountDownLatch(workerThreads);

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
                      sharedPayload,
                      touchedObjects,
                      state,
                      deadlines,
                      setupReadyLatch,
                      startWarmupLatch,
                      warmupDoneLatch,
                      startTimedLatch,
                      timedDoneLatch,
                      abort,
                      firstError);
                }
              },
              "wl6-scale-write-" + workerId);
      t.setDaemon(true);
      threads.add(t);
      t.start();
    }

    WorkloadResult result = null;
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

      int midTcp443 = -1;
      long halfMs = TimeUnit.NANOSECONDS.toMillis(durationNs / 2);
      if (halfMs >= 1000L) {
        if (!timedDoneLatch.await(halfMs, TimeUnit.MILLISECONDS)) {
          midTcp443 = WorkloadResult.ProcessSnapshot.countEstablishedTcp443(clients.udsClient());
          timedDoneLatch.await();
        }
      } else {
        timedDoneLatch.await();
      }
      long wlEndNs = System.nanoTime();
      Instant wlEndUtc = Instant.now();
      WorkloadResult.ProcessSnapshot afterSnap =
          WorkloadResult.ProcessSnapshot.capture(clients.udsClient(), false);

      for (int i = 0; i < threads.size(); i++) {
        threads.get(i).join(TimeUnit.SECONDS.toMillis(60));
      }
      rethrowIfFailed(firstError);

      double wallActiveSec = (wlEndNs - wlStartNs) / WorkloadResult.NANOS_PER_SEC;
      long[] mergedLatenciesNs = mergeWorkerSamples(states);
      long totalTimedBytes = (long) mergedLatenciesNs.length * expectedObjectBytes;

      result =
          WorkloadResult.fromParallelScaleSamples(
              WORKLOAD_ID,
              effectiveDisplayName,
              wlStartUtc,
              wlEndUtc,
              mergedLatenciesNs,
              totalTimedBytes,
              wallActiveSec,
              WorkloadResult.DiagnosticDelta.between(beforeSnap, afterSnap, midTcp443));
      return result;
    } catch (Throwable t) {
      abort.set(true);
      startWarmupLatch.countDown();
      startTimedLatch.countDown();
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
    } finally {
      // Strictly post-window parallel deletion of <= 96 rotating slot objects via out-of-band
      // Native Storage client (after wlEndUtc and afterSnap have already been captured).
      if (!touchedObjects.isEmpty()) {
        cleanupTouchedSlotObjects(clients, config.bucketName(), touchedObjects, result == null);
      }
    }
  }

  private void runWorker(
      int workerId,
      ClientProvider clients,
      String bucketName,
      boolean useGrpc,
      byte[] sharedPayload,
      Set<String> touchedObjects,
      WorkerState state,
      PhaseDeadlines deadlines,
      CountDownLatch setupReadyLatch,
      CountDownLatch startWarmupLatch,
      CountDownLatch warmupDoneLatch,
      CountDownLatch startTimedLatch,
      CountDownLatch timedDoneLatch,
      AtomicBoolean abort,
      AtomicReference<Throwable> firstError) {
    Storage client = clients.clientForWorker(workerId);
    long opSeq = 0L;
    boolean warmupCountedDown = false;
    boolean timedCountedDown = false;

    setupReadyLatch.countDown();
    try {
      startWarmupLatch.await();
      while (!abort.get() && System.nanoTime() < deadlines.warmupDeadlineNs) {
        String objName = slotObjectName(workerId, opSeq++);
        touchedObjects.add(objName);
        if (useGrpc) {
          uploadSingleGrpcObject(client, bucketName, objName, sharedPayload);
        } else {
          uploadSingleJsonObject(client, bucketName, objName, sharedPayload);
        }
      }
      warmupDoneLatch.countDown();
      warmupCountedDown = true;

      startTimedLatch.await();
      while (!abort.get()
          && (state.count < minOpsPerWorker || System.nanoTime() < deadlines.timedDeadlineNs)) {
        String objName = slotObjectName(workerId, opSeq++);
        touchedObjects.add(objName);
        long elapsedNs =
            useGrpc
                ? uploadSingleGrpcObject(client, bucketName, objName, sharedPayload)
                : uploadSingleJsonObject(client, bucketName, objName, sharedPayload);
        state.record(elapsedNs);
      }
      timedDoneLatch.countDown();
      timedCountedDown = true;
    } catch (Throwable t) {
      abort.set(true);
      firstError.compareAndSet(null, t);
    } finally {
      if (!warmupCountedDown) {
        warmupDoneLatch.countDown();
      }
      if (!timedCountedDown) {
        timedDoneLatch.countDown();
      }
    }
  }

  private long uploadSingleGrpcObject(
      Storage client, String bucketName, String objName, byte[] sharedPayload) throws Exception {
    BlobInfo target = BlobInfo.newBuilder(bucketName, objName).build();
    long startNs = System.nanoTime();
    BlobAppendableUpload upload = client.blobAppendableUpload(target, APPENDABLE_FINALIZE_CONFIG);
    long totalWritten = 0L;
    try (AppendableUploadWriteableByteChannel ch = upload.open()) {
      for (int i = 0; i < writesPerObject; i++) {
        ByteBuffer buf = ByteBuffer.wrap(sharedPayload);
        totalWritten += StorageChannelUtils.blockingEmptyTo(buf, ch);
      }
      ch.finalizeAndClose();
    }
    BlobInfo finalized = upload.getResult().get(OP_TIMEOUT_SEC, TimeUnit.SECONDS);
    long elapsedNs = System.nanoTime() - startNs;
    if (totalWritten != expectedObjectBytes || finalized == null) {
      throw new IllegalStateException(
          "WL6 gRPC upload for "
              + objName
              + " expected "
              + expectedObjectBytes
              + " bytes, wrote "
              + totalWritten);
    }
    return elapsedNs;
  }

  private long uploadSingleJsonObject(
      Storage client, String bucketName, String objName, byte[] sharedPayload) throws IOException {
    BlobInfo target = BlobInfo.newBuilder(bucketName, objName).build();
    long startNs = System.nanoTime();
    long totalWritten = 0L;
    try (WriteChannel wch = client.writer(target)) {
      for (int i = 0; i < writesPerObject; i++) {
        ByteBuffer buf = ByteBuffer.wrap(sharedPayload);
        totalWritten += StorageChannelUtils.blockingEmptyTo(buf, wch);
      }
    }
    long elapsedNs = System.nanoTime() - startNs;
    if (totalWritten != expectedObjectBytes) {
      throw new IllegalStateException(
          "WL6 JSON upload for "
              + objName
              + " expected "
              + expectedObjectBytes
              + " bytes, wrote "
              + totalWritten);
    }
    return elapsedNs;
  }

  private static void cleanupTouchedSlotObjects(
      ClientProvider clients,
      final String bucketName,
      Set<String> touchedObjects,
      boolean suppressCleanupErrors)
      throws Exception {
    final Storage admin = clients.adminNativeClient();
    final AtomicInteger idx = new AtomicInteger(0);
    ExecutorService cleanupPool =
        Executors.newFixedThreadPool(
            Math.min(CLEANUP_POOL_THREADS, touchedObjects.size()),
            new ThreadFactory() {
              @Override
              public Thread newThread(Runnable r) {
                Thread t = new Thread(r, "wl6-slot-cleanup-" + idx.getAndIncrement());
                t.setDaemon(true);
                return t;
              }
            });
    try {
      List<Future<Void>> futures = new ArrayList<Future<Void>>(touchedObjects.size());
      for (final String objectName : touchedObjects) {
        futures.add(
            cleanupPool.submit(
                new Callable<Void>() {
                  @Override
                  public Void call() {
                    admin.delete(BlobId.of(bucketName, objectName));
                    return null;
                  }
                }));
      }
      for (int i = 0; i < futures.size(); i++) {
        try {
          futures.get(i).get(60, TimeUnit.SECONDS);
        } catch (Exception e) {
          if (!suppressCleanupErrors) {
            throw e;
          }
        }
      }
    } finally {
      cleanupPool.shutdownNow();
    }
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
    long[] samplesNs = new long[256];
    int count = 0;

    void record(long elapsedNs) {
      if (count == samplesNs.length) {
        samplesNs = Arrays.copyOf(samplesNs, samplesNs.length * 2);
      }
      samplesNs[count++] = elapsedNs;
    }
  }
}
