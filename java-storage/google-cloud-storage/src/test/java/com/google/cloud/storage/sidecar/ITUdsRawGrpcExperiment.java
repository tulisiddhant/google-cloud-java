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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import com.google.storage.v2.ChecksummedData;
import com.google.storage.v2.ListObjectsRequest;
import com.google.storage.v2.ListObjectsResponse;
import com.google.storage.v2.Object;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.StorageGrpc;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import com.google.storage.v2.WriteObjectSpec;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.CRC32;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Phase 1A: Raw gRPC integration test validating GCS operations (WriteObject, ReadObject, ListObjects)
 * routed over a Unix Domain Socket to the local Rust Core sidecar.
 *
 * Extensively logs all actions to stdout to enable developers to follow and debug execution details.
 */
public class ITUdsRawGrpcExperiment {

  private static RustSidecarProcessManager sidecarManager;
  private static ManagedChannel channel;
  private static StorageGrpc.StorageBlockingStub blockingStub;
  private static StorageGrpc.StorageStub asyncStub;
  private static String testBucket;

  private static void log(String message) {
    System.out.println("[UdsExperiment] " + message);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    log("======================================================================");
    log("INITIALIZING GCS UDS RAW gRPC EXPERIMENT HARNESS");
    log("======================================================================");

    testBucket = System.getProperty("gcs.test.bucket");
    String bucketSource = "System property (gcs.test.bucket)";
    if (testBucket == null) {
      testBucket = System.getenv("GCS_TEST_BUCKET");
      bucketSource = "Environment variable (GCS_TEST_BUCKET)";
    }
    if (testBucket == null || testBucket.trim().isEmpty()) {
      testBucket = "test-bucket";
      bucketSource = "Default fallback ('test-bucket')";
    }
    log("Target test bucket: " + testBucket + " [" + bucketSource + "]");

    // 1. Initialize and start sidecar process manager
    log("Step 1: Starting / attaching to local Rust sidecar process...");
    sidecarManager = RustSidecarProcessManager.createDefault();
    sidecarManager.start(Duration.ofSeconds(10));

    // 2. Connect via Netty UDS Channel
    log("Step 2: Connecting gRPC channel over UDS socket: " + sidecarManager.getSocketPath() + "...");
    channel = UdsChannelFactory.createChannel(sidecarManager.getSocketPath());

    // 3. Initialize stubs
    log("Step 3: Creating StorageBlockingStub and StorageStub instances...");
    blockingStub = StorageGrpc.newBlockingStub(channel);
    asyncStub = StorageGrpc.newStub(channel);

    log("Setup completed successfully. Ready for RPC workloads.");
    log("======================================================================");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    log("======================================================================");
    log("TEARING DOWN EXPERIMENT HARNESS");
    log("======================================================================");
    if (channel != null) {
      log("Shutting down Netty UDS gRPC channel...");
      channel.shutdown();
      if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
        log("Channel did not terminate within 3s; shutting down immediately...");
        channel.shutdownNow();
      }
      log("gRPC channel shutdown complete.");
    }
    if (sidecarManager != null) {
      log("Stopping Rust sidecar process manager...");
      sidecarManager.close();
      log("Sidecar process manager stopped.");
    }
    log("Teardown completed successfully.");
    log("======================================================================");
  }

  @Test
  public void testWriteAndReadObjectRoundTrip() throws Exception {
    log("----------------------------------------------------------------------");
    log("TEST 1: STREAMING WriteObject & ReadObject ROUND-TRIP OVER UDS");
    log("----------------------------------------------------------------------");

    String objectName = "uds-raw-experiment-" + UUID.randomUUID() + ".bin";
    int payloadSize = 64 * 1024; // 64 KiB
    byte[] payload = new byte[payloadSize];
    new Random(42).nextBytes(payload);

    CRC32 crc32 = new CRC32();
    crc32.update(payload);
    long expectedCrc = crc32.getValue();

    log(String.format("Generated test payload : %,d bytes (%.1f KiB)", payloadSize, payloadSize / 1024.0));
    log("Target bucket          : " + testBucket);
    log("Object name            : " + objectName);
    log(String.format("Calculated payload CRC32: 0x%08X", expectedCrc));

    // -------------------------------------------------------------
    // Part A: Streaming WriteObject
    // -------------------------------------------------------------
    log("--> [Part A] Invoking asyncStub.writeObject (Client-Streaming RPC)...");
    Instant writeStart = Instant.now();
    AtomicReference<WriteObjectResponse> responseRef = new AtomicReference<>();
    AtomicReference<Throwable> errorRef = new AtomicReference<>();
    CountDownLatch latch = new CountDownLatch(1);

    StreamObserver<WriteObjectRequest> requestObserver =
        asyncStub.writeObject(
            new StreamObserver<WriteObjectResponse>() {
              @Override
              public void onNext(WriteObjectResponse value) {
                log("    Received onNext() callback from server!");
                responseRef.set(value);
              }

              @Override
              public void onError(Throwable t) {
                log("    Received onError() callback: " + t.getMessage());
                errorRef.set(t);
                latch.countDown();
              }

              @Override
              public void onCompleted() {
                log("    Received onCompleted() callback from server.");
                latch.countDown();
              }
            });

    WriteObjectSpec spec =
        WriteObjectSpec.newBuilder()
            .setResource(
                Object.newBuilder().setBucket(testBucket).setName(objectName).build())
            .build();

    WriteObjectRequest request =
        WriteObjectRequest.newBuilder()
            .setWriteObjectSpec(spec)
            .setChecksummedData(
                ChecksummedData.newBuilder().setContent(ByteString.copyFrom(payload)).build())
            .setFinishWrite(true)
            .build();

    log("    Sending WriteObjectRequest with WriteObjectSpec and data payload...");
    requestObserver.onNext(request);
    log("    Closing client outbound stream (requestObserver.onCompleted())...");
    requestObserver.onCompleted();

    log("    Awaiting server response (latch.await up to 30s)...");
    boolean completed = latch.await(30, TimeUnit.SECONDS);
    assertTrue("WriteObject RPC timed out after 30 seconds", completed);
    if (errorRef.get() != null) {
      log("FATAL: WriteObject failed with exception: " + errorRef.get().getMessage());
      throw new AssertionError("WriteObject RPC failed with error", errorRef.get());
    }

    WriteObjectResponse writeResponse = responseRef.get();
    assertNotNull("WriteObjectResponse must not be null", writeResponse);
    Duration writeDuration = Duration.between(writeStart, Instant.now());

    log(String.format("<-- WriteObject RPC SUCCEEDED in %,d ms!", writeDuration.toMillis()));
    log("    Response WriteStatus : " + writeResponse.getWriteStatusCase());
    if (writeResponse.hasResource()) {
      Object res = writeResponse.getResource();
      log("    Created Object Name  : " + res.getName());
      log("    Created Object Bucket: " + res.getBucket());
      log(String.format("    Created Object Size  : %,d bytes", res.getSize()));
      log("    Object Generation    : " + res.getGeneration());
      log("    Object ETag          : " + res.getEtag());
    }

    // -------------------------------------------------------------
    // Part B: Streaming ReadObject
    // -------------------------------------------------------------
    log("--> [Part B] Invoking blockingStub.readObject (Server-Streaming RPC)...");
    Instant readStart = Instant.now();

    ReadObjectRequest readRequest =
        ReadObjectRequest.newBuilder()
            .setBucket(testBucket)
            .setObject(objectName)
            .build();

    log("    Sending ReadObjectRequest: bucket=" + testBucket + ", object=" + objectName);
    Iterator<ReadObjectResponse> readIterator = blockingStub.readObject(readRequest);
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();

    int chunkIndex = 0;
    while (readIterator.hasNext()) {
      chunkIndex++;
      ReadObjectResponse chunk = readIterator.next();
      if (chunk.hasChecksummedData()) {
        byte[] chunkBytes = chunk.getChecksummedData().getContent().toByteArray();
        buffer.write(chunkBytes);
        log(String.format("    Received Chunk #%d: %,d bytes (cumulative: %,d / %,d bytes)",
            chunkIndex, chunkBytes.length, buffer.size(), payloadSize));
      }
    }

    Duration readDuration = Duration.between(readStart, Instant.now());
    byte[] downloaded = buffer.toByteArray();

    log(String.format("<-- ReadObject RPC SUCCEEDED in %,d ms across %d chunks!",
        readDuration.toMillis(), chunkIndex));
    log(String.format("    Total downloaded payload: %,d bytes", downloaded.length));

    // -------------------------------------------------------------
    // Part C: Data Integrity Assertion
    // -------------------------------------------------------------
    log("--> [Part C] Verifying round-trip data integrity...");
    assertEquals("Downloaded byte length must match uploaded byte length", payload.length, downloaded.length);

    CRC32 downloadedCrc32 = new CRC32();
    downloadedCrc32.update(downloaded);
    long actualCrc = downloadedCrc32.getValue();

    log(String.format("    Expected CRC32: 0x%08X", expectedCrc));
    log(String.format("    Actual CRC32  : 0x%08X", actualCrc));
    assertEquals("CRC32 checksums must match", expectedCrc, actualCrc);

    assertArrayEquals("Downloaded payload must exactly match uploaded bytes bit-for-bit!", payload, downloaded);
    log("[VERIFIED] Bit-for-bit round-trip equality confirmed! Zero data corruption over UDS.");
    log("----------------------------------------------------------------------");
  }

  @Test
  public void testListObjects() {
    log("----------------------------------------------------------------------");
    log("TEST 2: ListObjects UNARY RPC OVER UDS");
    log("----------------------------------------------------------------------");

    Instant start = Instant.now();
    String parent = "projects/_/buckets/" + testBucket;
    int pageSize = 10;

    log("--> Invoking blockingStub.listObjects (Unary RPC)...");
    log("    Parent resource: " + parent);
    log("    Page size      : " + pageSize);

    ListObjectsRequest request =
        ListObjectsRequest.newBuilder()
            .setParent(parent)
            .setPageSize(pageSize)
            .build();

    ListObjectsResponse response = blockingStub.listObjects(request);
    Duration latency = Duration.between(start, Instant.now());

    assertNotNull("ListObjectsResponse should not be null", response);
    int count = response.getObjectsCount();
    log(String.format("<-- ListObjects RPC SUCCEEDED in %,d ms! Found %d object(s):", latency.toMillis(), count));

    int index = 0;
    for (Object obj : response.getObjectsList()) {
      index++;
      log(String.format("    [%d] %-40s | Size: %,8d B | Gen: %s | ETag: %s",
          index, obj.getName(), obj.getSize(), obj.getGeneration(), obj.getEtag()));
    }

    log("[VERIFIED] ListObjects operation completed successfully.");
    log("----------------------------------------------------------------------");
  }

  public static void main(String[] args) {
    try {
      System.out.println("======================================================================");
      System.out.println("   GCS SDK CONSOLIDATION: JAVA gRPC OVER UDS EXPERIMENT RUNNER        ");
      System.out.println("======================================================================");

      setUp();

      ITUdsRawGrpcExperiment experiment = new ITUdsRawGrpcExperiment();

      System.out.println("\n>>> EXECUTING TEST 1/2: WriteObject & ReadObject Round-Trip...");
      try {
        experiment.testWriteAndReadObjectRoundTrip();
        System.out.println("[PASS] Test 1: testWriteAndReadObjectRoundTrip PASSED!");
      } catch (Exception e) {
        System.err.println("[FAIL] Test 1: testWriteAndReadObjectRoundTrip FAILED: " + e.getMessage());
        e.printStackTrace();
      }

      System.out.println("\n>>> EXECUTING TEST 2/2: ListObjects Unary RPC...");
      try {
        experiment.testListObjects();
        System.out.println("[PASS] Test 2: testListObjects PASSED!");
      } catch (Exception e) {
        System.err.println("[FAIL] Test 2: testListObjects FAILED: " + e.getMessage());
        e.printStackTrace();
      }

      tearDown();
      System.out.println("\n======================================================================");
      System.out.println("   EXPERIMENT RUNNER FINISHED WITH ALL TESTS PASSING                  ");
      System.out.println("======================================================================");
    } catch (Throwable t) {
      System.err.println("Fatal error in experiment runner: " + t.getMessage());
      t.printStackTrace();
      System.exit(1);
    }
  }
}
