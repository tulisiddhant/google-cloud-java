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
import gcscore.v1.CloseObjectRequest;
import gcscore.v1.DeleteObjectRequest;
import gcscore.v1.GcsCoreGrpc;
import gcscore.v1.GetObjectRequest;
import gcscore.v1.ListObjectsRequest;
import gcscore.v1.ListObjectsResponse;
import gcscore.v1.ListedObject;
import gcscore.v1.OpenObjectRequest;
import gcscore.v1.OpenObjectResponse;
import gcscore.v1.ReadChunk;
import gcscore.v1.ReadObjectRequest;
import gcscore.v1.ReadRangeRequest;
import gcscore.v1.Transport;
import gcscore.v1.WriteHeader;
import gcscore.v1.WriteObjectResponse;
import gcscore.v1.WriteRequest;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.time.Duration;
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
 * Raw {@code gcscore.v1.GcsCore} gRPC-over-UDS integration test validating session token lending,
 * {@code WriteObject}, {@code ReadObject}, {@code OpenObject}/{@code ReadRange}/{@code
 * CloseObject}, {@code GetObject}, {@code ListObjects}, and {@code DeleteObject}.
 */
public class ITUdsRawGrpcExperiment {

  private static final boolean IS_LIVE =
      Boolean.parseBoolean(
          System.getProperty("gcs.live.benchmark", System.getProperty("gcs.bench.live", "false")));

  private static MockStorageGrpcServer mockServer;
  private static RustSidecarProcessManager sidecarManager;
  private static ManagedChannel channel;
  private static GcsSessionManager sessionManager;
  private static GcsCoreGrpc.GcsCoreBlockingStub blockingStub;
  private static GcsCoreGrpc.GcsCoreStub asyncStub;
  private static String testBucket;

  private static void log(String message) {
    System.out.println("[UdsExperiment] " + message);
  }

  @BeforeClass
  public static void setUp() throws Exception {
    testBucket = System.getProperty("gcs.test.bucket", System.getenv("GCS_TEST_BUCKET"));
    if (testBucket == null || testBucket.trim().isEmpty()) {
      testBucket = "raw-uds-test-bucket";
    }

    if (!IS_LIVE) {
      mockServer = new MockStorageGrpcServer();
      mockServer.start();
      mockServer.addBucket(testBucket);
      System.setProperty("gcs.sidecar.endpoint", mockServer.getGrpcEndpoint());
      System.setProperty("gcs.sidecar.control.endpoint", mockServer.getGrpcEndpoint());
      System.setProperty("gcs.sidecar.default.transport", "grpc");
    }

    sidecarManager = RustSidecarProcessManager.createDefault();
    sidecarManager.start(Duration.ofSeconds(15));

    channel = UdsChannelFactory.createChannel(sidecarManager.getSocketSpec());
    sessionManager = new GcsSessionManager(GcsCoreGrpc.newBlockingStub(channel));
    sessionManager.initSession();

    Channel authed =
        ClientInterceptors.intercept(channel, sessionManager.createClientInterceptor());
    blockingStub = GcsCoreGrpc.newBlockingStub(authed);
    asyncStub = GcsCoreGrpc.newStub(authed);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (sessionManager != null) {
      sessionManager.close();
    }
    if (channel != null) {
      UdsChannelFactory.shutdownChannel(channel, 3, TimeUnit.SECONDS);
    }
    if (sidecarManager != null) {
      sidecarManager.close();
    }
    if (mockServer != null) {
      mockServer.close();
      System.clearProperty("gcs.sidecar.endpoint");
      System.clearProperty("gcs.sidecar.control.endpoint");
      System.clearProperty("gcs.sidecar.default.transport");
    }
  }

  @Test
  public void testWriteReadBidiAndControlRoundTrip() throws Exception {
    String objectName = "uds-raw-experiment-" + UUID.randomUUID() + ".bin";
    int payloadSize = 64 * 1024;
    byte[] payload = new byte[payloadSize];
    new Random(42).nextBytes(payload);

    CRC32 crc32 = new CRC32();
    crc32.update(payload);
    long expectedCrc = crc32.getValue();

    // 1. Streaming WriteObject over gcscore.v1
    final AtomicReference<WriteObjectResponse> responseRef =
        new AtomicReference<WriteObjectResponse>();
    final AtomicReference<Throwable> errorRef = new AtomicReference<Throwable>();
    final CountDownLatch latch = new CountDownLatch(1);

    StreamObserver<WriteRequest> reqObs =
        asyncStub.writeObject(
            new StreamObserver<WriteObjectResponse>() {
              @Override
              public void onNext(WriteObjectResponse value) {
                responseRef.set(value);
              }

              @Override
              public void onError(Throwable t) {
                errorRef.set(t);
                latch.countDown();
              }

              @Override
              public void onCompleted() {
                latch.countDown();
              }
            });

    reqObs.onNext(
        WriteRequest.newBuilder()
            .setHeader(
                WriteHeader.newBuilder()
                    .setBucket(testBucket)
                    .setObject(objectName)
                    .setContentType("application/octet-stream")
                    .setTransport(Transport.GRPC)
                    .build())
            .build());
    reqObs.onNext(
        WriteRequest.newBuilder().setData(ByteString.copyFrom(payload, 0, 32 * 1024)).build());
    reqObs.onNext(
        WriteRequest.newBuilder()
            .setData(ByteString.copyFrom(payload, 32 * 1024, 32 * 1024))
            .build());
    reqObs.onCompleted();

    assertTrue("WriteObject timed out", latch.await(30, TimeUnit.SECONDS));
    if (errorRef.get() != null) {
      throw new AssertionError("WriteObject failed", errorRef.get());
    }
    WriteObjectResponse writeResp = responseRef.get();
    assertNotNull(writeResp);
    assertEquals(payloadSize, writeResp.getSize());

    // 2. Streaming ReadObject over gcscore.v1
    Iterator<ReadChunk> readIt =
        blockingStub.readObject(
            ReadObjectRequest.newBuilder()
                .setBucket(testBucket)
                .setObject(objectName)
                .setOffset(0)
                .setLength(-1)
                .setTransport(Transport.GRPC)
                .build());
    ByteArrayOutputStream downloaded = new ByteArrayOutputStream();
    while (readIt.hasNext()) {
      ReadChunk c = readIt.next();
      for (ByteString slice : c.getDataList()) {
        slice.writeTo(downloaded);
      }
    }
    byte[] roundTrip = downloaded.toByteArray();
    assertArrayEquals(payload, roundTrip);
    CRC32 actualCrc = new CRC32();
    actualCrc.update(roundTrip);
    assertEquals(expectedCrc, actualCrc.getValue());

    // 3. Persistent BidiReadObject handle (OpenObject + ReadRange + CloseObject)
    OpenObjectResponse opened =
        blockingStub.openObject(
            OpenObjectRequest.newBuilder().setBucket(testBucket).setObject(objectName).build());
    assertTrue(opened.getHandle() > 0);
    assertEquals(payloadSize, opened.getMeta().getSize());

    Iterator<ReadChunk> rangeIt =
        blockingStub.readRange(
            ReadRangeRequest.newBuilder()
                .setHandle(opened.getHandle())
                .setOffset(1024)
                .setLength(4096)
                .build());
    ByteArrayOutputStream rangeOut = new ByteArrayOutputStream();
    while (rangeIt.hasNext()) {
      for (ByteString bs : rangeIt.next().getDataList()) {
        bs.writeTo(rangeOut);
      }
    }
    byte[] expectedSlice = new byte[4096];
    System.arraycopy(payload, 1024, expectedSlice, 0, 4096);
    assertArrayEquals(expectedSlice, rangeOut.toByteArray());
    blockingStub.closeObject(
        CloseObjectRequest.newBuilder().setHandle(opened.getHandle()).build());

    // 4. GetObject + ListObjects + DeleteObject
    ListedObject got =
        blockingStub.getObject(
            GetObjectRequest.newBuilder().setBucket(testBucket).setObject(objectName).build());
    assertEquals(objectName, got.getName());
    assertEquals(payloadSize, got.getSize());

    ListObjectsResponse listed =
        blockingStub.listObjects(
            ListObjectsRequest.newBuilder()
                .setBucket(testBucket)
                .setPrefix(objectName)
                .setPageSize(10)
                .build());
    assertEquals(1, listed.getItemsCount());

    blockingStub.deleteObject(
        DeleteObjectRequest.newBuilder().setBucket(testBucket).setObject(objectName).build());
    log("Verified full gcscore.v1 round-trip over UDS.");
  }
}
