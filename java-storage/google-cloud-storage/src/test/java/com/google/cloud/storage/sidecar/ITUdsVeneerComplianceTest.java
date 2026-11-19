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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.api.core.ApiFuture;
import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobAppendableUpload;
import com.google.cloud.storage.BlobAppendableUpload.AppendableUploadWriteableByteChannel;
import com.google.cloud.storage.BlobAppendableUploadConfig;
import com.google.cloud.storage.BlobAppendableUploadConfig.CloseAction;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.BlobReadSession;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.RangeSpec;
import com.google.cloud.storage.ReadProjectionConfigs;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.UdsVeneerBridge;
import com.google.cloud.storage.sidecar.UdsStorage.DriverMode;
import com.google.common.hash.Hashing;
import gcscore.v1.Transport;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.channels.ScatteringByteChannel;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * End-to-end Veneer API & constraints compliance test suite verifying all 6 required {@link
 * Storage} methods, option unwrapping, multi-page iteration, {@code NOT_FOUND} error propagation
 * (no dummy data), early {@link ReadChannel#close()} stream cancellation, {@link
 * BlobAppendableUpload} flush/unfinalized-close/takeover/CRC32C finalize, and background {@link
 * GcsSessionManager} OAuth2 token rotation.
 */
public class ITUdsVeneerComplianceTest {

  private static final String BUCKET = "compliance-test-bucket";
  private static final String DRIVER =
      System.getProperty("gcs.benchmark.driver", System.getProperty("gcs.driver", "uds"));

  private static MockStorageGrpcServer mockServer;
  private static Storage grpcUdsStorage;
  private static Storage jsonUdsStorage;

  @BeforeClass
  public static void setUp() throws Exception {
    mockServer = new MockStorageGrpcServer();
    mockServer.start();
    mockServer.addBucket(BUCKET);
    for (int i = 0; i < 12; i++) {
      mockServer.addBucket(String.format("proj-bkt-%02d", i));
    }

    DriverMode mode =
        "uds-inproc".equalsIgnoreCase(DRIVER) ? DriverMode.INPROC : DriverMode.SIDECAR;

    // 1. gRPC-mode UDS client (endpoint = gRPC v2 mock)
    System.setProperty("gcs.sidecar.endpoint", mockServer.getGrpcEndpoint());
    System.setProperty("gcs.sidecar.control.endpoint", mockServer.getGrpcEndpoint());
    System.setProperty("gcs.sidecar.default.transport", "grpc");
    StorageOptions grpcOpts =
        StorageOptions.grpc()
            .setProjectId("compliance-proj")
            .setCredentials(NoCredentials.getInstance())
            .build();
    grpcUdsStorage =
        UdsStorage.newBuilder(grpcOpts).setDriverMode(mode).setTransport(Transport.GRPC).build();

    // 2. JSON-mode UDS client (data endpoint = HTTP/1.1 JSON mock, control = gRPC v2 mock)
    String jsonSocket = "/tmp/gcs-core-json-compliance-" + System.nanoTime() + ".sock";
    System.setProperty("gcs.sidecar.socket.path", jsonSocket);
    System.setProperty("gcs.sidecar.endpoint", mockServer.getHttpEndpoint());
    System.setProperty("gcs.sidecar.control.endpoint", mockServer.getGrpcEndpoint());
    System.setProperty("gcs.sidecar.default.transport", "json");
    StorageOptions httpOpts =
        StorageOptions.http()
            .setProjectId("compliance-proj")
            .setCredentials(NoCredentials.getInstance())
            .build();
    jsonUdsStorage =
        UdsStorage.newBuilder(httpOpts)
            .setDriverMode(mode)
            .setSocketPath(null) // allow owned server startup on jsonSocket
            .setTransport(Transport.JSON)
            .build();
    System.clearProperty("gcs.sidecar.socket.path");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    if (grpcUdsStorage != null) {
      grpcUdsStorage.close();
    }
    if (jsonUdsStorage != null) {
      jsonUdsStorage.close();
    }
    if (mockServer != null) {
      mockServer.close();
    }
    System.clearProperty("gcs.sidecar.endpoint");
    System.clearProperty("gcs.sidecar.control.endpoint");
    System.clearProperty("gcs.sidecar.default.transport");
  }

  @Test
  public void testBlobAppendableUploadFlushTakeoverAndCrc32cFinalize() throws Exception {
    assertNotNull(grpcUdsStorage.getOptions());
    byte[] part1 = "Hello-Rapid-Appendable-".getBytes(StandardCharsets.UTF_8);
    byte[] part2 = "Takeover-Finalized!".getBytes(StandardCharsets.UTF_8);
    byte[] combined = new byte[part1.length + part2.length];
    System.arraycopy(part1, 0, combined, 0, part1.length);
    System.arraycopy(part2, 0, combined, part1.length, part2.length);
    String expectedCrcB64 =
        UdsVeneerBridge.encodeCrc32cBase64(Hashing.crc32c().hashBytes(combined).asInt());

    BlobInfo initialInfo =
        BlobInfo.newBuilder(BUCKET, "appendable-takeover.bin")
            .setContentType("text/plain")
            .build();

    // Session 1: write part1, flush to GCS, closeWithoutFinalizing
    BlobAppendableUpload upload1 =
        grpcUdsStorage.blobAppendableUpload(
            initialInfo,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.CLOSE_WITHOUT_FINALIZING),
            BlobWriteOption.doesNotExist());
    try (AppendableUploadWriteableByteChannel ch1 = upload1.open()) {
      try {
        upload1.open();
        fail("Duplicate open() must throw IllegalStateException");
      } catch (IllegalStateException expected) {
      }
      ByteBuffer b1 = ByteBuffer.wrap(part1);
      while (b1.hasRemaining()) {
        ch1.write(b1);
      }
      ch1.flush();
      ch1.closeWithoutFinalizing();
    }
    BlobInfo unfinalized = upload1.getResult().get(15, TimeUnit.SECONDS);
    assertEquals(part1.length, unfinalized.getSize().longValue());
    assertNotNull(unfinalized.getGeneration());

    // Session 2: takeover by generation, append part2, finalizeAndClose with CRC32C check
    BlobInfo takeoverInfo =
        BlobInfo.newBuilder(BUCKET, "appendable-takeover.bin", unfinalized.getGeneration())
            .build();
    BlobAppendableUpload upload2 =
        grpcUdsStorage.blobAppendableUpload(
            takeoverInfo,
            BlobAppendableUploadConfig.of().withCloseAction(CloseAction.FINALIZE_WHEN_CLOSING));
    try (AppendableUploadWriteableByteChannel ch2 = upload2.open()) {
      ByteBuffer b2 = ByteBuffer.wrap(part2);
      while (b2.hasRemaining()) {
        ch2.write(b2);
      }
      ch2.finalizeAndClose(expectedCrcB64);
    }
    BlobInfo finalized = upload2.getResult().get(15, TimeUnit.SECONDS);
    assertEquals(combined.length, finalized.getSize().longValue());
    assertEquals(expectedCrcB64, finalized.getCrc32c());

    assertArrayEquals(
        combined, grpcUdsStorage.readAllBytes(BUCKET, "appendable-takeover.bin"));
  }

  @Test
  public void testBlobReadSessionAsyncFutureBytesAndScatteringChannel() throws Exception {
    byte[] payload = new byte[512 * 1024];
    new Random(77).nextBytes(payload);
    mockServer.putBytes(BUCKET, "bidi-session-obj.bin", payload, "application/octet-stream");

    try (BlobReadSession session =
        grpcUdsStorage
            .blobReadSession(BlobId.of(BUCKET, "bidi-session-obj.bin"))
            .get(15, TimeUnit.SECONDS)) {
      assertEquals(payload.length, session.getBlobInfo().getSize().longValue());
      assertNotNull(session.getBlobInfo().getCrc32c());

      // 1. ReadAsFutureBytes projection
      ApiFuture<byte[]> fut =
          session.readAs(
              ReadProjectionConfigs.asFutureBytes().withRangeSpec(RangeSpec.of(4096, 16384)));
      byte[] slice1 = fut.get(15, TimeUnit.SECONDS);
      byte[] expected1 = new byte[16384];
      System.arraycopy(payload, 4096, expected1, 0, 16384);
      assertArrayEquals(expected1, slice1);

      // 2. ReadAsChannel (ScatteringByteChannel) projection
      byte[] slice2 = new byte[8192];
      ByteBuffer dst = ByteBuffer.wrap(slice2);
      try (ScatteringByteChannel ch =
          session.readAs(
              ReadProjectionConfigs.asChannel().withRangeSpec(RangeSpec.of(32768, 8192)))) {
        while (dst.hasRemaining()) {
          long n = ch.read(new ByteBuffer[] {dst});
          if (n == -1L) {
            break;
          }
          if (n == 0L) {
            Thread.sleep(2);
          }
        }
      }
      byte[] expected2 = new byte[8192];
      System.arraycopy(payload, 32768, expected2, 0, 8192);
      assertArrayEquals(expected2, slice2);
    }

    // Missing object on blobReadSession must fail with StorageException(404), never fake data
    try {
      grpcUdsStorage
          .blobReadSession(BlobId.of(BUCKET, "does-not-exist.bin"))
          .get(15, TimeUnit.SECONDS);
      fail("Expected ExecutionException for missing object");
    } catch (ExecutionException expected) {
      assertTrue(expected.getCause() instanceof StorageException);
      assertEquals(404, ((StorageException) expected.getCause()).getCode());
    }
  }

  @Test
  public void testJsonReadWritePreconditionsAndEarlyCloseStreamCancellation() throws Exception {
    byte[] data = new byte[384 * 1024];
    new Random(19).nextBytes(data);
    BlobInfo info = BlobInfo.newBuilder(BUCKET, "json-writer-obj.bin").build();

    try (WriteChannel w = jsonUdsStorage.writer(info, BlobWriteOption.doesNotExist())) {
      ByteBuffer src = ByteBuffer.wrap(data);
      while (src.hasRemaining()) {
        w.write(src);
      }
    }

    // Duplicate write with doesNotExist() must fail with 412 Precondition Failed
    try {
      try (WriteChannel w2 = jsonUdsStorage.writer(info, BlobWriteOption.doesNotExist())) {
        w2.write(ByteBuffer.wrap(new byte[] {1, 2, 3}));
      }
      fail("Expected IOException from failed doesNotExist precondition");
    } catch (IOException expected) {
      assertTrue(expected.getCause() instanceof StorageException);
      assertEquals(412, ((StorageException) expected.getCause()).getCode());
    }

    // Read range via seek + limit over JSON
    byte[] slice = new byte[65536];
    ByteBuffer dst = ByteBuffer.wrap(slice);
    try (ReadChannel r = jsonUdsStorage.reader(BUCKET, "json-writer-obj.bin")) {
      r.seek(16384);
      r.limit(16384 + 65536);
      while (dst.hasRemaining()) {
        if (r.read(dst) == -1) {
          break;
        }
      }
      assertEquals(-1, r.read(ByteBuffer.allocate(16)));
    }
    byte[] expected = new byte[65536];
    System.arraycopy(data, 16384, expected, 0, 65536);
    assertArrayEquals(expected, slice);

    // Early close() on a large 64 MiB virtual object must cancel the stream without hanging/leaking
    mockServer.putVirtualObject(BUCKET, "large-64mb-cancel.bin", 64L * 1024 * 1024);
    try (ReadChannel r = grpcUdsStorage.reader(BUCKET, "large-64mb-cancel.bin")) {
      ByteBuffer small = ByteBuffer.allocate(64 * 1024);
      while (small.hasRemaining()) {
        if (r.read(small) == -1) {
          break;
        }
      }
      assertEquals(64 * 1024, small.position());
    }
  }

  @Test
  public void testListPaginationOptionsDeleteAndRealErrorPropagation() throws Exception {
    for (int i = 0; i < 15; i++) {
      mockServer.putBytes(
          BUCKET,
          String.format("paged-pfx/sub-%d/obj-%02d.txt", i % 3, i),
          ("v" + i).getBytes(StandardCharsets.UTF_8),
          "text/plain");
    }

    // Multi-page iterateAll() with prefix + pageSize=4
    Page<Blob> firstPage =
        grpcUdsStorage.list(
            BUCKET, BlobListOption.prefix("paged-pfx/"), BlobListOption.pageSize(4));
    assertTrue("Expected multiple pages when pageSize=4 for 15 objects", firstPage.hasNextPage());
    int totalIterated = 0;
    for (Blob ignored : firstPage.iterateAll()) {
      totalIterated++;
    }
    assertEquals(15, totalIterated);

    // Delimiter option (`currentDirectory`) returns 3 virtual directory prefixes
    Page<Blob> dirPage =
        grpcUdsStorage.list(
            BUCKET, BlobListOption.prefix("paged-pfx/"), BlobListOption.currentDirectory());
    int dirCount = 0;
    for (Blob b : dirPage.getValues()) {
      assertTrue(b.isDirectory());
      dirCount++;
    }
    assertEquals(3, dirCount);

    // ListBuckets multi-page iteration with prefix + pageSize=5
    Page<Bucket> bktPage =
        grpcUdsStorage.list(BucketListOption.prefix("proj-bkt-"), BucketListOption.pageSize(5));
    assertTrue(bktPage.hasNextPage());
    int bktCount = 0;
    for (Bucket ignored : bktPage.iterateAll()) {
      bktCount++;
    }
    assertEquals(12, bktCount);

    // Listing a non-existent bucket must throw StorageException(404), NEVER return dummy blobs!
    try {
      grpcUdsStorage.list("non-existent-bucket-xyz");
      fail("Expected StorageException(404) on missing bucket");
    } catch (StorageException expected) {
      assertEquals(404, expected.getCode());
    }

    // Real DeleteObject & GetObject contract
    String target = "paged-pfx/sub-0/obj-00.txt";
    assertNotNull(grpcUdsStorage.get(BUCKET, target));
    assertTrue("First delete must return true", grpcUdsStorage.delete(BUCKET, target));
    assertNull("get() after delete must return null", grpcUdsStorage.get(BUCKET, target));
    assertFalse("Second delete on missing object must return false", grpcUdsStorage.delete(BUCKET, target));

    // Precondition mismatch on reader() must throw IOException caused by StorageException(412)
    try (ReadChannel r =
        grpcUdsStorage.reader(
            BUCKET, "paged-pfx/sub-1/obj-01.txt", BlobSourceOption.generationMatch(999999L))) {
      r.read(ByteBuffer.allocate(16));
      fail("Expected precondition failure");
    } catch (IOException expected) {
      assertTrue(expected.getCause() instanceof StorageException);
      assertEquals(412, ((StorageException) expected.getCause()).getCode());
    }
  }

  @Test
  public void testBackgroundOAuth2TokenRotationOverUds() throws Exception {
    UdsStorage handler = (UdsStorage) Proxy.getInvocationHandler(grpcUdsStorage);
    GcsSessionManager sm = handler.getSessionManager();
    assertNotNull(sm);
    int before = sm.getUpdateCount();

    final AtomicInteger refreshCalls = new AtomicInteger(0);
    OAuth2Credentials rotatingCreds =
        new OAuth2Credentials() {
          private static final long serialVersionUID = 1L;

          @Override
          public AccessToken refreshAccessToken() {
            int n = refreshCalls.incrementAndGet();
            Date exp = Date.from(Instant.now().plusSeconds(3600));
            return new AccessToken("rotated-oauth2-token-" + n, exp);
          }
        };

    sm.schedulePeriodicRefresh(rotatingCreds, 80L);
    long deadline = System.currentTimeMillis() + 3000L;
    while (sm.getUpdateCount() < before + 2 && System.currentTimeMillis() < deadline) {
      Thread.sleep(40L);
    }
    assertTrue(
        "Expected background UpdateToken RPCs to execute",
        sm.getUpdateCount() >= before + 2);

    // Subsequent RPCs continue succeeding seamlessly with rotated token
    mockServer.putBytes(BUCKET, "post-rotate.bin", new byte[] {7, 8, 9}, "application/octet-stream");
    assertArrayEquals(new byte[] {7, 8, 9}, grpcUdsStorage.readAllBytes(BUCKET, "post-rotate.bin"));
  }
}
