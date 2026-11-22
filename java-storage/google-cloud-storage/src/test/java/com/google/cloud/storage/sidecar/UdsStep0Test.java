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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.UdsPackageBridge;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkConfig;
import com.google.cloud.storage.sidecar.benchmark.WorkloadResult;
import com.google.cloud.storage.sidecar.proto.StatsResponse;
import com.google.storage.v2.ListBucketsRequest;
import com.google.storage.v2.ListObjectsRequest;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Date;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assume;
import org.junit.Test;

/** Step 0 unit and live sidecar plumbing verification tests. */
public class UdsStep0Test {

  @Test
  public void packageBridgeExtractsOptionsAndConstructsEntities() {
    ListObjectsRequest listObj =
        UdsPackageBridge.extractListObjectsRequest(
            "my-bucket",
            Storage.BlobListOption.prefix("list-bench-100/"),
            Storage.BlobListOption.pageSize(100));
    assertThat(listObj.getParent()).isEqualTo("projects/_/buckets/my-bucket");
    assertThat(listObj.getPrefix()).isEqualTo("list-bench-100/");
    assertThat(listObj.getPageSize()).isEqualTo(100);

    ListBucketsRequest listBkt =
        UdsPackageBridge.extractListBucketsRequest(
            "vaibhavpratap-sdk-test", Storage.BucketListOption.pageSize(100));
    assertThat(listBkt.getParent()).isEqualTo("projects/vaibhavpratap-sdk-test");
    assertThat(listBkt.getPageSize()).isEqualTo(100);

    OptionalLong srcGen =
        UdsPackageBridge.extractSourceIfGenerationMatch(
            BlobId.of("b", "o", 42L), Storage.BlobSourceOption.generationMatch());
    assertThat(srcGen.isPresent()).isTrue();
    assertThat(srcGen.getAsLong()).isEqualTo(42L);

    OptionalLong writeDne =
        UdsPackageBridge.extractWriteIfGenerationMatch(
            BlobInfo.newBuilder("b", "o").build(), Storage.BlobWriteOption.doesNotExist());
    assertThat(writeDne.isPresent()).isTrue();
    assertThat(writeDne.getAsLong()).isEqualTo(0L);

    Storage dummyStorage =
        StorageOptions.http().setProjectId("test-proj").build().getService();
    Blob blob =
        UdsPackageBridge.buildBlob(
            dummyStorage,
            "my-bucket",
            "obj.bin",
            7L,
            123L,
            2L,
            "application/octet-stream",
            "",
            "STANDARD",
            1700000000L,
            0x12345678);
    assertThat(blob.getBucket()).isEqualTo("my-bucket");
    assertThat(blob.getName()).isEqualTo("obj.bin");
    assertThat(blob.getGeneration()).isEqualTo(7L);
    assertThat(blob.getSize()).isEqualTo(123L);
    assertThat(blob.getMetageneration()).isEqualTo(2L);
    assertThat(blob.getCrc32c()).isNotEmpty();

    Bucket bucket =
        UdsPackageBridge.buildBucket(
            dummyStorage, "my-bucket", "US-WEST1", "STANDARD", 5L, 1700000000L);
    assertThat(bucket.getName()).isEqualTo("my-bucket");
    assertThat(bucket.getLocation()).isEqualTo("US-WEST1");
    assertThat(bucket.getMetageneration()).isEqualTo(5L);
  }

  @Test
  public void benchmarkConfigValidatesMatrixAndFlags() {
    BenchmarkConfig cfg =
        BenchmarkConfig.fromArgs(
            new String[] {
              "--driver=uds-sidecar",
              "--protocol=grpc",
              "--bucket-type=zonal",
              "--workloads=WL1,WL2"
            });
    assertThat(cfg.driver()).isEqualTo(BenchmarkConfig.DriverType.UDS_SIDECAR);
    assertThat(cfg.protocol()).isEqualTo(UdsTransport.GRPC);
    assertThat(cfg.bucketType()).isEqualTo(BenchmarkConfig.BucketType.ZONAL);
    assertThat(cfg.bucketName()).isEqualTo(BenchmarkConfig.DEFAULT_ZONAL_BUCKET);
    assertThat(cfg.shouldRunWorkload("WL1")).isTrue();
    assertThat(cfg.shouldRunWorkload("WL6")).isFalse();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            BenchmarkConfig.fromArgs(
                new String[] {"--driver=native", "--protocol=json", "--bucket-type=zonal"}));
  }

  @Test
  public void workloadResultComputesExactPercentilesAndThroughput() {
    long[] samplesNs = new long[100];
    for (int i = 0; i < 100; i++) {
      samplesNs[i] = (100 - i) * 1_000_000L; // 100 ms down to 1 ms (unsorted)
    }
    long totalBytes = 100L * 1_048_576L; // 100 MiB = 104,857,600 B
    Instant t0 = Instant.parse("2026-11-20T00:00:00Z");
    Instant t1 = Instant.parse("2026-11-20T00:02:00Z");

    WorkloadResult seq =
        WorkloadResult.fromSequentialSamples("WL2", "Read50x2x4 Serial", t0, t1, samplesNs, totalBytes, null);
    // ceil(0.50 * 100) - 1 = 49 -> 50 ms; ceil(0.90 * 100) - 1 = 89 -> 90 ms; ceil(0.99 * 100) - 1 = 98 -> 99 ms
    assertThat(seq.p50Ms()).isWithin(1e-6).of(50.0);
    assertThat(seq.p90Ms()).isWithin(1e-6).of(90.0);
    assertThat(seq.p99Ms()).isWithin(1e-6).of(99.0);
    // Sum of 1..100 ms = 5050 ms = 5.05 s
    assertThat(seq.activeSec()).isWithin(1e-6).of(5.05);
    assertThat(seq.binaryMibPerSec()).isWithin(1e-4).of(100.0 / 5.05);
    assertThat(seq.decimalMbPerSec()).isWithin(1e-4).of(104.8576 / 5.05);
  }

  @Test
  public void sidecarLifecyclePingSessionLendingAndStats() throws Exception {
    String sidecarBin = System.getProperty("uds.sidecar.bin", "");
    Assume.assumeFalse("Skipping live sidecar test: -Duds.sidecar.bin not set", sidecarBin.isEmpty());

    final AtomicInteger tokenCounter = new AtomicInteger(0);
    OAuth2Credentials rotatingCreds =
        new OAuth2Credentials() {
          @Override
          public AccessToken refreshAccessToken() throws IOException {
            int n = tokenCounter.incrementAndGet();
            Date expiry = new Date(System.currentTimeMillis() + 3600_000L);
            return new AccessToken("synthetic-lent-token-" + n, expiry);
          }
        };

    StorageOptions baseOptions =
        StorageOptions.grpc()
            .setProjectId("vaibhavpratap-sdk-test")
            .setCredentials(rotatingCreds)
            .setAttemptDirectPath(false)
            .build();

    String sockPath = UdsServerRuntime.newUniqueSocketPath();
    try (UdsStorage uds =
        UdsStorage.newBuilder(baseOptions)
            .setDriverMode(DriverMode.SIDECAR)
            .setTransport(UdsTransport.GRPC)
            .setSidecarBinaryPath(sidecarBin)
            .setSocketPath(sockPath)
            .build()) {
      assertThat(tokenCounter.get()).isEqualTo(1);
      assertThat(uds.getSessionManager().sessionId()).isNotEmpty();
      assertThat(uds.getServerPid().isPresent()).isTrue();
      assertThat(uds.getServerPid().getAsLong()).isGreaterThan(0L);

      // Verify proactive UpdateToken rotation RPC succeeds under SO_PEERCRED binding
      long updatedExpiry = uds.getSessionManager().rotateTokenNow();
      assertThat(tokenCounter.get()).isEqualTo(2);
      assertThat(updatedExpiry).isGreaterThan(System.currentTimeMillis() / 1000L);

      // Verify GcsCore.Stats RPC returns live sidecar telemetry
      StatsResponse stats = uds.getCoreStats(true);
      assertThat(stats.getPid()).isGreaterThan(0);
      assertThat(stats.getRssBytes()).isGreaterThan(0L);
      assertThat(stats.getRpcsServed()).isAtLeast(2L);
    }

    // Verify clean socket & .shm removal on close()
    assertThat(Files.exists(Paths.get(sockPath))).isFalse();
    assertThat(Files.exists(Paths.get(sockPath + ".shm"))).isFalse();
  }
}
