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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.benchmark.BenchmarkConfig;
import com.google.cloud.storage.sidecar.proto.PingRequest;
import com.google.cloud.storage.sidecar.proto.PingResponse;
import com.google.cloud.storage.sidecar.proto.StatsResponse;
import com.google.protobuf.ByteString;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Assume;
import org.junit.Test;

/**
 * Step 8 ({@code uds-inproc-v2}) live JNA FFI verification tests against unmodified {@code
 * libstorage_grpc_uds.so}.
 */
public class UdsInprocTest {

  @Test
  public void benchmarkConfigParsesUdsInprocDriverAndLibraryFlag() {
    BenchmarkConfig cfg =
        BenchmarkConfig.fromArgs(
            new String[] {
              "--driver=uds-inproc",
              "--protocol=GRPC",
              "--bucket-type=zonal",
              "--inproc-lib=/tmp/libstorage_grpc_uds.so"
            });
    assertThat(cfg.driver()).isEqualTo(BenchmarkConfig.DriverType.UDS_INPROC);
    assertThat(cfg.driver().cliName()).isEqualTo("uds-inproc");
    assertThat(cfg.protocol()).isEqualTo(UdsTransport.GRPC);
    assertThat(cfg.bucketType()).isEqualTo(BenchmarkConfig.BucketType.ZONAL);
    assertThat(cfg.inprocLibraryPath()).isEqualTo("/tmp/libstorage_grpc_uds.so");
  }

  @Test
  public void liveInprocLibraryStartsInSamePidAnswersPingSessionStatsAndStopsCleanlyTwice()
      throws Exception {
    String libPath = System.getProperty("uds.inproc.lib", "");
    Assume.assumeFalse(
        "Pass -Duds.inproc.lib=.../libstorage_grpc_uds.so to run live JNA inproc test",
        libPath.isEmpty());
    Assume.assumeTrue("libstorage_grpc_uds.so must exist", Files.isRegularFile(Paths.get(libPath)));

    long jvmPid = InprocServerRuntime.resolveCurrentProcessPid();
    assertThat(jvmPid).isGreaterThan(0L);

    // Run two consecutive start -> RPC -> stop lifecycles in the same JVM to verify clean Tokio
    // runtime teardown and socket unlinking via gcs_core_stop_inproc.
    for (int cycle = 0; cycle < 2; cycle++) {
      final AtomicInteger tokenRefreshes = new AtomicInteger(0);
      OAuth2Credentials rotatingCreds =
          new OAuth2Credentials() {
            @Override
            public AccessToken refreshAccessToken() {
              int n = tokenRefreshes.incrementAndGet();
              return new AccessToken(
                  "inproc-token-" + n, new Date(System.currentTimeMillis() + 3600_000L));
            }
          };
      StorageOptions opts =
          StorageOptions.grpc()
              .setProjectId("vaibhavpratap-sdk-test")
              .setCredentials(rotatingCreds)
              .setAttemptDirectPath(false)
              .build();

      String sockPath;
      try (UdsStorage uds =
          UdsStorage.newBuilder(opts)
              .setDriverMode(DriverMode.INPROC)
              .setTransport(UdsTransport.GRPC)
              .setInprocLibraryPath(libPath)
              .build()) {
        sockPath = uds.getSocketPath();
        assertThat(uds.getDriverMode()).isEqualTo(DriverMode.INPROC);
        assertThat(Files.exists(Paths.get(sockPath))).isTrue();

        ByteString payload = ByteString.copyFromUtf8("hello-inproc-" + cycle);
        PingResponse ping =
            uds.blockingStub().ping(PingRequest.newBuilder().setPayload(payload).build());
        assertThat(ping.getPayload()).isEqualTo(payload);

        uds.getSessionManager().rotateTokenNow();
        assertThat(tokenRefreshes.get()).isAtLeast(2);

        StatsResponse stats = uds.getCoreStats(true);
        // Critical architectural assertion: in uds-inproc, GcsCore runs inside the SAME OS PID as
        // the host JVM process!
        assertThat((long) stats.getPid()).isEqualTo(jvmPid);
        assertThat(stats.getRssBytes()).isGreaterThan(0L);
      }

      assertThat(Files.exists(Paths.get(sockPath))).isFalse();
      assertThat(Files.exists(Paths.get(sockPath + ".shm"))).isFalse();
    }
  }
}
