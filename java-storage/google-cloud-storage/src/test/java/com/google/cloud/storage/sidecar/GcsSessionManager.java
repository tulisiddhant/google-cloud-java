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

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import gcscore.v1.CreateSessionRequest;
import gcscore.v1.CreateSessionResponse;
import gcscore.v1.GcsCoreGrpc;
import gcscore.v1.SessionRef;
import gcscore.v1.UpdateTokenRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages OAuth2 token-lending sessions for the GCS Rust Core sidecar ({@code gcs-core-server}).
 *
 * <p>Implements {@code CreateSession}, automatic background {@code UpdateToken} rotation (refreshing
 * {@link OAuth2Credentials} 300 seconds prior to expiry so multi-workload suites never suffer mid-run
 * {@code 401 UNAUTHENTICATED} failures), and {@code CloseSession} over UDS.
 */
public final class GcsSessionManager implements AutoCloseable {

  public static final Metadata.Key<String> SESSION_METADATA_KEY =
      Metadata.Key.of("x-session", Metadata.ASCII_STRING_MARSHALLER);

  /** Refresh OAuth2 tokens 5 minutes (300s) before expiry. */
  private static final long REFRESH_MARGIN_SECS = 300L;

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";

  private final GcsCoreGrpc.GcsCoreBlockingStub stub;
  private final OAuth2Credentials credentials;
  private final boolean allowDummyFallback;
  private final AtomicInteger updateCount = new AtomicInteger(0);

  private ScheduledExecutorService refreshScheduler;
  private ScheduledFuture<?> nextRefreshTask;
  private String sessionId;
  private String currentToken;
  private long expiryEpochSecs;
  private String quotaProject;

  public GcsSessionManager(GcsCoreGrpc.GcsCoreBlockingStub stub) {
    this(stub, null, /* allowDummyFallback= */ true);
  }

  public GcsSessionManager(
      GcsCoreGrpc.GcsCoreBlockingStub stub,
      Credentials explicitCredentials,
      boolean allowDummyFallback) {
    this.stub = stub;
    this.allowDummyFallback = allowDummyFallback;
    if (explicitCredentials instanceof NoCredentials) {
      this.credentials = null;
    } else if (explicitCredentials instanceof OAuth2Credentials) {
      this.credentials = maybeScope((OAuth2Credentials) explicitCredentials);
    } else {
      this.credentials = null;
    }
  }

  private static OAuth2Credentials maybeScope(OAuth2Credentials creds) {
    if (creds instanceof GoogleCredentials) {
      GoogleCredentials gc = (GoogleCredentials) creds;
      if (gc.createScopedRequired()) {
        return gc.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
      }
    }
    return creds;
  }

  private static void log(String msg) {
    System.out.println("[GcsSessionManager] " + msg);
  }

  private static final class ResolvedToken {
    final String tokenValue;
    final long expiryEpochSecs;
    final OAuth2Credentials refreshableSource;

    ResolvedToken(String tokenValue, long expiryEpochSecs, OAuth2Credentials refreshableSource) {
      this.tokenValue = tokenValue;
      this.expiryEpochSecs = expiryEpochSecs;
      this.refreshableSource = refreshableSource;
    }
  }

  /**
   * Resolves an OAuth2 access token (with real expiration metadata) and establishes an
   * authenticated session with the Rust UDS server.
   */
  public synchronized String initSession() {
    return initSession(null, null);
  }

  /**
   * Establishes a session with explicit token and quota project, or resolves them from {@link
   * GoogleCredentials#getApplicationDefault()} / environment.
   */
  public synchronized String initSession(String explicitToken, String explicitQuotaProject) {
    if (this.sessionId != null) {
      log("Session already established (ID: " + this.sessionId + "). Reusing.");
      return this.sessionId;
    }

    log("======================================================================");
    log("INITIALIZING OAUTH2 TOKEN-LENDING SESSION OVER UDS");
    log("======================================================================");

    ResolvedToken resolved;
    if (explicitToken != null && !explicitToken.isEmpty()) {
      resolved = new ResolvedToken(explicitToken, Instant.now().getEpochSecond() + 3600L, null);
    } else {
      resolved = resolveTokenFromCredentialsOrEnv();
    }

    this.currentToken = resolved.tokenValue;
    this.expiryEpochSecs = resolved.expiryEpochSecs;
    this.quotaProject =
        explicitQuotaProject != null ? explicitQuotaProject : resolveQuotaProject(resolved.refreshableSource);

    log("Issuing CreateSession RPC to UDS server...");
    log(
        "  - Token prefix    : "
            + (currentToken.length() > 10
                ? currentToken.substring(0, 10) + "..."
                : currentToken));
    log("  - Expiry (epoch)  : " + expiryEpochSecs);
    log("  - Quota Project   : " + (this.quotaProject.isEmpty() ? "(none)" : this.quotaProject));

    CreateSessionRequest req =
        CreateSessionRequest.newBuilder()
            .setAccessToken(this.currentToken)
            .setExpiryUnixSecs(this.expiryEpochSecs)
            .setQuotaProject(this.quotaProject)
            .build();

    CreateSessionResponse resp = stub.withDeadlineAfter(10, TimeUnit.SECONDS).createSession(req);
    this.sessionId = resp.getSessionId();

    if (resolved.refreshableSource != null) {
      startBackgroundRefresh(resolved.refreshableSource);
    }

    log("Session successfully registered!");
    log("  - Assigned Session ID : " + this.sessionId);
    log("  - Kernel SO_PEERCRED  : Bound to current JVM process (UID / PID)");
    log("======================================================================");
    return this.sessionId;
  }

  private void startBackgroundRefresh(final OAuth2Credentials source) {
    ensureScheduler();
    long now = Instant.now().getEpochSecond();
    long delaySecs =
        this.expiryEpochSecs > 0
            ? Math.max(5L, (this.expiryEpochSecs - now) - REFRESH_MARGIN_SECS)
            : 1800L;
    log("Scheduled automatic OAuth2 token refresh in " + delaySecs + " seconds.");
    this.nextRefreshTask =
        refreshScheduler.schedule(
            new Runnable() {
              @Override
              public void run() {
                refreshFromSource(source);
              }
            },
            delaySecs,
            TimeUnit.SECONDS);
  }

  private synchronized void ensureScheduler() {
    if (this.refreshScheduler == null) {
      this.refreshScheduler =
          Executors.newSingleThreadScheduledExecutor(
              new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                  Thread t = new Thread(r, "gcs-uds-token-refresh");
                  t.setDaemon(true);
                  return t;
                }
              });
    }
  }

  /**
   * Immediately refreshes the underlying {@link OAuth2Credentials} and pushes {@code UpdateToken}
   * over UDS, then schedules the next refresh prior to the new token's expiration.
   */
  public synchronized void refreshFromSource(final OAuth2Credentials source) {
    if (this.sessionId == null || source == null) {
      return;
    }
    try {
      AccessToken refreshed = source.refreshAccessToken();
      if (refreshed == null || refreshed.getTokenValue() == null) {
        source.refresh();
        refreshed = source.getAccessToken();
      }
      if (refreshed != null && refreshed.getTokenValue() != null) {
        Date expDate = refreshed.getExpirationTime();
        long newExpiry =
            expDate != null
                ? (expDate.getTime() / 1000L)
                : (Instant.now().getEpochSecond() + 3600L);
        updateToken(refreshed.getTokenValue(), newExpiry);
        long now = Instant.now().getEpochSecond();
        long nextDelay = Math.max(5L, (newExpiry - now) - REFRESH_MARGIN_SECS);
        if (refreshScheduler != null && !refreshScheduler.isShutdown()) {
          nextRefreshTask =
              refreshScheduler.schedule(
                  new Runnable() {
                    @Override
                    public void run() {
                      refreshFromSource(source);
                    }
                  },
                  nextDelay,
                  TimeUnit.SECONDS);
        }
      }
    } catch (Exception e) {
      log("Warning: Background OAuth2 token refresh failed (retrying in 30s): " + e.getMessage());
      if (refreshScheduler != null && !refreshScheduler.isShutdown()) {
        nextRefreshTask =
            refreshScheduler.schedule(
                new Runnable() {
                  @Override
                  public void run() {
                    refreshFromSource(source);
                  }
                },
                30L,
                TimeUnit.SECONDS);
      }
    }
  }

  /** Schedules periodic {@link OAuth2Credentials} rotation at a short interval (used by tests). */
  public synchronized void schedulePeriodicRefresh(
      final OAuth2Credentials source, long periodMillis) {
    if (this.sessionId == null) {
      throw new IllegalStateException("Session must be initialized before scheduling refresh");
    }
    ensureScheduler();
    if (nextRefreshTask != null) {
      nextRefreshTask.cancel(false);
    }
    nextRefreshTask =
        refreshScheduler.scheduleAtFixedRate(
            new Runnable() {
              @Override
              public void run() {
                try {
                  AccessToken refreshed = source.refreshAccessToken();
                  if (refreshed != null && refreshed.getTokenValue() != null) {
                    long exp =
                        refreshed.getExpirationTime() != null
                            ? (refreshed.getExpirationTime().getTime() / 1000L)
                            : (Instant.now().getEpochSecond() + 3600L);
                    updateToken(refreshed.getTokenValue(), exp);
                  }
                } catch (IOException e) {
                  log("Periodic refresh error: " + e.getMessage());
                }
              }
            },
            periodMillis,
            periodMillis,
            TimeUnit.MILLISECONDS);
  }

  public synchronized String getSessionId() {
    return sessionId;
  }

  public int getUpdateCount() {
    return updateCount.get();
  }

  /**
   * Atomically updates the lent token inside the Rust UDS server for an active session without
   * dropping in-flight streams.
   */
  public synchronized void updateToken(String newToken, long newExpiryEpochSecs) {
    if (this.sessionId == null) {
      throw new IllegalStateException("Cannot update token: no active session.");
    }
    this.currentToken = newToken;
    this.expiryEpochSecs = newExpiryEpochSecs;

    UpdateTokenRequest req =
        UpdateTokenRequest.newBuilder()
            .setSessionId(this.sessionId)
            .setAccessToken(newToken)
            .setExpiryUnixSecs(newExpiryEpochSecs)
            .build();

    stub.withDeadlineAfter(10, TimeUnit.SECONDS).updateToken(req);
    updateCount.incrementAndGet();
    log("Token updated in UDS server for session " + this.sessionId + " (new expiry=" + newExpiryEpochSecs + ").");
  }

  /**
   * Returns a gRPC {@link ClientInterceptor} that automatically injects {@code x-session:
   * <sessionId>} on all outbound RPC calls.
   */
  public ClientInterceptor createClientInterceptor() {
    return new ClientInterceptor() {
      @Override
      public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
          MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
            next.newCall(method, callOptions)) {
          @Override
          public void start(Listener<RespT> responseListener, Metadata headers) {
            String sid = getSessionId();
            if (sid != null) {
              headers.put(SESSION_METADATA_KEY, sid);
            }
            super.start(responseListener, headers);
          }
        };
      }
    };
  }

  @Override
  public synchronized void close() {
    if (nextRefreshTask != null) {
      nextRefreshTask.cancel(false);
      nextRefreshTask = null;
    }
    if (refreshScheduler != null) {
      refreshScheduler.shutdownNow();
      refreshScheduler = null;
    }
    if (this.sessionId != null) {
      try {
        log("Closing session: " + this.sessionId + "...");
        stub.withDeadlineAfter(5, TimeUnit.SECONDS)
            .closeSession(SessionRef.newBuilder().setSessionId(this.sessionId).build());
        log("Session closed cleanly.");
      } catch (Exception e) {
        log("Warning: Failed to close session cleanly: " + e.getMessage());
      } finally {
        this.sessionId = null;
      }
    }
  }

  private ResolvedToken resolveTokenFromCredentialsOrEnv() {
    // 1. Explicit system property or environment variable override
    String explicit = System.getProperty("gcs.sidecar.access.token");
    if (explicit == null || explicit.isEmpty()) {
      explicit = System.getenv("GCS_SIDECAR_ACCESS_TOKEN");
    }
    if (explicit != null && !explicit.isEmpty()) {
      return new ResolvedToken(explicit.trim(), Instant.now().getEpochSecond() + 3600L, null);
    }

    // 2. Explicit OAuth2Credentials supplied to GcsSessionManager
    if (this.credentials != null) {
      try {
        this.credentials.refreshIfExpired();
        AccessToken at = this.credentials.getAccessToken();
        if (at == null || at.getTokenValue() == null) {
          at = this.credentials.refreshAccessToken();
        }
        if (at != null && at.getTokenValue() != null && !at.getTokenValue().isEmpty()) {
          long exp =
              at.getExpirationTime() != null
                  ? (at.getExpirationTime().getTime() / 1000L)
                  : (Instant.now().getEpochSecond() + 3600L);
          return new ResolvedToken(at.getTokenValue(), exp, this.credentials);
        }
      } catch (IOException e) {
        if (!allowDummyFallback) {
          throw new IllegalStateException("Failed to refresh supplied OAuth2Credentials", e);
        }
      }
    }

    // 3. Application Default Credentials (only in live mode or when dummy fallback is disabled,
    //    avoiding MDS network delays during offline unit/mock runs)
    boolean isLive =
        Boolean.parseBoolean(
            System.getProperty("gcs.live.benchmark", System.getProperty("gcs.bench.live", "false")));
    if (isLive || !allowDummyFallback) {
      try {
        OAuth2Credentials adc = maybeScope(GoogleCredentials.getApplicationDefault());
        adc.refreshIfExpired();
        AccessToken at = adc.getAccessToken();
        if (at == null || at.getTokenValue() == null) {
          at = adc.refreshAccessToken();
        }
        if (at != null && at.getTokenValue() != null && !at.getTokenValue().isEmpty()) {
          long exp =
              at.getExpirationTime() != null
                  ? (at.getExpirationTime().getTime() / 1000L)
                  : (Instant.now().getEpochSecond() + 3600L);
          return new ResolvedToken(at.getTokenValue(), exp, adc);
        }
      } catch (Exception e) {
        log("ADC lookup notice (" + e.getMessage() + "); trying gcloud fallback...");
      }

      // 4. Fallback to gcloud CLI in live developer environments without ADC file
      try {
        ProcessBuilder pb = new ProcessBuilder("gcloud", "auth", "print-access-token");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        try (BufferedReader reader =
            new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
          String token = reader.readLine();
          if (p.waitFor(10, TimeUnit.SECONDS)
              && p.exitValue() == 0
              && token != null
              && !token.trim().isEmpty()) {
            return new ResolvedToken(
                token.trim(), Instant.now().getEpochSecond() + 3300L, null);
          }
        }
      } catch (Throwable ignored) {
      }

      if (!allowDummyFallback) {
        throw new IllegalStateException(
            "Unable to resolve OAuth2 access token from ADC, explicit credentials, or gcloud.");
      }
    }

    // 5. Offline mock/unit test fallback token (expiry = 0 disables expiration check in mock runs)
    return new ResolvedToken("sidecar_experiment_dummy_bearer_token", 0L, null);
  }

  private static String resolveQuotaProject(OAuth2Credentials creds) {
    String qp = System.getProperty("gcs.sidecar.quota.project");
    if (qp == null || qp.isEmpty()) {
      qp = System.getenv("GCS_SIDECAR_QUOTA_PROJECT");
    }
    if ((qp == null || qp.isEmpty()) && creds instanceof GoogleCredentials) {
      qp = ((GoogleCredentials) creds).getQuotaProjectId();
    }
    if (qp == null || qp.isEmpty()) {
      qp = System.getProperty("gcs.project.id");
    }
    if (qp == null || qp.isEmpty()) {
      qp = System.getenv("GOOGLE_CLOUD_PROJECT");
    }
    return qp != null ? qp.trim() : "";
  }
}
