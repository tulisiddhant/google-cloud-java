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

import static java.util.Objects.requireNonNull;

import com.google.auth.Credentials;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.cloud.storage.sidecar.proto.CreateSessionRequest;
import com.google.cloud.storage.sidecar.proto.CreateSessionResponse;
import com.google.cloud.storage.sidecar.proto.GcsCoreGrpc;
import com.google.cloud.storage.sidecar.proto.SessionRef;
import com.google.cloud.storage.sidecar.proto.UpdateTokenRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall.SimpleForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Owns per-caller OAuth2 credential lending over UDS ({@code CreateSession}, {@code x-session}
 * metadata injection, proactive {@code UpdateToken} refresh 300s prior to expiry, and {@code
 * CloseSession}).
 */
public final class GcsSessionManager implements ClientInterceptor, AutoCloseable {

  public static final Metadata.Key<String> SESSION_HEADER_KEY =
      Metadata.Key.of("x-session", Metadata.ASCII_STRING_MARSHALLER);

  private static final String CLOUD_PLATFORM_SCOPE =
      "https://www.googleapis.com/auth/cloud-platform";
  private static final long PROACTIVE_REFRESH_LEAD_SECS = 300L;
  private static final long MIN_REFRESH_DELAY_SECS = 30L;
  private static final long DEFAULT_FALLBACK_TTL_SECS = 3600L;

  private final OAuth2Credentials credentials;
  private final GcsCoreGrpc.GcsCoreBlockingStub rawSessionStub;
  private final String sessionId;
  private final ScheduledExecutorService scheduler;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  private volatile ScheduledFuture<?> nextRefreshTask;

  private GcsSessionManager(
      OAuth2Credentials credentials,
      GcsCoreGrpc.GcsCoreBlockingStub rawSessionStub,
      String sessionId,
      long initialExpiryUnixSecs) {
    this.credentials = credentials;
    this.rawSessionStub = rawSessionStub;
    this.sessionId = sessionId;
    this.scheduler =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "gcs-uds-token-refresher");
              t.setDaemon(true);
              return t;
            });
    scheduleNextRefresh(initialExpiryUnixSecs);
  }

  /** Resolves OAuth2 credentials, calls {@code GcsCore.CreateSession}, and starts token rotation. */
  public static GcsSessionManager create(ManagedChannel rawChannel, StorageOptions baseOptions)
      throws IOException {
    requireNonNull(rawChannel, "rawChannel");
    requireNonNull(baseOptions, "baseOptions");

    OAuth2Credentials oauth2 = resolveCredentials(baseOptions);
    TokenSnapshot initial = fetchFreshToken(oauth2, false);

    CreateSessionRequest.Builder reqBuilder =
        CreateSessionRequest.newBuilder()
            .setAccessToken(initial.tokenValue)
            .setExpiryUnixSecs(initial.expiryUnixSecs);
    if (baseOptions.getQuotaProjectId() != null && !baseOptions.getQuotaProjectId().isEmpty()) {
      reqBuilder.setQuotaProject(baseOptions.getQuotaProjectId());
    }

    GcsCoreGrpc.GcsCoreBlockingStub stub = GcsCoreGrpc.newBlockingStub(rawChannel);
    CreateSessionResponse resp =
        stub.withDeadlineAfter(30, TimeUnit.SECONDS).createSession(reqBuilder.build());
    if (resp.getSessionId().isEmpty()) {
      throw new IOException("GcsCore.CreateSession returned an empty session_id");
    }
    return new GcsSessionManager(oauth2, stub, resp.getSessionId(), initial.expiryUnixSecs);
  }

  private static OAuth2Credentials resolveCredentials(StorageOptions baseOptions)
      throws IOException {
    Credentials configured = baseOptions.getCredentials();
    if (configured instanceof OAuth2Credentials && !(configured instanceof NoCredentials)) {
      if (configured instanceof GoogleCredentials) {
        GoogleCredentials gc = (GoogleCredentials) configured;
        if (gc.createScopedRequired()) {
          return gc.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
        }
      }
      return (OAuth2Credentials) configured;
    }
    GoogleCredentials adc = GoogleCredentials.getApplicationDefault();
    if (adc.createScopedRequired()) {
      adc = adc.createScoped(Collections.singletonList(CLOUD_PLATFORM_SCOPE));
    }
    return adc;
  }

  private static TokenSnapshot fetchFreshToken(OAuth2Credentials creds, boolean forceRefresh)
      throws IOException {
    if (forceRefresh) {
      creds.refresh();
    } else {
      creds.refreshIfExpired();
    }
    AccessToken token = creds.getAccessToken();
    if (token == null || token.getTokenValue() == null || token.getTokenValue().isEmpty()) {
      token = creds.refreshAccessToken();
    }
    if (token == null || token.getTokenValue() == null || token.getTokenValue().isEmpty()) {
      throw new IOException("OAuth2Credentials produced an empty bearer token");
    }
    Date exp = token.getExpirationTime();
    long nowSecs = System.currentTimeMillis() / 1000L;
    long expirySecs = exp != null ? (exp.getTime() / 1000L) : (nowSecs + DEFAULT_FALLBACK_TTL_SECS);
    return new TokenSnapshot(token.getTokenValue(), expirySecs);
  }

  private void scheduleNextRefresh(long expiryUnixSecs) {
    if (closed.get()) {
      return;
    }
    long nowSecs = System.currentTimeMillis() / 1000L;
    long delaySecs = Math.max(MIN_REFRESH_DELAY_SECS, (expiryUnixSecs - nowSecs) - PROACTIVE_REFRESH_LEAD_SECS);
    nextRefreshTask =
        scheduler.schedule(this::runScheduledRefresh, delaySecs, TimeUnit.SECONDS);
  }

  private void runScheduledRefresh() {
    if (closed.get()) {
      return;
    }
    try {
      long newExpiry = rotateTokenNow();
      scheduleNextRefresh(newExpiry);
    } catch (Exception e) {
      if (!closed.get()) {
        nextRefreshTask =
            scheduler.schedule(this::runScheduledRefresh, MIN_REFRESH_DELAY_SECS, TimeUnit.SECONDS);
      }
    }
  }

  /** Forces an immediate credential refresh and pushes {@code GcsCore.UpdateToken}. */
  public long rotateTokenNow() throws IOException {
    TokenSnapshot refreshed = fetchFreshToken(credentials, true);
    rawSessionStub
        .withDeadlineAfter(15, TimeUnit.SECONDS)
        .updateToken(
            UpdateTokenRequest.newBuilder()
                .setSessionId(sessionId)
                .setAccessToken(refreshed.tokenValue)
                .setExpiryUnixSecs(refreshed.expiryUnixSecs)
                .build());
    return refreshed.expiryUnixSecs;
  }

  public String sessionId() {
    return sessionId;
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
    return new SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        headers.put(SESSION_HEADER_KEY, sessionId);
        super.start(responseListener, headers);
      }
    };
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    ScheduledFuture<?> task = this.nextRefreshTask;
    if (task != null) {
      task.cancel(false);
    }
    scheduler.shutdownNow();
    try {
      rawSessionStub
          .withDeadlineAfter(3, TimeUnit.SECONDS)
          .closeSession(SessionRef.newBuilder().setSessionId(sessionId).build());
    } catch (RuntimeException ignored) {
      // Best-effort session release before channel shutdown.
    }
  }

  private static final class TokenSnapshot {
    final String tokenValue;
    final long expiryUnixSecs;

    TokenSnapshot(String tokenValue, long expiryUnixSecs) {
      this.tokenValue = tokenValue;
      this.expiryUnixSecs = expiryUnixSecs;
    }
  }
}
