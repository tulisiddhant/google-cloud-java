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
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Instant;

/**
 * Manages OAuth2 token-lending sessions for the GCS Rust Core sidecar (PR #7350).
 * Implements CreateSession, UpdateToken, and CloseSession RPCs over UDS, and provides
 * a gRPC ClientInterceptor to attach the required 'x-session' header to outbound calls.
 */
public final class GcsSessionManager implements AutoCloseable {

  public static final Metadata.Key<String> SESSION_METADATA_KEY =
      Metadata.Key.of("x-session", Metadata.ASCII_STRING_MARSHALLER);

  private final GcsCoreGrpc.GcsCoreBlockingStub stub;
  private String sessionId;
  private String currentToken;
  private long expiryEpochSecs;
  private String quotaProject;

  public GcsSessionManager(GcsCoreGrpc.GcsCoreBlockingStub stub) {
    this.stub = stub;
  }

  private static void log(String msg) {
    System.out.println("[GcsSessionManager] " + msg);
  }

  /**
   * Resolves an OAuth2 access token and establishes an authenticated session with the sidecar.
   */
  public synchronized String initSession() {
    return initSession(null, null);
  }

  /**
   * Establishes a session with explicit token and quota project, or resolves them from the environment.
   */
  public synchronized String initSession(String token, String quotaProject) {
    if (this.sessionId != null) {
      log("Session already established (ID: " + this.sessionId + "). Reusing.");
      return this.sessionId;
    }

    log("======================================================================");
    log("INITIALIZING OAUTH2 TOKEN-LENDING SESSION OVER UDS");
    log("======================================================================");

    if (token == null || token.isEmpty()) {
      token = resolveAccessToken();
    }
    this.currentToken = token;
    this.quotaProject = quotaProject != null ? quotaProject : resolveQuotaProject();
    this.expiryEpochSecs = Instant.now().getEpochSecond() + 3600; // 1-hour nominal validity

    log("Issuing CreateSession RPC to sidecar...");
    log("  - Token prefix    : " + (currentToken.length() > 10 ? currentToken.substring(0, 10) + "..." : currentToken));
    log("  - Expiry (epoch)  : " + expiryEpochSecs);
    log("  - Quota Project   : " + (this.quotaProject.isEmpty() ? "(none)" : this.quotaProject));

    CreateSessionRequest req =
        CreateSessionRequest.newBuilder()
            .setAccessToken(this.currentToken)
            .setExpiryUnixSecs(this.expiryEpochSecs)
            .setQuotaProject(this.quotaProject)
            .build();

    CreateSessionResponse resp = stub.createSession(req);
    this.sessionId = resp.getSessionId();

    log("Session successfully registered with sidecar!");
    log("  - Assigned Session ID : " + this.sessionId);
    log("  - Kernel SO_PEERCRED  : Bound to current JVM process (UID / PID)");
    log("======================================================================");
    return this.sessionId;
  }

  public synchronized String getSessionId() {
    return sessionId;
  }

  /**
   * Refreshes the lent token inside the sidecar for an active session without dropping in-flight streams.
   */
  public synchronized void updateToken(String newToken, long newExpiryEpochSecs) {
    if (this.sessionId == null) {
      throw new IllegalStateException("Cannot update token: no active session.");
    }
    log("Updating lent token for session: " + this.sessionId);
    this.currentToken = newToken;
    this.expiryEpochSecs = newExpiryEpochSecs;

    UpdateTokenRequest req =
        UpdateTokenRequest.newBuilder()
            .setSessionId(this.sessionId)
            .setAccessToken(newToken)
            .setExpiryUnixSecs(newExpiryEpochSecs)
            .build();

    stub.updateToken(req);
    log("Token successfully updated in sidecar.");
  }

  /**
   * Returns a gRPC ClientInterceptor that automatically injects 'x-session: <sessionId>'
   * on all outbound RPC calls.
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
    if (this.sessionId != null) {
      try {
        log("Closing session: " + this.sessionId + "...");
        stub.closeSession(SessionRef.newBuilder().setSessionId(this.sessionId).build());
        log("Session closed cleanly.");
      } catch (Exception e) {
        log("Warning: Failed to close session cleanly: " + e.getMessage());
      } finally {
        this.sessionId = null;
      }
    }
  }

  private static String resolveAccessToken() {
    // 1. Check system property or environment override
    String explicit = System.getProperty("gcs.sidecar.access.token");
    if (explicit == null) {
      explicit = System.getenv("GCS_SIDECAR_ACCESS_TOKEN");
    }
    if (explicit != null && !explicit.isEmpty()) {
      return explicit;
    }

    // 2. Fall back to gcloud auth print-access-token
    try {
      ProcessBuilder pb = new ProcessBuilder("gcloud", "auth", "print-access-token");
      pb.redirectErrorStream(true);
      Process p = pb.start();
      try (BufferedReader reader =
          new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8))) {
        String token = reader.readLine();
        if (p.waitFor() == 0 && token != null && !token.trim().isEmpty()) {
          return token.trim();
        }
      }
    } catch (Throwable ignored) {
    }

    // 3. Fallback dummy token for mock/offline testing
    return "sidecar_experiment_dummy_bearer_token";
  }

  private static String resolveQuotaProject() {
    String qp = System.getProperty("gcs.sidecar.quota.project");
    if (qp == null) {
      qp = System.getenv("GCS_SIDECAR_QUOTA_PROJECT");
    }
    if (qp == null) {
      qp = System.getenv("GOOGLE_CLOUD_PROJECT");
    }
    return qp != null ? qp.trim() : "";
  }
}
