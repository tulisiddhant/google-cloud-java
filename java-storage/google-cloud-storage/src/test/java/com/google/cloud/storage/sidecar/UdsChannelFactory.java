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

import com.google.cloud.storage.sidecar.proto.GcsCoreGrpc;
import com.google.cloud.storage.sidecar.proto.PingRequest;
import com.google.cloud.storage.sidecar.proto.PingResponse;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Factory for high-throughput, Java 8-compatible Netty {@code AF_UNIX} gRPC channels tuned to
 * match the Rust Tonic UDS server's {@code 64 MiB} HTTP/2 window and {@code 8 MiB} socket buffers.
 */
public final class UdsChannelFactory {

  public static final int EVENT_LOOP_THREADS = 8;
  public static final int SOCKET_BUFFER_BYTES = 8 * 1024 * 1024;
  public static final int FLOW_CONTROL_WINDOW_BYTES = 64 * 1024 * 1024;
  public static final int MAX_INBOUND_MESSAGE_BYTES = 64 * 1024 * 1024;

  private static final ByteString PING_PAYLOAD = ByteString.copyFromUtf8("uds-ready");

  private UdsChannelFactory() {}

  /** Owns a {@link ManagedChannel} and its dedicated {@link EpollEventLoopGroup}. */
  public static final class ChannelHandle implements AutoCloseable {
    private final ManagedChannel channel;
    private final EpollEventLoopGroup eventLoopGroup;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    ChannelHandle(ManagedChannel channel, EpollEventLoopGroup eventLoopGroup) {
      this.channel = requireNonNull(channel, "channel");
      this.eventLoopGroup = requireNonNull(eventLoopGroup, "eventLoopGroup");
    }

    public ManagedChannel channel() {
      return channel;
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      channel.shutdown();
      try {
        if (!channel.awaitTermination(5, TimeUnit.SECONDS)) {
          channel.shutdownNow();
          channel.awaitTermination(2, TimeUnit.SECONDS);
        }
      } catch (InterruptedException e) {
        channel.shutdownNow();
        Thread.currentThread().interrupt();
      } finally {
        eventLoopGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS);
      }
    }
  }

  /** Creates a tuned {@code AF_UNIX} {@link ChannelHandle} connected to {@code socketPath}. */
  public static ChannelHandle open(String socketPath) throws IOException {
    requireNonNull(socketPath, "socketPath");
    if (!Epoll.isAvailable()) {
      throw new IOException("Linux Netty Epoll transport is unavailable", Epoll.unavailabilityCause());
    }
    EpollEventLoopGroup group =
        new EpollEventLoopGroup(
            EVENT_LOOP_THREADS, new DefaultThreadFactory("gcs-uds-epoll", true));
    boolean ok = false;
    try {
      ManagedChannel channel =
          NettyChannelBuilder.forAddress(new DomainSocketAddress(socketPath))
              .overrideAuthority("localhost")
              .channelType(EpollDomainSocketChannel.class)
              .eventLoopGroup(group)
              .withOption(ChannelOption.SO_RCVBUF, SOCKET_BUFFER_BYTES)
              .withOption(ChannelOption.SO_SNDBUF, SOCKET_BUFFER_BYTES)
              .initialFlowControlWindow(FLOW_CONTROL_WINDOW_BYTES)
              .maxInboundMessageSize(MAX_INBOUND_MESSAGE_BYTES)
              .directExecutor()
              .usePlaintext()
              .build();
      ok = true;
      return new ChannelHandle(channel, group);
    } finally {
      if (!ok) {
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      }
    }
  }

  /**
   * Probes {@code GcsCore.Ping} until the UDS server responds or {@code timeoutMs} elapses,
   * avoiding JDK 16+ {@code java.net.UnixDomainSocketAddress} to preserve Java 8 compatibility.
   */
  public static void awaitReady(ManagedChannel channel, long timeoutMs, UdsServerRuntime runtime)
      throws IOException {
    long deadlineNs = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    Throwable lastError = null;
    while (System.nanoTime() < deadlineNs) {
      if (runtime != null && !runtime.isAlive()) {
        String logs =
            runtime instanceof SidecarServerRuntime
                ? ((SidecarServerRuntime) runtime).capturedLogs()
                : "";
        throw new IOException(
            "UDS server runtime exited before becoming ready on "
                + runtime.socketPath()
                + (logs.isEmpty() ? "" : "\nLogs:\n" + logs));
      }
      try {
        PingResponse resp =
            GcsCoreGrpc.newBlockingStub(channel)
                .withDeadlineAfter(250, TimeUnit.MILLISECONDS)
                .ping(PingRequest.newBuilder().setPayload(PING_PAYLOAD).build());
        if (PING_PAYLOAD.equals(resp.getPayload())) {
          return;
        }
      } catch (RuntimeException e) {
        lastError = e;
      }
      try {
        Thread.sleep(20);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted while waiting for UDS server readiness", e);
      }
    }
    throw new IOException(
        "Timed out after " + timeoutMs + " ms waiting for GcsCore.Ping readiness", lastError);
  }
}
