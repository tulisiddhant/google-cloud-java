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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  /** Owns one or more {@link ManagedChannel}s and their shared {@link EpollEventLoopGroup}. */
  public static final class ChannelHandle implements AutoCloseable {
    private final List<ManagedChannel> channels;
    private final EpollEventLoopGroup eventLoopGroup;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    ChannelHandle(List<ManagedChannel> channels, EpollEventLoopGroup eventLoopGroup) {
      if (channels == null || channels.isEmpty()) {
        throw new IllegalArgumentException("channels must be non-empty");
      }
      this.channels = Collections.unmodifiableList(new ArrayList<ManagedChannel>(channels));
      this.eventLoopGroup = requireNonNull(eventLoopGroup, "eventLoopGroup");
    }

    public ManagedChannel channel() {
      return channels.get(0);
    }

    public List<ManagedChannel> channels() {
      return channels;
    }

    public int channelCount() {
      return channels.size();
    }

    @Override
    public void close() {
      if (!closed.compareAndSet(false, true)) {
        return;
      }
      for (int i = 0; i < channels.size(); i++) {
        channels.get(i).shutdown();
      }
      try {
        for (int i = 0; i < channels.size(); i++) {
          ManagedChannel ch = channels.get(i);
          if (!ch.awaitTermination(5, TimeUnit.SECONDS)) {
            ch.shutdownNow();
            ch.awaitTermination(2, TimeUnit.SECONDS);
          }
        }
      } catch (InterruptedException e) {
        for (int i = 0; i < channels.size(); i++) {
          channels.get(i).shutdownNow();
        }
        Thread.currentThread().interrupt();
      } finally {
        eventLoopGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS);
      }
    }
  }

  /** Creates a tuned {@code AF_UNIX} {@link ChannelHandle} connected to {@code socketPath}. */
  public static ChannelHandle open(String socketPath) throws IOException {
    return open(socketPath, Integer.getInteger("uds.local.channels", 1));
  }

  /**
   * Creates a tuned pool of {@code channelCount} {@code AF_UNIX} {@link ManagedChannel}s connected
   * to {@code socketPath}, sizing the {@link EpollEventLoopGroup} so every channel can bind to its
   * own Netty event-loop thread.
   */
  public static ChannelHandle open(String socketPath, int channelCount) throws IOException {
    requireNonNull(socketPath, "socketPath");
    if (channelCount <= 0) {
      throw new IllegalArgumentException("channelCount must be > 0, got: " + channelCount);
    }
    if (!Epoll.isAvailable()) {
      throw new IOException("Linux Netty Epoll transport is unavailable", Epoll.unavailabilityCause());
    }
    int threads = Math.max(EVENT_LOOP_THREADS, channelCount);
    EpollEventLoopGroup group =
        new EpollEventLoopGroup(threads, new DefaultThreadFactory("gcs-uds-epoll", true));
    List<ManagedChannel> created = new ArrayList<ManagedChannel>(channelCount);
    boolean ok = false;
    try {
      for (int i = 0; i < channelCount; i++) {
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
        created.add(channel);
      }
      ok = true;
      return new ChannelHandle(created, group);
    } finally {
      if (!ok) {
        for (int i = 0; i < created.size(); i++) {
          created.get(i).shutdownNow();
        }
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
