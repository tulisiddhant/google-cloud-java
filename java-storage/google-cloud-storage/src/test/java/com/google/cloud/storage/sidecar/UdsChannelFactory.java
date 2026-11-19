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

import gcscore.v1.GcsCoreGrpc;
import gcscore.v1.PingRequest;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Creates tuned Netty gRPC ManagedChannels over Unix Domain Sockets (UDS) aligned with
 * the high-throughput GCS Rust core sidecar (PR #7350 / gcs-core-server).
 *
 * <p>Optimizations and lifecycle guarantees:
 * <ol>
 *   <li>Linux Epoll native domain socket transport ({@link EpollDomainSocketChannel}) with KQueue
 *       fallback on macOS — compatible with JDK 8+ without requiring JDK 16+
 *       {@code UnixDomainSocketAddress}.
 *   <li>8 MiB kernel socket buffer options ({@code SO_RCVBUF} and {@code SO_SNDBUF}) matching
 *       {@code gcs-core-server} {@code setsockopt}.
 *   <li>64 MiB HTTP/2 stream flow control window matching Tonic {@code initial_stream_window_size}
 *       to eliminate HTTP/2 {@code WINDOW_UPDATE} stalls on multi-megabyte transfers.
 *   <li>64 MiB maximum inbound message size matching sidecar encoding/decoding caps.
 *   <li>{@code directExecutor()} dispatch to eliminate thread-hop overhead on UDS chunk callbacks.
 *   <li>Deterministic {@link EventLoopGroup} tracking and graceful shutdown via {@link
 *       #shutdownChannel(ManagedChannel, long, TimeUnit)} to prevent thread/FD leaks.
 * </ol>
 */
public final class UdsChannelFactory {

  public static final int SOCKET_BUFFER_SIZE = 8 * 1024 * 1024;   // 8 MiB SO_RCVBUF / SO_SNDBUF
  public static final int FLOW_CONTROL_WINDOW = 64 * 1024 * 1024; // 64 MiB HTTP/2 stream window
  public static final int MAX_MESSAGE_SIZE = 64 * 1024 * 1024;    // 64 MiB max gRPC frame/message
  public static final int MAX_METADATA_SIZE = 64 * 1024;          // 64 KiB header limit

  private static final Map<ManagedChannel, EventLoopGroup> CHANNEL_GROUPS =
      Collections.synchronizedMap(new IdentityHashMap<ManagedChannel, EventLoopGroup>());

  private UdsChannelFactory() {}

  private static void log(String message) {
    System.out.println("[UdsChannelFactory] " + message);
  }

  public static ManagedChannel createChannel(Path socketPath) {
    return createChannel(socketPath.toAbsolutePath().toString());
  }

  public static ManagedChannel createChannel(String socketPath) {
    return buildChannel(socketPath, /* threadsOverride= */ 0, /* verbose= */ true);
  }

  /**
   * JDK 8-compatible readiness probe over Unix Domain Sockets (supporting both filesystem paths and
   * Linux {@code @abstract} socket names) using Netty domain sockets and a short-deadline {@code
   * Ping} RPC.
   */
  public static boolean probeReadiness(Path socketPath) {
    return probeReadiness(socketPath.toAbsolutePath().toString(), 250);
  }

  public static boolean probeReadiness(String socketPath, long timeoutMs) {
    if (socketPath == null || socketPath.isEmpty()) {
      return false;
    }
    if (!socketPath.startsWith("@") && !Files.exists(Paths.get(socketPath))) {
      return false;
    }
    ManagedChannel probeChannel = null;
    try {
      probeChannel = buildChannel(socketPath, /* threadsOverride= */ 1, /* verbose= */ false);
      GcsCoreGrpc.newBlockingStub(probeChannel)
          .withDeadlineAfter(Math.max(50L, timeoutMs), TimeUnit.MILLISECONDS)
          .ping(PingRequest.getDefaultInstance());
      return true;
    } catch (Throwable ignored) {
      return false;
    } finally {
      if (probeChannel != null) {
        shutdownChannel(probeChannel, 500, TimeUnit.MILLISECONDS);
      }
    }
  }

  /**
   * Shuts down both the gRPC {@link ManagedChannel} and its dedicated Netty {@link EventLoopGroup}.
   */
  public static void shutdownChannel(ManagedChannel channel, long timeout, TimeUnit unit) {
    if (channel == null) {
      return;
    }
    EventLoopGroup group = CHANNEL_GROUPS.remove(channel);
    try {
      channel.shutdown();
      if (!channel.awaitTermination(timeout, unit)) {
        channel.shutdownNow();
        channel.awaitTermination(1, TimeUnit.SECONDS);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      channel.shutdownNow();
    } finally {
      if (group != null) {
        group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static ManagedChannel buildChannel(
      String socketPath, int threadsOverride, boolean verbose) {
    if (verbose) {
      log("======================================================================");
      log("BUILDING TUNED NETTY gRPC CHANNEL OVER UNIX DOMAIN SOCKET");
      log("======================================================================");
      log("Target UDS Socket Address : " + socketPath);
    }

    String nettySocketPath =
        socketPath.startsWith("@") ? ("\0" + socketPath.substring(1)) : socketPath;
    DomainSocketAddress address = new DomainSocketAddress(nettySocketPath);
    NettyChannelBuilder builder = NettyChannelBuilder.forAddress(address);

    boolean epollAvailable = Epoll.isAvailable();
    if (verbose) {
      log("Checking transport layer capabilities...");
      log("  - Epoll.isAvailable()   : " + epollAvailable);
    }

    EventLoopGroup group;
    if (epollAvailable) {
      int nThreads =
          threadsOverride > 0
              ? threadsOverride
              : Math.min(8, Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
      group =
          new EpollEventLoopGroup(
              nThreads,
              new DefaultThreadFactory(verbose ? "gcs-uds-epoll" : "gcs-uds-probe", true));
      builder
          .channelType(EpollDomainSocketChannel.class)
          .eventLoopGroup(group)
          .withOption(ChannelOption.SO_RCVBUF, SOCKET_BUFFER_SIZE)
          .withOption(ChannelOption.SO_SNDBUF, SOCKET_BUFFER_SIZE);
      if (verbose) {
        log("  - Selected Channel Type : EpollDomainSocketChannel.class (Linux native epoll)");
        log("  - Selected EventLoop    : EpollEventLoopGroup (" + nThreads + " daemon threads)");
        log(
            String.format(
                "  - ChannelOption.SO_RCVBUF : %,d bytes (8 MiB kernel buffer)",
                SOCKET_BUFFER_SIZE));
        log(
            String.format(
                "  - ChannelOption.SO_SNDBUF : %,d bytes (8 MiB kernel buffer)",
                SOCKET_BUFFER_SIZE));
      }
    } else {
      try {
        Class<?> kqueueClass =
            Class.forName("io.grpc.netty.shaded.io.netty.channel.kqueue.KQueue");
        boolean isKQueueAvailable = (boolean) kqueueClass.getMethod("isAvailable").invoke(null);
        if (isKQueueAvailable) {
          Class<?> channelClass =
              Class.forName(
                  "io.grpc.netty.shaded.io.netty.channel.kqueue.KQueueDomainSocketChannel");
          Class<?> groupClass =
              Class.forName("io.grpc.netty.shaded.io.netty.channel.kqueue.KQueueEventLoopGroup");
          int nThreads = threadsOverride > 0 ? threadsOverride : 2;
          group =
              (EventLoopGroup)
                  groupClass
                      .getConstructor(int.class, java.util.concurrent.ThreadFactory.class)
                      .newInstance(
                          nThreads,
                          new DefaultThreadFactory(
                              verbose ? "gcs-uds-kqueue" : "gcs-uds-probe", true));
          builder
              .channelType((Class) channelClass)
              .eventLoopGroup(group)
              .withOption(ChannelOption.SO_RCVBUF, SOCKET_BUFFER_SIZE)
              .withOption(ChannelOption.SO_SNDBUF, SOCKET_BUFFER_SIZE);
        } else {
          throw new UnsupportedOperationException(
              "Epoll not available and KQueue not available.");
        }
      } catch (ReflectiveOperationException e) {
        String err =
            "Unix Domain Sockets require Epoll (Linux) or KQueue (macOS). Neither is available.";
        if (verbose) {
          log("FATAL: " + err);
        }
        throw new UnsupportedOperationException(err, e);
      }
    }

    if (verbose) {
      log("Configuring Netty gRPC channel tuning parameters:");
      log("  - overrideAuthority     : 'localhost'");
      log("  - security / credentials : usePlaintext() (kernel-isolated IPC)");
      log("  - executor model        : directExecutor()");
      log(
          String.format(
              "  - initialFlowControlWindow: %,d bytes (64 MiB HTTP/2 stream flow window)",
              FLOW_CONTROL_WINDOW));
      log(
          String.format(
              "  - maxInboundMessageSize : %,d bytes (64 MiB max message size)", MAX_MESSAGE_SIZE));
      log(
          String.format(
              "  - maxInboundMetadataSize: %,d bytes (64 KiB header limit)", MAX_METADATA_SIZE));
    }

    ManagedChannel channel =
        builder
            .overrideAuthority("localhost")
            .usePlaintext()
            .directExecutor()
            .initialFlowControlWindow(FLOW_CONTROL_WINDOW)
            .maxInboundMessageSize(MAX_MESSAGE_SIZE)
            .maxInboundMetadataSize(MAX_METADATA_SIZE)
            .build();
    CHANNEL_GROUPS.put(channel, group);

    if (verbose) {
      log("Tuned channel successfully initialized and ready for RPC invocation.");
      log("======================================================================");
    }
    return channel;
  }
}
