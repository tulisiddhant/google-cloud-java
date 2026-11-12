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

import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import io.grpc.netty.shaded.io.netty.channel.ChannelOption;
import io.grpc.netty.shaded.io.netty.channel.EventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.epoll.Epoll;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollDomainSocketChannel;
import io.grpc.netty.shaded.io.netty.channel.epoll.EpollEventLoopGroup;
import io.grpc.netty.shaded.io.netty.channel.unix.DomainSocketAddress;
import io.grpc.netty.shaded.io.netty.util.concurrent.DefaultThreadFactory;
import java.nio.file.Path;

/**
 * Creates tuned Netty gRPC ManagedChannels over Unix Domain Sockets (UDS) aligned with
 * the high-throughput GCS Rust core sidecar (PR #7350 / gcs-core-server).
 *
 * Optimizations applied:
 * 1. Linux Epoll native domain socket transport (EpollDomainSocketChannel) with daemon event loop threads.
 * 2. 8 MiB kernel socket buffer options (SO_RCVBUF and SO_SNDBUF) matching sidecar setsockopt.
 * 3. 64 MiB HTTP/2 stream flow control window to eliminate gRPC backpressure stalls on large transfers.
 * 4. 64 MiB maximum inbound message size matching sidecar encoding/decoding caps.
 * 5. directExecutor() dispatch to eliminate thread hop overhead on UDS chunk callbacks.
 * 6. overrideAuthority("localhost") to prevent HTTP/2 RFC 7540 PROTOCOL_ERROR over domain sockets.
 */
public final class UdsChannelFactory {

  public static final int SOCKET_BUFFER_SIZE = 8 * 1024 * 1024;    // 8 MiB SO_RCVBUF / SO_SNDBUF
  public static final int FLOW_CONTROL_WINDOW = 2 * 1024 * 1024;   // 2 MiB HTTP/2 stream window
  public static final int MAX_MESSAGE_SIZE = 64 * 1024 * 1024;     // 64 MiB max gRPC frame/message
  public static final int MAX_METADATA_SIZE = 64 * 1024;           // 64 KiB header limit

  private UdsChannelFactory() {}

  private static void log(String message) {
    System.out.println("[UdsChannelFactory] " + message);
  }

  public static ManagedChannel createChannel(Path socketPath) {
    return createChannel(socketPath.toAbsolutePath().toString());
  }

  public static ManagedChannel createChannel(String socketPath) {
    log("======================================================================");
    log("BUILDING TUNED NETTY gRPC CHANNEL OVER UNIX DOMAIN SOCKET");
    log("======================================================================");
    log("Target UDS Socket Address : " + socketPath);

    DomainSocketAddress address = new DomainSocketAddress(socketPath);
    NettyChannelBuilder builder = NettyChannelBuilder.forAddress(address);

    log("Checking transport layer capabilities...");
    boolean epollAvailable = Epoll.isAvailable();
    log("  - Epoll.isAvailable()   : " + epollAvailable);

    if (epollAvailable) {
      log("  - Selected Channel Type : EpollDomainSocketChannel.class (Linux native epoll)");
      log("  - Selected EventLoop    : EpollEventLoopGroup (daemon threads: 'gcs-uds-epoll')");
      int nThreads = Math.min(4, Math.max(2, Runtime.getRuntime().availableProcessors() / 2));
      EventLoopGroup group = new EpollEventLoopGroup(
          nThreads, new DefaultThreadFactory("gcs-uds-epoll", true));
      builder
          .channelType(EpollDomainSocketChannel.class)
          .eventLoopGroup(group)
          .withOption(ChannelOption.SO_RCVBUF, SOCKET_BUFFER_SIZE)
          .withOption(ChannelOption.SO_SNDBUF, SOCKET_BUFFER_SIZE);
      log(String.format("  - ChannelOption.SO_RCVBUF : %,d bytes (8 MiB kernel buffer)", SOCKET_BUFFER_SIZE));
      log(String.format("  - ChannelOption.SO_SNDBUF : %,d bytes (8 MiB kernel buffer)", SOCKET_BUFFER_SIZE));
    } else {
      log("  - Epoll not available: " + Epoll.unavailabilityCause());
      log("  - Checking macOS KQueue fallback via reflection...");
      try {
        Class<?> kqueueClass = Class.forName("io.grpc.netty.shaded.io.netty.channel.kqueue.KQueue");
        boolean isKQueueAvailable = (boolean) kqueueClass.getMethod("isAvailable").invoke(null);
        log("  - KQueue.isAvailable()  : " + isKQueueAvailable);
        if (isKQueueAvailable) {
          Class<?> channelClass =
              Class.forName("io.grpc.netty.shaded.io.netty.channel.kqueue.KQueueDomainSocketChannel");
          Class<?> groupClass =
              Class.forName("io.grpc.netty.shaded.io.netty.channel.kqueue.KQueueEventLoopGroup");
          EventLoopGroup group = (EventLoopGroup) groupClass
              .getConstructor(int.class, java.util.concurrent.ThreadFactory.class)
              .newInstance(2, new DefaultThreadFactory("gcs-uds-kqueue", true));
          log("  - Selected Channel Type : KQueueDomainSocketChannel.class (macOS native kqueue)");
          builder
              .channelType((Class) channelClass)
              .eventLoopGroup(group)
              .withOption(ChannelOption.SO_RCVBUF, SOCKET_BUFFER_SIZE)
              .withOption(ChannelOption.SO_SNDBUF, SOCKET_BUFFER_SIZE);
        } else {
          throw new UnsupportedOperationException("Epoll not available and KQueue not available.");
        }
      } catch (ReflectiveOperationException e) {
        String err = "Unix Domain Sockets require Epoll (Linux) or KQueue (macOS). Neither is available on this system.";
        log("FATAL: " + err);
        throw new UnsupportedOperationException(err, e);
      }
    }

    log("Configuring Netty gRPC channel tuning parameters:");
    log("  - overrideAuthority     : 'localhost' (prevents HTTP/2 RFC 7540 PROTOCOL_ERROR over UDS)");
    log("  - security / credentials : usePlaintext() (kernel-isolated IPC, zero TLS handshake overhead)");
    log("  - executor model        : directExecutor() (eliminates thread hop on chunk callbacks)");
    log(String.format("  - initialFlowControlWindow: %,d bytes (2 MiB HTTP/2 stream flow window)", FLOW_CONTROL_WINDOW));
    log(String.format("  - maxInboundMessageSize : %,d bytes (64 MiB max message size)", MAX_MESSAGE_SIZE));
    log(String.format("  - maxInboundMetadataSize: %,d bytes (64 KiB header limit)", MAX_METADATA_SIZE));

    ManagedChannel channel =
        builder
            .overrideAuthority("localhost")
            .usePlaintext()
            .directExecutor()
            .initialFlowControlWindow(FLOW_CONTROL_WINDOW)
            .maxInboundMessageSize(MAX_MESSAGE_SIZE)
            .maxInboundMetadataSize(MAX_METADATA_SIZE)
            .build();

    log("Tuned channel successfully initialized and ready for RPC invocation.");
    log("======================================================================");
    return channel;
  }
}
