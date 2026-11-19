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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.storage.v2.AppendObjectSpec;
import com.google.storage.v2.BidiReadObjectRequest;
import com.google.storage.v2.BidiReadObjectResponse;
import com.google.storage.v2.BidiReadObjectSpec;
import com.google.storage.v2.BidiWriteHandle;
import com.google.storage.v2.BidiWriteObjectRequest;
import com.google.storage.v2.BidiWriteObjectResponse;
import com.google.storage.v2.Bucket;
import com.google.storage.v2.ChecksummedData;
import com.google.storage.v2.DeleteObjectRequest;
import com.google.storage.v2.GetObjectRequest;
import com.google.storage.v2.ListBucketsRequest;
import com.google.storage.v2.ListBucketsResponse;
import com.google.storage.v2.ListObjectsRequest;
import com.google.storage.v2.ListObjectsResponse;
import com.google.storage.v2.ObjectChecksums;
import com.google.storage.v2.ObjectRangeData;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.ReadObjectResponse;
import com.google.storage.v2.ReadRange;
import com.google.storage.v2.StartResumableWriteRequest;
import com.google.storage.v2.StartResumableWriteResponse;
import com.google.storage.v2.StorageGrpc.StorageImplBase;
import com.google.storage.v2.WriteObjectRequest;
import com.google.storage.v2.WriteObjectResponse;
import com.google.storage.v2.WriteObjectSpec;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Dual-protocol (gRPC v2 {@link StorageImplBase} + HTTP/1.1 JSON {@link HttpServer}) local testbench
 * sharing a consistent in-memory object and bucket registry.
 *
 * <p>Enables hermetic end-to-end verification of {@code Native Java}, {@code uds-sidecar}, and
 * {@code uds-inproc} across both {@code gRPC} and {@code JSON} transports without dummy-data
 * fallbacks.
 */
public final class MockStorageGrpcServer implements AutoCloseable {

  public static final class StoredObject {
    public final String bucket;
    public final String name;
    public final byte[] data; // null when backed by a large virtual zero-filled size
    public final long size;
    public final long generation;
    public final long metageneration;
    public final String contentType;
    public final int crc32c;
    public final boolean finalized;

    StoredObject(
        String bucket,
        String name,
        byte[] data,
        long size,
        long generation,
        long metageneration,
        String contentType,
        int crc32c,
        boolean finalized) {
      this.bucket = bucket;
      this.name = name;
      this.data = data;
      this.size = size;
      this.generation = generation;
      this.metageneration = metageneration;
      this.contentType =
          (contentType != null && !contentType.isEmpty())
              ? contentType
              : "application/octet-stream";
      this.crc32c = crc32c;
      this.finalized = finalized;
    }
  }

  private static final int STREAM_SLICE_BYTES = 256 * 1024;
  private static final byte[] ZERO_SLICE = new byte[STREAM_SLICE_BYTES];

  private final ConcurrentHashMap<String, StoredObject> objects =
      new ConcurrentHashMap<String, StoredObject>();
  private final ConcurrentSkipListSet<String> buckets = new ConcurrentSkipListSet<String>();
  private final ConcurrentHashMap<String, WriteObjectSpec> resumableSessions =
      new ConcurrentHashMap<String, WriteObjectSpec>();
  private final AtomicLong nextGen = new AtomicLong(100L);
  private final AtomicInteger abortedHttpReads = new AtomicInteger(0);
  private final AtomicInteger cancelledGrpcReads = new AtomicInteger(0);

  private Server grpcServer;
  private HttpServer httpServer;

  public void start() throws IOException {
    grpcServer =
        NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0))
            .maxInboundMessageSize(64 * 1024 * 1024)
            .addService(new StorageV2Impl())
            .build()
            .start();

    httpServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    httpServer.setExecutor(Executors.newCachedThreadPool());
    registerHttpHandlers();
    httpServer.start();
  }

  public String getGrpcEndpoint() {
    return "http://127.0.0.1:" + grpcServer.getPort();
  }

  public String getHttpEndpoint() {
    return "http://127.0.0.1:" + httpServer.getAddress().getPort();
  }

  public int getAbortedHttpReads() {
    return abortedHttpReads.get();
  }

  public int getCancelledGrpcReads() {
    return cancelledGrpcReads.get();
  }

  public void addBucket(String bucket) {
    buckets.add(stripBucketPrefix(bucket));
  }

  public StoredObject putBytes(String bucket, String name, byte[] bytes, String contentType) {
    String cleanBucket = stripBucketPrefix(bucket);
    buckets.add(cleanBucket);
    int crc = Hashing.crc32c().hashBytes(bytes).asInt();
    long gen = nextGen.incrementAndGet();
    StoredObject obj =
        new StoredObject(
            cleanBucket, name, bytes.clone(), bytes.length, gen, 1L, contentType, crc, true);
    objects.put(objectKey(cleanBucket, name), obj);
    return obj;
  }

  public StoredObject putVirtualObject(String bucket, String name, long size) {
    String cleanBucket = stripBucketPrefix(bucket);
    buckets.add(cleanBucket);
    long gen = nextGen.incrementAndGet();
    StoredObject obj =
        new StoredObject(
            cleanBucket, name, null, size, gen, 1L, "application/octet-stream", 0, true);
    objects.put(objectKey(cleanBucket, name), obj);
    return obj;
  }

  public StoredObject getStoredObject(String bucket, String name) {
    return objects.get(objectKey(stripBucketPrefix(bucket), name));
  }

  private static String stripBucketPrefix(String bucket) {
    if (bucket == null) {
      return "";
    }
    if (bucket.startsWith("projects/")) {
      int idx = bucket.lastIndexOf("/buckets/");
      if (idx >= 0) {
        return bucket.substring(idx + "/buckets/".length());
      }
    }
    return bucket;
  }

  private static String toV2Bucket(String cleanBucket) {
    return "projects/_/buckets/" + cleanBucket;
  }

  private static String objectKey(String cleanBucket, String name) {
    return cleanBucket + "\0" + name;
  }

  private static ByteString sliceObject(StoredObject obj, long offset, int length) {
    if (length <= 0) {
      return ByteString.EMPTY;
    }
    if (obj.data != null) {
      int start = (int) Math.min(offset, obj.data.length);
      int count = (int) Math.min(length, obj.data.length - start);
      return ByteString.copyFrom(obj.data, start, Math.max(0, count));
    }
    return ByteString.copyFrom(ZERO_SLICE, 0, Math.min(length, ZERO_SLICE.length));
  }

  private static com.google.storage.v2.Object toProtoObject(StoredObject s) {
    com.google.storage.v2.Object.Builder b =
        com.google.storage.v2.Object.newBuilder()
            .setBucket(toV2Bucket(s.bucket))
            .setName(s.name)
            .setSize(s.size)
            .setGeneration(s.generation)
            .setMetageneration(s.metageneration)
            .setContentType(s.contentType)
            .setStorageClass("STANDARD")
            .setEtag("etag-" + s.generation)
            .setChecksums(ObjectChecksums.newBuilder().setCrc32C(s.crc32c).build());
    if (s.finalized) {
      b.setFinalizeTime(Timestamp.newBuilder().setSeconds(1700000000L).build());
    }
    return b.build();
  }

  private static Status checkGenerationMatch(StoredObject existing, boolean hasCond, long cond) {
    if (!hasCond) {
      return null;
    }
    if (cond == 0L) {
      if (existing != null) {
        return Status.FAILED_PRECONDITION.withDescription("Precondition ifGenerationMatch=0 failed");
      }
      return null;
    }
    if (existing == null || existing.generation != cond) {
      return Status.FAILED_PRECONDITION.withDescription(
          "Precondition ifGenerationMatch=" + cond + " failed");
    }
    return null;
  }

  // =========================================================================
  // gRPC google.storage.v2.Storage Implementation
  // =========================================================================

  private final class StorageV2Impl extends StorageImplBase {

    @Override
    public void getObject(
        GetObjectRequest request, StreamObserver<com.google.storage.v2.Object> responseObserver) {
      String cleanBucket = stripBucketPrefix(request.getBucket());
      StoredObject stored = objects.get(objectKey(cleanBucket, request.getObject()));
      if (stored == null) {
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("No such object: " + cleanBucket + "/" + request.getObject())
                .asRuntimeException());
        return;
      }
      Status preErr =
          checkGenerationMatch(
              stored, request.hasIfGenerationMatch(), request.getIfGenerationMatch());
      if (preErr != null) {
        responseObserver.onError(preErr.asRuntimeException());
        return;
      }
      responseObserver.onNext(toProtoObject(stored));
      responseObserver.onCompleted();
    }

    @Override
    public void deleteObject(DeleteObjectRequest request, StreamObserver<Empty> responseObserver) {
      String cleanBucket = stripBucketPrefix(request.getBucket());
      String key = objectKey(cleanBucket, request.getObject());
      StoredObject existing = objects.get(key);
      if (existing == null) {
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("No such object: " + cleanBucket + "/" + request.getObject())
                .asRuntimeException());
        return;
      }
      Status preErr =
          checkGenerationMatch(
              existing, request.hasIfGenerationMatch(), request.getIfGenerationMatch());
      if (preErr != null) {
        responseObserver.onError(preErr.asRuntimeException());
        return;
      }
      objects.remove(key);
      responseObserver.onNext(Empty.getDefaultInstance());
      responseObserver.onCompleted();
    }

    @Override
    public void listObjects(
        ListObjectsRequest request, StreamObserver<ListObjectsResponse> responseObserver) {
      String cleanBucket = stripBucketPrefix(request.getParent());
      if (!buckets.contains(cleanBucket)) {
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("No such bucket: " + cleanBucket)
                .asRuntimeException());
        return;
      }

      String prefix = request.getPrefix();
      String delimiter = request.getDelimiter();
      String startOffset = request.getLexicographicStart();
      String endOffset = request.getLexicographicEnd();

      List<StoredObject> matching = new ArrayList<StoredObject>();
      Set<String> prefixes = new TreeSet<String>();
      String keyPrefix = cleanBucket + "\0";
      for (Map.Entry<String, StoredObject> entry : objects.entrySet()) {
        if (!entry.getKey().startsWith(keyPrefix)) {
          continue;
        }
        StoredObject obj = entry.getValue();
        if (!prefix.isEmpty() && !obj.name.startsWith(prefix)) {
          continue;
        }
        if (!startOffset.isEmpty() && obj.name.compareTo(startOffset) < 0) {
          continue;
        }
        if (!endOffset.isEmpty() && obj.name.compareTo(endOffset) >= 0) {
          continue;
        }
        if (!delimiter.isEmpty()) {
          int delimIdx = obj.name.indexOf(delimiter, prefix.length());
          if (delimIdx >= 0) {
            prefixes.add(obj.name.substring(0, delimIdx + delimiter.length()));
            continue;
          }
        }
        matching.add(obj);
      }
      Collections.sort(
          matching,
          new java.util.Comparator<StoredObject>() {
            @Override
            public int compare(StoredObject a, StoredObject b) {
              return a.name.compareTo(b.name);
            }
          });

      int startIdx = 0;
      if (!request.getPageToken().isEmpty()) {
        try {
          startIdx = Integer.parseInt(request.getPageToken());
        } catch (NumberFormatException ignored) {
          startIdx = 0;
        }
      }
      int pageSize = request.getPageSize() > 0 ? request.getPageSize() : matching.size();
      int endIdx = Math.min(matching.size(), startIdx + Math.max(1, pageSize));

      ListObjectsResponse.Builder rb = ListObjectsResponse.newBuilder();
      if (startIdx == 0) {
        rb.addAllPrefixes(prefixes);
      }
      for (int i = startIdx; i < endIdx; i++) {
        rb.addObjects(toProtoObject(matching.get(i)));
      }
      if (endIdx < matching.size()) {
        rb.setNextPageToken(String.valueOf(endIdx));
      }
      responseObserver.onNext(rb.build());
      responseObserver.onCompleted();
    }

    @Override
    public void listBuckets(
        ListBucketsRequest request, StreamObserver<ListBucketsResponse> responseObserver) {
      String prefix = request.getPrefix();
      List<String> matching = new ArrayList<String>();
      for (String b : buckets) {
        if (prefix.isEmpty() || b.startsWith(prefix)) {
          matching.add(b);
        }
      }
      int startIdx = 0;
      if (!request.getPageToken().isEmpty()) {
        try {
          startIdx = Integer.parseInt(request.getPageToken());
        } catch (NumberFormatException ignored) {
          startIdx = 0;
        }
      }
      int pageSize = request.getPageSize() > 0 ? request.getPageSize() : matching.size();
      int endIdx = Math.min(matching.size(), startIdx + Math.max(1, pageSize));

      ListBucketsResponse.Builder rb = ListBucketsResponse.newBuilder();
      for (int i = startIdx; i < endIdx; i++) {
        String name = matching.get(i);
        rb.addBuckets(
            Bucket.newBuilder()
                .setName(toV2Bucket(name))
                .setBucketId(name)
                .setLocation("US-WEST1")
                .setStorageClass("STANDARD")
                .setMetageneration(1L)
                .build());
      }
      if (endIdx < matching.size()) {
        rb.setNextPageToken(String.valueOf(endIdx));
      }
      responseObserver.onNext(rb.build());
      responseObserver.onCompleted();
    }

    @Override
    public void readObject(
        ReadObjectRequest request, StreamObserver<ReadObjectResponse> responseObserver) {
      final ServerCallStreamObserver<ReadObjectResponse> srvObs =
          (ServerCallStreamObserver<ReadObjectResponse>) responseObserver;
      srvObs.setOnCancelHandler(
          new Runnable() {
            @Override
            public void run() {
              cancelledGrpcReads.incrementAndGet();
            }
          });

      String cleanBucket = stripBucketPrefix(request.getBucket());
      StoredObject stored = objects.get(objectKey(cleanBucket, request.getObject()));
      if (stored == null) {
        responseObserver.onError(
            Status.NOT_FOUND
                .withDescription("No such object: " + cleanBucket + "/" + request.getObject())
                .asRuntimeException());
        return;
      }
      Status preErr =
          checkGenerationMatch(
              stored, request.hasIfGenerationMatch(), request.getIfGenerationMatch());
      if (preErr != null) {
        responseObserver.onError(preErr.asRuntimeException());
        return;
      }

      long offset = request.getReadOffset();
      if (offset < 0) {
        offset = Math.max(0L, stored.size + offset);
      }
      long remaining =
          request.getReadLimit() > 0
              ? Math.min(request.getReadLimit(), Math.max(0L, stored.size - offset))
              : Math.max(0L, stored.size - offset);

      boolean first = true;
      long cursor = offset;
      while (remaining > 0 || first) {
        if (srvObs.isCancelled()) {
          return;
        }
        int step = (int) Math.min(STREAM_SLICE_BYTES, remaining);
        ByteString data = sliceObject(stored, cursor, step);
        int sliceCrc = Hashing.crc32c().hashBytes(data.toByteArray()).asInt();
        ReadObjectResponse.Builder rb =
            ReadObjectResponse.newBuilder()
                .setChecksummedData(
                    ChecksummedData.newBuilder().setContent(data).setCrc32C(sliceCrc).build());
        if (first) {
          rb.setMetadata(toProtoObject(stored));
          first = false;
        }
        responseObserver.onNext(rb.build());
        cursor += step;
        remaining -= step;
        if (remaining <= 0) {
          break;
        }
      }
      if (!srvObs.isCancelled()) {
        responseObserver.onCompleted();
      }
    }

    @Override
    public StreamObserver<BidiReadObjectRequest> bidiReadObject(
        final StreamObserver<BidiReadObjectResponse> responseObserver) {
      final ServerCallStreamObserver<BidiReadObjectResponse> srvObs =
          (ServerCallStreamObserver<BidiReadObjectResponse>) responseObserver;
      return new StreamObserver<BidiReadObjectRequest>() {
        private StoredObject opened;

        @Override
        public void onNext(BidiReadObjectRequest req) {
          if (req.hasReadObjectSpec()) {
            BidiReadObjectSpec spec = req.getReadObjectSpec();
            String cleanBucket = stripBucketPrefix(spec.getBucket());
            StoredObject stored = objects.get(objectKey(cleanBucket, spec.getObject()));
            if (stored == null) {
              responseObserver.onError(
                  Status.NOT_FOUND
                      .withDescription("No such object: " + cleanBucket + "/" + spec.getObject())
                      .asRuntimeException());
              return;
            }
            Status preErr =
                checkGenerationMatch(
                    stored, spec.hasIfGenerationMatch(), spec.getIfGenerationMatch());
            if (preErr != null) {
              responseObserver.onError(preErr.asRuntimeException());
              return;
            }
            this.opened = stored;
            if (req.getReadRangesCount() == 0) {
              responseObserver.onNext(
                  BidiReadObjectResponse.newBuilder()
                      .setMetadata(toProtoObject(stored))
                      .build());
              return;
            }
          }

          if (opened == null) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT
                    .withDescription("Missing ReadObjectSpec")
                    .asRuntimeException());
            return;
          }

          boolean sendMeta = req.hasReadObjectSpec();
          for (ReadRange rr : req.getReadRangesList()) {
            long start = rr.getReadOffset();
            if (start < 0) {
              start = Math.max(0L, opened.size + start);
            }
            long rem =
                rr.getReadLength() > 0
                    ? Math.min(rr.getReadLength(), Math.max(0L, opened.size - start))
                    : Math.max(0L, opened.size - start);
            long cur = start;
            do {
              if (srvObs.isCancelled()) {
                return;
              }
              int step = (int) Math.min(STREAM_SLICE_BYTES, rem);
              ByteString bytes = sliceObject(opened, cur, step);
              int sliceCrc = Hashing.crc32c().hashBytes(bytes.toByteArray()).asInt();
              boolean isEnd = (rem - step) <= 0;
              ObjectRangeData rangeData =
                  ObjectRangeData.newBuilder()
                      .setChecksummedData(
                          ChecksummedData.newBuilder()
                              .setContent(bytes)
                              .setCrc32C(sliceCrc)
                              .build())
                      .setReadRange(
                          ReadRange.newBuilder()
                              .setReadId(rr.getReadId())
                              .setReadOffset(cur)
                              .setReadLength(step)
                              .build())
                      .setRangeEnd(isEnd)
                      .build();
              BidiReadObjectResponse.Builder resp =
                  BidiReadObjectResponse.newBuilder().addObjectDataRanges(rangeData);
              if (sendMeta) {
                resp.setMetadata(toProtoObject(opened));
                sendMeta = false;
              }
              responseObserver.onNext(resp.build());
              cur += step;
              rem -= step;
            } while (rem > 0);
          }
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {
          if (!srvObs.isCancelled()) {
            responseObserver.onCompleted();
          }
        }
      };
    }

    @Override
    public void startResumableWrite(
        StartResumableWriteRequest request,
        StreamObserver<StartResumableWriteResponse> responseObserver) {
      String uploadId = "upload-" + nextGen.incrementAndGet();
      resumableSessions.put(uploadId, request.getWriteObjectSpec());
      responseObserver.onNext(
          StartResumableWriteResponse.newBuilder().setUploadId(uploadId).build());
      responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<WriteObjectRequest> writeObject(
        final StreamObserver<WriteObjectResponse> responseObserver) {
      return new StreamObserver<WriteObjectRequest>() {
        private String cleanBucket;
        private String objectName;
        private String contentType = "application/octet-stream";
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        private final Hasher crcHasher = Hashing.crc32c().newHasher();
        private long totalWritten = 0;
        private boolean keepBytes = true;

        @Override
        public void onNext(WriteObjectRequest req) {
          WriteObjectSpec spec = null;
          if (req.hasWriteObjectSpec()) {
            spec = req.getWriteObjectSpec();
          } else if (!req.getUploadId().isEmpty()) {
            spec = resumableSessions.get(req.getUploadId());
          }
          if (spec != null && cleanBucket == null) {
            cleanBucket = stripBucketPrefix(spec.getResource().getBucket());
            objectName = spec.getResource().getName();
            if (!spec.getResource().getContentType().isEmpty()) {
              contentType = spec.getResource().getContentType();
            }
            StoredObject existing = objects.get(objectKey(cleanBucket, objectName));
            Status preErr =
                checkGenerationMatch(
                    existing, spec.hasIfGenerationMatch(), spec.getIfGenerationMatch());
            if (preErr != null) {
              responseObserver.onError(preErr.asRuntimeException());
              return;
            }
          }
          if (req.hasChecksummedData()) {
            byte[] chunk = req.getChecksummedData().getContent().toByteArray();
            crcHasher.putBytes(chunk);
            totalWritten += chunk.length;
            if (keepBytes && totalWritten <= 32L * 1024 * 1024) {
              buf.write(chunk, 0, chunk.length);
            } else {
              keepBytes = false;
              buf.reset();
            }
          }
          if (req.getFinishWrite()) {
            if (cleanBucket == null) {
              cleanBucket = "mock-bucket";
              objectName = "mock-obj";
            }
            buckets.add(cleanBucket);
            long gen = nextGen.incrementAndGet();
            int crc = crcHasher.hash().asInt();
            StoredObject stored =
                new StoredObject(
                    cleanBucket,
                    objectName,
                    keepBytes ? buf.toByteArray() : null,
                    totalWritten,
                    gen,
                    1L,
                    contentType,
                    crc,
                    true);
            objects.put(objectKey(cleanBucket, objectName), stored);
            responseObserver.onNext(
                WriteObjectResponse.newBuilder().setResource(toProtoObject(stored)).build());
          }
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }

    @Override
    public StreamObserver<BidiWriteObjectRequest> bidiWriteObject(
        final StreamObserver<BidiWriteObjectResponse> responseObserver) {
      return new StreamObserver<BidiWriteObjectRequest>() {
        private String cleanBucket;
        private String objectName;
        private String contentType = "application/octet-stream";
        private long generation;
        private final ByteArrayOutputStream buf = new ByteArrayOutputStream();
        private long totalWritten = 0;
        private boolean keepBytes = true;
        private Hasher crcHasher = Hashing.crc32c().newHasher();
        private boolean failed = false;

        @Override
        public void onNext(BidiWriteObjectRequest req) {
          if (failed) {
            return;
          }
          boolean isFirstSpec = false;
          if (req.hasWriteObjectSpec()) {
            isFirstSpec = true;
            WriteObjectSpec spec = req.getWriteObjectSpec();
            cleanBucket = stripBucketPrefix(spec.getResource().getBucket());
            objectName = spec.getResource().getName();
            if (!spec.getResource().getContentType().isEmpty()) {
              contentType = spec.getResource().getContentType();
            }
            StoredObject existing = objects.get(objectKey(cleanBucket, objectName));
            Status preErr =
                checkGenerationMatch(
                    existing, spec.hasIfGenerationMatch(), spec.getIfGenerationMatch());
            if (preErr != null) {
              failed = true;
              responseObserver.onError(preErr.asRuntimeException());
              return;
            }
            buckets.add(cleanBucket);
            generation = nextGen.incrementAndGet();
          } else if (req.hasAppendObjectSpec()) {
            isFirstSpec = true;
            AppendObjectSpec appSpec = req.getAppendObjectSpec();
            cleanBucket = stripBucketPrefix(appSpec.getBucket());
            objectName = appSpec.getObject();
            generation = appSpec.getGeneration();
            StoredObject existing = objects.get(objectKey(cleanBucket, objectName));
            if (existing == null || existing.generation != generation) {
              failed = true;
              responseObserver.onError(
                  Status.NOT_FOUND
                      .withDescription("Appendable object generation not found: " + generation)
                      .asRuntimeException());
              return;
            }
            contentType = existing.contentType;
            totalWritten = existing.size;
            if (existing.data != null) {
              buf.write(existing.data, 0, existing.data.length);
              crcHasher.putBytes(existing.data);
            } else {
              keepBytes = false;
            }
          }

          if (req.hasChecksummedData()) {
            byte[] chunk = req.getChecksummedData().getContent().toByteArray();
            crcHasher.putBytes(chunk);
            totalWritten += chunk.length;
            if (keepBytes && totalWritten <= 32L * 1024 * 1024) {
              buf.write(chunk, 0, chunk.length);
            } else {
              keepBytes = false;
              buf.reset();
            }
          }

          BidiWriteHandle handle =
              BidiWriteHandle.newBuilder()
                  .setHandle(ByteString.copyFromUtf8("wh-" + generation))
                  .build();

          if (req.getFinishWrite()) {
            int finalCrc = crcHasher.hash().asInt();
            if (req.hasObjectChecksums() && req.getObjectChecksums().hasCrc32C()) {
              int expected = req.getObjectChecksums().getCrc32C();
              if (expected != finalCrc) {
                failed = true;
                responseObserver.onError(
                    Status.INVALID_ARGUMENT
                        .withDescription("CRC32C mismatch on BidiWriteObject finalize")
                        .asRuntimeException());
                return;
              }
            }
            StoredObject finalizedObj =
                new StoredObject(
                    cleanBucket,
                    objectName,
                    keepBytes ? buf.toByteArray() : null,
                    totalWritten,
                    generation,
                    1L,
                    contentType,
                    finalCrc,
                    true);
            objects.put(objectKey(cleanBucket, objectName), finalizedObj);
            responseObserver.onNext(
                BidiWriteObjectResponse.newBuilder()
                    .setResource(toProtoObject(finalizedObj))
                    .setWriteHandle(handle)
                    .build());
            return;
          }

          // Persist intermediate state for unfinalized close / reopen takeover
          StoredObject partial =
              new StoredObject(
                  cleanBucket,
                  objectName,
                  keepBytes ? buf.toByteArray() : null,
                  totalWritten,
                  generation,
                  1L,
                  contentType,
                  0,
                  false);
          objects.put(objectKey(cleanBucket, objectName), partial);

          if (isFirstSpec) {
            responseObserver.onNext(
                BidiWriteObjectResponse.newBuilder()
                    .setResource(toProtoObject(partial))
                    .setWriteHandle(handle)
                    .build());
          } else if (req.getFlush() && req.getStateLookup()) {
            responseObserver.onNext(
                BidiWriteObjectResponse.newBuilder()
                    .setPersistedSize(totalWritten)
                    .setWriteHandle(handle)
                    .build());
          }
        }

        @Override
        public void onError(Throwable t) {}

        @Override
        public void onCompleted() {
          if (!failed) {
            responseObserver.onCompleted();
          }
        }
      };
    }
  }

  // =========================================================================
  // HTTP/1.1 JSON API Implementation (shares `objects` & `buckets` maps)
  // =========================================================================

  private static String urlDecode(String s) {
    if (s == null) {
      return "";
    }
    try {
      return URLDecoder.decode(s, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      return s;
    }
  }

  private static String queryParam(String rawQuery, String key) {
    if (rawQuery == null || rawQuery.isEmpty()) {
      return null;
    }
    for (String pair : rawQuery.split("&")) {
      int eq = pair.indexOf('=');
      if (eq > 0) {
        String k = urlDecode(pair.substring(0, eq));
        if (key.equals(k)) {
          return urlDecode(pair.substring(eq + 1));
        }
      }
    }
    return null;
  }

  private static void sendJsonError(HttpExchange exchange, int code, String message)
      throws IOException {
    String json =
        String.format(
            "{\"error\":{\"code\":%d,\"message\":\"%s\"}}",
            code, message.replace("\"", "'"));
    byte[] body = json.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(code, body.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(body);
    }
  }

  private void registerHttpHandlers() {
    HttpHandler storageHandler =
        new HttpHandler() {
          @Override
          public void handle(HttpExchange exchange) throws IOException {
            String rawPath = exchange.getRequestURI().getRawPath();
            String rawQuery = exchange.getRequestURI().getRawQuery();
            String method = exchange.getRequestMethod();

            // List Buckets: GET /storage/v1/b?project=...
            if ("GET".equalsIgnoreCase(method)
                && ("/storage/v1/b".equals(rawPath) || "/storage/v1/b/".equals(rawPath))) {
              String prefix = queryParam(rawQuery, "prefix");
              String maxResStr = queryParam(rawQuery, "maxResults");
              String pageToken = queryParam(rawQuery, "pageToken");
              List<String> matching = new ArrayList<String>();
              for (String b : buckets) {
                if (prefix == null || prefix.isEmpty() || b.startsWith(prefix)) {
                  matching.add(b);
                }
              }
              int startIdx = pageToken != null ? Integer.parseInt(pageToken) : 0;
              int pageSize = maxResStr != null ? Integer.parseInt(maxResStr) : matching.size();
              int endIdx = Math.min(matching.size(), startIdx + Math.max(1, pageSize));
              StringBuilder sb = new StringBuilder("{\"kind\":\"storage#buckets\",\"items\":[");
              for (int i = startIdx; i < endIdx; i++) {
                if (i > startIdx) {
                  sb.append(",");
                }
                String b = matching.get(i);
                sb.append(
                    String.format(
                        "{\"kind\":\"storage#bucket\",\"id\":\"%s\",\"name\":\"%s\",\"location\":\"US-WEST1\",\"storageClass\":\"STANDARD\"}",
                        b, b));
              }
              sb.append("]");
              if (endIdx < matching.size()) {
                sb.append(",\"nextPageToken\":\"").append(endIdx).append("\"");
              }
              sb.append("}");
              byte[] resp = sb.toString().getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().set("Content-Type", "application/json");
              exchange.sendResponseHeaders(200, resp.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
              }
              return;
            }

            // Parse /storage/v1/b/<bucket>/o[/<object>] or /download/storage/v1/b/<bucket>/o/<object>
            String trimmed =
                rawPath.startsWith("/download/") ? rawPath.substring("/download".length()) : rawPath;
            String prefixMarker = "/storage/v1/b/";
            if (!trimmed.startsWith(prefixMarker)) {
              sendJsonError(exchange, 404, "Unknown path: " + rawPath);
              return;
            }
            String rest = trimmed.substring(prefixMarker.length());
            int oIdx = rest.indexOf("/o");
            if (oIdx < 0) {
              sendJsonError(exchange, 404, "Unknown bucket path: " + rawPath);
              return;
            }
            String bucket = urlDecode(rest.substring(0, oIdx));
            String afterO = rest.substring(oIdx + 2); // "" or "/" or "/<encodedObj>"

            if ("DELETE".equalsIgnoreCase(method) && afterO.startsWith("/") && afterO.length() > 1) {
              String object = urlDecode(afterO.substring(1));
              StoredObject removed = objects.remove(objectKey(bucket, object));
              if (removed == null) {
                sendJsonError(exchange, 404, "No such object: " + bucket + "/" + object);
              } else {
                exchange.sendResponseHeaders(204, -1);
                exchange.close();
              }
              return;
            }

            if ("GET".equalsIgnoreCase(method)) {
              boolean isMedia =
                  "media".equals(queryParam(rawQuery, "alt")) || rawPath.startsWith("/download/");
              if (afterO.startsWith("/") && afterO.length() > 1) {
                String object = urlDecode(afterO.substring(1));
                StoredObject stored = objects.get(objectKey(bucket, object));
                if (stored == null) {
                  sendJsonError(exchange, 404, "No such object: " + bucket + "/" + object);
                  return;
                }
                String ifGenStr = queryParam(rawQuery, "ifGenerationMatch");
                if (ifGenStr != null) {
                  long cond = Long.parseLong(ifGenStr);
                  if (cond == 0L || stored.generation != cond) {
                    sendJsonError(exchange, 412, "Precondition Failed");
                    return;
                  }
                }
                if (!isMedia) {
                  String json =
                      String.format(
                          "{\"kind\":\"storage#object\",\"name\":\"%s\",\"bucket\":\"%s\",\"size\":\"%d\",\"generation\":\"%d\",\"metageneration\":\"1\",\"contentType\":\"%s\"}",
                          object, bucket, stored.size, stored.generation, stored.contentType);
                  byte[] resp = json.getBytes(StandardCharsets.UTF_8);
                  exchange.getResponseHeaders().set("Content-Type", "application/json");
                  exchange.sendResponseHeaders(200, resp.length);
                  try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                  }
                  return;
                }

                // Media download
                long startOffset = 0;
                long requestedLength = stored.size;
                String rangeHeader = exchange.getRequestHeaders().getFirst("Range");
                if (rangeHeader != null && rangeHeader.startsWith("bytes=")) {
                  String spec = rangeHeader.substring(6);
                  int dash = spec.indexOf('-');
                  if (dash == 0) {
                    long tail = Long.parseLong(spec.substring(1));
                    startOffset = Math.max(0L, stored.size - tail);
                    requestedLength = stored.size - startOffset;
                  } else if (dash > 0) {
                    startOffset = Long.parseLong(spec.substring(0, dash));
                    String endStr = spec.substring(dash + 1);
                    if (!endStr.isEmpty()) {
                      long endOffset = Math.min(stored.size - 1, Long.parseLong(endStr));
                      requestedLength = Math.max(0L, endOffset - startOffset + 1);
                    } else {
                      requestedLength = Math.max(0L, stored.size - startOffset);
                    }
                  }
                }

                if (stored.size > 0 && rangeHeader != null && startOffset >= stored.size) {
                  exchange.getResponseHeaders().set("Content-Range", "bytes */" + stored.size);
                  exchange.sendResponseHeaders(416, -1);
                  exchange.close();
                  return;
                }

                exchange.getResponseHeaders().set("Content-Type", stored.contentType);
                exchange
                    .getResponseHeaders()
                    .set("x-goog-generation", String.valueOf(stored.generation));
                if (rangeHeader != null && stored.size > 0) {
                  exchange
                      .getResponseHeaders()
                      .set(
                          "Content-Range",
                          String.format(
                              "bytes %d-%d/%d",
                              startOffset, startOffset + requestedLength - 1, stored.size));
                  exchange.sendResponseHeaders(206, requestedLength);
                } else {
                  exchange.sendResponseHeaders(200, requestedLength == 0 ? -1 : requestedLength);
                }

                long remaining = requestedLength;
                long cur = startOffset;
                try (OutputStream os = exchange.getResponseBody()) {
                  while (remaining > 0) {
                    int step = (int) Math.min(64 * 1024, remaining);
                    if (stored.data != null) {
                      os.write(stored.data, (int) cur, step);
                    } else {
                      os.write(ZERO_SLICE, 0, step);
                    }
                    cur += step;
                    remaining -= step;
                  }
                } catch (IOException aborted) {
                  abortedHttpReads.incrementAndGet();
                }
                return;
              }

              // List Objects in bucket: GET /storage/v1/b/<bucket>/o
              if (!buckets.contains(bucket)) {
                sendJsonError(exchange, 404, "No such bucket: " + bucket);
                return;
              }
              String prefix = queryParam(rawQuery, "prefix");
              if (prefix == null) {
                prefix = "";
              }
              String maxResStr = queryParam(rawQuery, "maxResults");
              String pageToken = queryParam(rawQuery, "pageToken");

              List<StoredObject> matching = new ArrayList<StoredObject>();
              String keyPrefix = bucket + "\0";
              for (Map.Entry<String, StoredObject> entry : objects.entrySet()) {
                if (entry.getKey().startsWith(keyPrefix)) {
                  StoredObject o = entry.getValue();
                  if (prefix.isEmpty() || o.name.startsWith(prefix)) {
                    matching.add(o);
                  }
                }
              }
              Collections.sort(
                  matching,
                  new java.util.Comparator<StoredObject>() {
                    @Override
                    public int compare(StoredObject a, StoredObject b) {
                      return a.name.compareTo(b.name);
                    }
                  });
              int startIdx = pageToken != null ? Integer.parseInt(pageToken) : 0;
              int pageSize = maxResStr != null ? Integer.parseInt(maxResStr) : matching.size();
              int endIdx = Math.min(matching.size(), startIdx + Math.max(1, pageSize));

              StringBuilder sb = new StringBuilder("{\"kind\":\"storage#objects\",\"items\":[");
              for (int i = startIdx; i < endIdx; i++) {
                if (i > startIdx) {
                  sb.append(",");
                }
                StoredObject o = matching.get(i);
                sb.append(
                    String.format(
                        "{\"kind\":\"storage#object\",\"name\":\"%s\",\"bucket\":\"%s\",\"size\":\"%d\",\"generation\":\"%d\",\"contentType\":\"%s\"}",
                        o.name, bucket, o.size, o.generation, o.contentType));
              }
              sb.append("]");
              if (endIdx < matching.size()) {
                sb.append(",\"nextPageToken\":\"").append(endIdx).append("\"");
              }
              sb.append("}");
              byte[] resp = sb.toString().getBytes(StandardCharsets.UTF_8);
              exchange.getResponseHeaders().set("Content-Type", "application/json");
              exchange.sendResponseHeaders(200, resp.length);
              try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
              }
              return;
            }

            sendJsonError(exchange, 404, "Unhandled request: " + method + " " + rawPath);
          }
        };

    httpServer.createContext("/storage/v1/b", storageHandler);
    httpServer.createContext("/download/storage/v1/b/", storageHandler);

    // Upload handler (multipart & resumable)
    httpServer.createContext(
        "/upload/storage/v1/b/",
        new HttpHandler() {
          private final ConcurrentHashMap<String, ByteArrayOutputStream> resumableBuffers =
              new ConcurrentHashMap<String, ByteArrayOutputStream>();
          private final ConcurrentHashMap<String, Long> resumableSizes =
              new ConcurrentHashMap<String, Long>();

          @Override
          public void handle(HttpExchange exchange) throws IOException {
            String rawPath = exchange.getRequestURI().getRawPath();
            String rawQuery = exchange.getRequestURI().getRawQuery();
            String[] parts = rawPath.split("/");
            String bucket = parts.length > 5 ? urlDecode(parts[5]) : "mock-bucket";
            String object = queryParam(rawQuery, "name");
            String uploadType = queryParam(rawQuery, "uploadType");
            String uploadId = queryParam(rawQuery, "upload_id");

            if ("POST".equalsIgnoreCase(exchange.getRequestMethod())
                && "resumable".equals(uploadId == null ? uploadType : null)) {
              // Extract object name from JSON body if not in query string
              ByteArrayOutputStream reqBody = new ByteArrayOutputStream();
              try (InputStream is = exchange.getRequestBody()) {
                byte[] b = new byte[4096];
                int r;
                while ((r = is.read(b)) != -1) {
                  reqBody.write(b, 0, r);
                }
              }
              if (object == null || object.isEmpty()) {
                String bodyStr = new String(reqBody.toByteArray(), StandardCharsets.UTF_8);
                int nIdx = bodyStr.indexOf("\"name\"");
                if (nIdx >= 0) {
                  int q1 = bodyStr.indexOf('"', bodyStr.indexOf(':', nIdx) + 1);
                  int q2 = bodyStr.indexOf('"', q1 + 1);
                  if (q1 >= 0 && q2 > q1) {
                    object = bodyStr.substring(q1 + 1, q2);
                  }
                }
              }
              if (object == null || object.isEmpty()) {
                object = "bench-obj";
              }
              String ifGenStr = queryParam(rawQuery, "ifGenerationMatch");
              if (ifGenStr != null) {
                long cond = Long.parseLong(ifGenStr);
                StoredObject existing = objects.get(objectKey(bucket, object));
                if ((cond == 0L && existing != null)
                    || (cond > 0L && (existing == null || existing.generation != cond))) {
                  sendJsonError(exchange, 412, "Precondition Failed");
                  return;
                }
              }

              String sid = "sess-" + nextGen.incrementAndGet();
              resumableBuffers.put(sid, new ByteArrayOutputStream());
              resumableSizes.put(sid, 0L);
              String loc =
                  getHttpEndpoint()
                      + rawPath
                      + "?uploadType=resumable&name="
                      + object
                      + "&upload_id="
                      + sid;
              exchange.getResponseHeaders().set("Location", loc);
              exchange.sendResponseHeaders(200, -1);
              exchange.close();
              return;
            }

            if (object == null || object.isEmpty()) {
              object = "bench-obj";
            }

            ByteArrayOutputStream bodyBuf =
                uploadId != null ? resumableBuffers.get(uploadId) : new ByteArrayOutputStream();
            long accumulated = uploadId != null ? resumableSizes.getOrDefault(uploadId, 0L) : 0L;

            try (InputStream is = exchange.getRequestBody()) {
              byte[] buf = new byte[65536];
              int r;
              while ((r = is.read(buf)) != -1) {
                accumulated += r;
                if (bodyBuf != null && accumulated <= 32L * 1024 * 1024) {
                  bodyBuf.write(buf, 0, r);
                } else if (bodyBuf != null) {
                  bodyBuf.reset();
                  bodyBuf = null;
                  if (uploadId != null) {
                    resumableBuffers.remove(uploadId);
                  }
                }
              }
            }
            if (uploadId != null) {
              resumableSizes.put(uploadId, accumulated);
            }

            // Multipart upload body framing: strip MIME boundaries if uploadType=multipart
            String ct = exchange.getRequestHeaders().getFirst("Content-Type");
            if ("multipart".equals(uploadType)
                && bodyBuf != null
                && ct != null
                && ct.contains("boundary=")) {
              byte[] rawMultipart = bodyBuf.toByteArray();
              String boundary = "--" + ct.substring(ct.indexOf("boundary=") + 9).trim();
              if (boundary.endsWith(";")) {
                boundary = boundary.substring(0, boundary.length() - 1);
              }
              byte[] extracted = extractMultipartMediaPart(rawMultipart, boundary);
              if (extracted != null) {
                bodyBuf = new ByteArrayOutputStream(extracted.length);
                bodyBuf.write(extracted);
                accumulated = extracted.length;
              }
            }

            String cr = exchange.getRequestHeaders().getFirst("Content-Range");
            if (cr != null && cr.startsWith("bytes ")) {
              String[] parts2 = cr.substring(6).split("/");
              String rangePart = parts2[0];
              long total = -1;
              if (parts2.length > 1 && !"*".equals(parts2[1])) {
                total = Long.parseLong(parts2[1]);
              }
              long end = -1;
              if (rangePart.contains("-")) {
                String[] r = rangePart.split("-");
                if (r.length > 1 && !"*".equals(r[1])) {
                  end = Long.parseLong(r[1]);
                }
              }
              if ((total < 0 && end >= 0) || (total > 0 && end >= 0 && end + 1 < total)) {
                exchange.getResponseHeaders().set("Range", "bytes=0-" + end);
                exchange.sendResponseHeaders(308, -1);
                exchange.close();
                return;
              }
              if (total >= 0) {
                accumulated = total;
              }
            }

            buckets.add(bucket);
            byte[] finalBytes = bodyBuf != null ? bodyBuf.toByteArray() : null;
            int crc = finalBytes != null ? Hashing.crc32c().hashBytes(finalBytes).asInt() : 0;
            long gen = nextGen.incrementAndGet();
            StoredObject stored =
                new StoredObject(
                    bucket,
                    object,
                    finalBytes,
                    accumulated,
                    gen,
                    1L,
                    "application/octet-stream",
                    crc,
                    true);
            objects.put(objectKey(bucket, object), stored);
            if (uploadId != null) {
              resumableBuffers.remove(uploadId);
              resumableSizes.remove(uploadId);
            }

            String json =
                String.format(
                    "{\"kind\":\"storage#object\",\"name\":\"%s\",\"bucket\":\"%s\",\"size\":\"%d\",\"generation\":\"%d\"}",
                    object, bucket, accumulated, gen);
            byte[] resp = json.getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
              os.write(resp);
            }
          }
        });
  }

  private static byte[] extractMultipartMediaPart(byte[] raw, String boundary) {
    byte[] bBytes = boundary.getBytes(StandardCharsets.ISO_8859_1);
    int firstBoundary = indexOf(raw, bBytes, 0);
    if (firstBoundary < 0) {
      return null;
    }
    int secondBoundary = indexOf(raw, bBytes, firstBoundary + bBytes.length);
    if (secondBoundary < 0) {
      return null;
    }
    byte[] headerSep = "\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1);
    int mediaStart = indexOf(raw, headerSep, secondBoundary + bBytes.length);
    if (mediaStart < 0) {
      headerSep = "\n\n".getBytes(StandardCharsets.ISO_8859_1);
      mediaStart = indexOf(raw, headerSep, secondBoundary + bBytes.length);
      if (mediaStart < 0) {
        return null;
      }
    }
    mediaStart += headerSep.length;
    int thirdBoundary = indexOf(raw, bBytes, mediaStart);
    if (thirdBoundary < 0) {
      return null;
    }
    int mediaEnd = thirdBoundary;
    if (mediaEnd >= 2 && raw[mediaEnd - 2] == '\r' && raw[mediaEnd - 1] == '\n') {
      mediaEnd -= 2;
    } else if (mediaEnd >= 1 && raw[mediaEnd - 1] == '\n') {
      mediaEnd -= 1;
    }
    if (mediaEnd < mediaStart) {
      return new byte[0];
    }
    byte[] out = new byte[mediaEnd - mediaStart];
    System.arraycopy(raw, mediaStart, out, 0, out.length);
    return out;
  }

  private static int indexOf(byte[] haystack, byte[] needle, int fromIdx) {
    outer:
    for (int i = Math.max(0, fromIdx); i <= haystack.length - needle.length; i++) {
      for (int j = 0; j < needle.length; j++) {
        if (haystack[i + j] != needle[j]) {
          continue outer;
        }
      }
      return i;
    }
    return -1;
  }

  @Override
  public void close() {
    if (grpcServer != null) {
      grpcServer.shutdownNow();
      grpcServer = null;
    }
    if (httpServer != null) {
      httpServer.stop(0);
      httpServer = null;
    }
  }
}
