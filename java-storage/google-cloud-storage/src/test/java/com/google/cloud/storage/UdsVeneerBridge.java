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

package com.google.cloud.storage;

import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.UnifiedOpts.BucketListOpt;
import com.google.cloud.storage.UnifiedOpts.ObjectListOpt;
import com.google.cloud.storage.UnifiedOpts.ObjectSourceOpt;
import com.google.cloud.storage.UnifiedOpts.ObjectTargetOpt;
import com.google.cloud.storage.UnifiedOpts.Opts;
import com.google.storage.v2.WriteObjectSpec;
import gcscore.v1.ListedBucket;
import gcscore.v1.ListedObject;
import gcscore.v1.ObjectMeta;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;

/**
 * Package-private bridge in {@code com.google.cloud.storage} providing zero-reflection access to
 * {@link UnifiedOpts}, {@link BlobInfo.Builder}, {@link BlobInfo#asBlob(Storage)}, and {@link
 * BucketInfo#asBucket(Storage)} for {@code UdsStorage}.
 */
public final class UdsVeneerBridge {

  private UdsVeneerBridge() {}

  /** Preconditions and generation resolved from {@link BlobSourceOption} or {@link BlobGetOption}. */
  public static final class SourcePreconditions {
    public final Long generation;
    public final Long ifGenerationMatch;

    SourcePreconditions(Long generation, Long ifGenerationMatch) {
      this.generation = generation;
      this.ifGenerationMatch = ifGenerationMatch;
    }
  }

  /** Preconditions and mapped metadata resolved from {@link BlobWriteOption} or {@link BlobTargetOption}. */
  public static final class TargetPreconditions {
    public final BlobInfo resolvedInfo;
    public final Long ifGenerationMatch;
    public final Long expectedObjectSize;

    TargetPreconditions(BlobInfo resolvedInfo, Long ifGenerationMatch, Long expectedObjectSize) {
      this.resolvedInfo = resolvedInfo;
      this.ifGenerationMatch = ifGenerationMatch;
      this.expectedObjectSize = expectedObjectSize;
    }
  }

  public static gcscore.v1.ListObjectsRequest buildListObjectsRequest(
      String bucket, String overridePageToken, BlobListOption... options) {
    Opts<ObjectListOpt> opts =
        (options == null || options.length == 0) ? Opts.empty() : Opts.unwrap(options);
    com.google.storage.v2.ListObjectsRequest v2 =
        opts.listObjectsRequest()
            .apply(
                com.google.storage.v2.ListObjectsRequest.newBuilder()
                    .setParent("projects/_/buckets/" + bucket))
            .build();

    gcscore.v1.ListObjectsRequest.Builder b =
        gcscore.v1.ListObjectsRequest.newBuilder()
            .setBucket(bucket)
            .setPrefix(v2.getPrefix())
            .setDelimiter(v2.getDelimiter())
            .setStartOffset(v2.getLexicographicStart())
            .setEndOffset(v2.getLexicographicEnd())
            .setVersions(v2.getVersions());
    if (v2.getPageSize() > 0) {
      b.setPageSize(v2.getPageSize());
    }
    if (overridePageToken != null && !overridePageToken.isEmpty()) {
      b.setPageToken(overridePageToken);
    } else if (!v2.getPageToken().isEmpty()) {
      b.setPageToken(v2.getPageToken());
    }
    return b.build();
  }

  public static gcscore.v1.ListBucketsRequest buildListBucketsRequest(
      String project, String overridePageToken, BucketListOption... options) {
    Opts<BucketListOpt> opts =
        (options == null || options.length == 0) ? Opts.empty() : Opts.unwrap(options);
    String parent = project.startsWith("projects/") ? project : ("projects/" + project);
    com.google.storage.v2.ListBucketsRequest v2 =
        opts.listBucketsRequest()
            .apply(com.google.storage.v2.ListBucketsRequest.newBuilder().setParent(parent))
            .build();

    gcscore.v1.ListBucketsRequest.Builder b =
        gcscore.v1.ListBucketsRequest.newBuilder()
            .setProject(project)
            .setPrefix(v2.getPrefix());
    if (v2.getPageSize() > 0) {
      b.setPageSize(v2.getPageSize());
    }
    if (overridePageToken != null && !overridePageToken.isEmpty()) {
      b.setPageToken(overridePageToken);
    } else if (!v2.getPageToken().isEmpty()) {
      b.setPageToken(v2.getPageToken());
    }
    return b.build();
  }

  public static SourcePreconditions extractSourceOptions(
      BlobId blobId, BlobSourceOption... options) {
    Opts<ObjectSourceOpt> opts =
        (options == null || options.length == 0)
            ? Opts.empty()
            : Opts.unwrap(options).resolveFrom(blobId);
    com.google.storage.v2.ReadObjectRequest v2 =
        opts.readObjectRequest().apply(com.google.storage.v2.ReadObjectRequest.newBuilder()).build();
    Long gen =
        blobId.getGeneration() != null
            ? blobId.getGeneration()
            : (v2.getGeneration() != 0 ? v2.getGeneration() : null);
    Long ifGenMatch = v2.hasIfGenerationMatch() ? v2.getIfGenerationMatch() : null;
    return new SourcePreconditions(gen, ifGenMatch);
  }

  public static SourcePreconditions extractGetOptions(BlobId blobId, BlobGetOption... options) {
    Opts<ObjectSourceOpt> opts =
        (options == null || options.length == 0)
            ? Opts.empty()
            : Opts.unwrap(options).resolveFrom(blobId);
    com.google.storage.v2.GetObjectRequest v2 =
        opts.getObjectsRequest().apply(com.google.storage.v2.GetObjectRequest.newBuilder()).build();
    Long gen =
        blobId.getGeneration() != null
            ? blobId.getGeneration()
            : (v2.getGeneration() != 0 ? v2.getGeneration() : null);
    Long ifGenMatch = v2.hasIfGenerationMatch() ? v2.getIfGenerationMatch() : null;
    return new SourcePreconditions(gen, ifGenMatch);
  }

  public static TargetPreconditions extractWriteOptions(
      BlobInfo info, BlobWriteOption... options) {
    Opts<ObjectTargetOpt> opts =
        (options == null || options.length == 0)
            ? Opts.empty()
            : Opts.unwrap(options).resolveFrom(info);
    return buildTargetPreconditions(info, opts);
  }

  public static TargetPreconditions extractTargetOptions(
      BlobInfo info, BlobTargetOption... options) {
    Opts<ObjectTargetOpt> opts =
        (options == null || options.length == 0)
            ? Opts.empty()
            : Opts.unwrap(options).resolveFrom(info);
    return buildTargetPreconditions(info, opts);
  }

  private static TargetPreconditions buildTargetPreconditions(
      BlobInfo info, Opts<ObjectTargetOpt> opts) {
    BlobInfo mapped = opts.blobInfoMapper().apply(info.toBuilder()).build();
    com.google.storage.v2.WriteObjectRequest v2 =
        opts.writeObjectRequest()
            .apply(com.google.storage.v2.WriteObjectRequest.newBuilder())
            .build();
    WriteObjectSpec spec = v2.getWriteObjectSpec();
    Long ifGenMatch = spec.hasIfGenerationMatch() ? spec.getIfGenerationMatch() : null;
    Long expectedSize = spec.hasObjectSize() ? spec.getObjectSize() : null;
    return new TargetPreconditions(mapped, ifGenMatch, expectedSize);
  }

  public static String encodeCrc32cBase64(int crc32cUint32) {
    byte[] bytes = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(crc32cUint32).array();
    return Base64.getEncoder().encodeToString(bytes);
  }

  public static int decodeCrc32cBase64(String base64Crc32c) {
    byte[] decoded;
    try {
      decoded = Base64.getDecoder().decode(base64Crc32c);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Base64 CRC32C string: " + base64Crc32c, e);
    }
    if (decoded.length != 4) {
      throw new IllegalArgumentException(
          "Expected 4-byte decoded CRC32C, got " + decoded.length + " bytes");
    }
    return ByteBuffer.wrap(decoded).order(ByteOrder.BIG_ENDIAN).getInt();
  }

  public static BlobInfo blobInfoFromMeta(String bucket, String name, ObjectMeta meta) {
    Long gen = meta.getGeneration() != 0 ? meta.getGeneration() : null;
    BlobInfo.Builder b = BlobInfo.newBuilder(BlobId.of(bucket, name, gen));
    b.setSize(meta.getSize());
    if (meta.getMetageneration() != 0) {
      b.setMetageneration(meta.getMetageneration());
    }
    if (!meta.getContentType().isEmpty()) {
      b.setContentType(meta.getContentType());
    }
    if (!meta.getContentEncoding().isEmpty()) {
      b.setContentEncoding(meta.getContentEncoding());
    }
    if (meta.getCrc32C() != 0) {
      b.setCrc32c(encodeCrc32cBase64(meta.getCrc32C()));
    }
    return b.build();
  }

  public static BlobInfo blobInfoFromListed(String bucket, ListedObject obj) {
    Long gen = obj.getGeneration() != 0 ? obj.getGeneration() : null;
    BlobInfo.Builder b = BlobInfo.newBuilder(BlobId.of(bucket, obj.getName(), gen));
    b.setSize(obj.getSize());
    if (obj.getMetageneration() != 0) {
      b.setMetageneration(obj.getMetageneration());
    }
    if (!obj.getContentType().isEmpty()) {
      b.setContentType(obj.getContentType());
    }
    if (!obj.getContentEncoding().isEmpty()) {
      b.setContentEncoding(obj.getContentEncoding());
    }
    if (!obj.getStorageClass().isEmpty()) {
      b.setStorageClass(StorageClass.valueOf(obj.getStorageClass()));
    }
    if (obj.getUpdatedUnixSecs() > 0) {
      b.setUpdateTimeOffsetDateTime(
          OffsetDateTime.ofInstant(Instant.ofEpochSecond(obj.getUpdatedUnixSecs()), ZoneOffset.UTC));
    }
    if (obj.getCrc32C() != 0) {
      b.setCrc32c(encodeCrc32cBase64(obj.getCrc32C()));
    }
    if (!obj.getEtag().isEmpty()) {
      b.setEtag(obj.getEtag());
    }
    return b.build();
  }

  public static BlobInfo directoryPrefixBlobInfo(String bucket, String prefix) {
    return BlobInfo.newBuilder(BlobId.of(bucket, prefix))
        .setSize(0L)
        .setIsDirectory(true)
        .build();
  }

  public static BlobInfo blobInfoFromWriteResult(
      BlobInfo base, long size, long generation, int crc32cUint32) {
    Long gen = generation != 0 ? generation : base.getGeneration();
    BlobInfo.Builder b = base.toBuilder().setBlobId(BlobId.of(base.getBucket(), base.getName(), gen));
    b.setSize(size);
    if (crc32cUint32 != 0) {
      b.setCrc32c(encodeCrc32cBase64(crc32cUint32));
    }
    return b.build();
  }

  public static Blob asBlob(BlobInfo info, Storage storage) {
    return info.asBlob(storage);
  }

  public static Bucket bucketFromListed(ListedBucket bkt, Storage storage) {
    BucketInfo.Builder b = BucketInfo.newBuilder(bkt.getName());
    if (!bkt.getLocation().isEmpty()) {
      b.setLocation(bkt.getLocation());
    }
    if (!bkt.getStorageClass().isEmpty()) {
      b.setStorageClass(StorageClass.valueOf(bkt.getStorageClass()));
    }
    if (bkt.getMetageneration() != 0) {
      b.setMetageneration(bkt.getMetageneration());
    }
    if (bkt.getCreatedUnixSecs() > 0) {
      b.setCreateTimeOffsetDateTime(
          OffsetDateTime.ofInstant(
              Instant.ofEpochSecond(bkt.getCreatedUnixSecs()), ZoneOffset.UTC));
    }
    return b.build().asBucket(storage);
  }

  public static long maxPendingBytes(BlobAppendableUploadConfig config) {
    return config.getFlushPolicy().getMaxPendingBytes();
  }
}
