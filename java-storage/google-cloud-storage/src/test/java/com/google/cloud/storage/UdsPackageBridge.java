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

import com.google.cloud.BaseServiceException;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;
import com.google.storage.v2.ListBucketsRequest;
import com.google.storage.v2.ListObjectsRequest;
import com.google.storage.v2.ReadObjectRequest;
import com.google.storage.v2.WriteObjectRequest;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.OptionalLong;

/**
 * Package-private bridge exposing {@code com.google.cloud.storage} constructors, package-private
 * metadata builders, and {@link UnifiedOpts} option decoders to the UDS veneer adapter without
 * reflection or {@code src/main} changes.
 */
public final class UdsPackageBridge {

  private UdsPackageBridge() {}

  public static BlobInfo buildBlobInfo(
      String bucket,
      String objectName,
      long generation,
      long size,
      long metageneration,
      String contentType,
      String contentEncoding,
      String storageClass,
      long updatedUnixSecs,
      int crc32c) {
    BlobId id =
        generation > 0 ? BlobId.of(bucket, objectName, generation) : BlobId.of(bucket, objectName);
    BlobInfo.Builder b = BlobInfo.newBuilder(id).setSize(size);
    if (metageneration > 0) {
      b.setMetageneration(metageneration);
    }
    if (contentType != null && !contentType.isEmpty()) {
      b.setContentType(contentType);
    }
    if (contentEncoding != null && !contentEncoding.isEmpty()) {
      b.setContentEncoding(contentEncoding);
    }
    if (storageClass != null && !storageClass.isEmpty()) {
      b.setStorageClass(StorageClass.valueOf(storageClass));
    }
    if (updatedUnixSecs > 0) {
      b.setUpdateTimeOffsetDateTime(
          OffsetDateTime.ofInstant(Instant.ofEpochSecond(updatedUnixSecs), ZoneOffset.UTC));
    }
    if (crc32c != 0) {
      b.setCrc32c(BaseEncoding.base64().encode(Ints.toByteArray(crc32c)));
    }
    return b.build();
  }

  public static Blob buildBlob(
      Storage storage,
      String bucket,
      String objectName,
      long generation,
      long size,
      long metageneration,
      String contentType,
      String contentEncoding,
      String storageClass,
      long updatedUnixSecs,
      int crc32c) {
    BlobInfo info =
        buildBlobInfo(
            bucket,
            objectName,
            generation,
            size,
            metageneration,
            contentType,
            contentEncoding,
            storageClass,
            updatedUnixSecs,
            crc32c);
    return new Blob(storage, (BlobInfo.BuilderImpl) info.toBuilder());
  }

  public static Blob buildSyntheticDirectoryBlob(Storage storage, String bucket, String prefix) {
    BlobInfo info =
        BlobInfo.newBuilder(BlobId.of(bucket, prefix)).setSize(0L).setIsDirectory(true).build();
    return new Blob(storage, (BlobInfo.BuilderImpl) info.toBuilder());
  }

  public static Bucket buildBucket(
      Storage storage,
      String name,
      String location,
      String storageClass,
      long metageneration,
      long createdUnixSecs) {
    BucketInfo.Builder b = BucketInfo.newBuilder(name);
    if (location != null && !location.isEmpty()) {
      b.setLocation(location);
    }
    if (storageClass != null && !storageClass.isEmpty()) {
      b.setStorageClass(StorageClass.valueOf(storageClass));
    }
    if (metageneration > 0) {
      b.setMetageneration(metageneration);
    }
    if (createdUnixSecs > 0) {
      b.setCreateTimeOffsetDateTime(
          OffsetDateTime.ofInstant(Instant.ofEpochSecond(createdUnixSecs), ZoneOffset.UTC));
    }
    BucketInfo info = b.build();
    return new Bucket(storage, (BucketInfo.BuilderImpl) info.toBuilder());
  }

  public static ListObjectsRequest extractListObjectsRequest(
      String bucket, Storage.BlobListOption... options) {
    UnifiedOpts.Opts<UnifiedOpts.ObjectListOpt> opts = UnifiedOpts.Opts.unwrap(options);
    ListObjectsRequest.Builder builder =
        ListObjectsRequest.newBuilder().setParent("projects/_/buckets/" + bucket);
    return opts.listObjectsRequest().apply(builder).build();
  }

  public static ListBucketsRequest extractListBucketsRequest(
      String projectId, Storage.BucketListOption... options) {
    UnifiedOpts.Opts<UnifiedOpts.BucketListOpt> opts = UnifiedOpts.Opts.unwrap(options);
    ListBucketsRequest.Builder builder =
        ListBucketsRequest.newBuilder().setParent("projects/" + projectId);
    return opts.listBucketsRequest().apply(builder).build();
  }

  public static OptionalLong extractSourceIfGenerationMatch(
      BlobId blobId, Storage.BlobSourceOption... options) {
    UnifiedOpts.Opts<UnifiedOpts.ObjectSourceOpt> opts =
        UnifiedOpts.Opts.unwrap(options).resolveFrom(blobId);
    ReadObjectRequest req = opts.readObjectRequest().apply(ReadObjectRequest.newBuilder()).build();
    return req.hasIfGenerationMatch()
        ? OptionalLong.of(req.getIfGenerationMatch())
        : OptionalLong.empty();
  }

  public static OptionalLong extractWriteIfGenerationMatch(
      BlobInfo blobInfo, Storage.BlobWriteOption... options) {
    UnifiedOpts.Opts<UnifiedOpts.ObjectTargetOpt> opts =
        UnifiedOpts.Opts.unwrap(options).resolveFrom(blobInfo);
    WriteObjectRequest req =
        opts.writeObjectRequest().apply(WriteObjectRequest.newBuilder()).build();
    return req.getWriteObjectSpec().hasIfGenerationMatch()
        ? OptionalLong.of(req.getWriteObjectSpec().getIfGenerationMatch())
        : OptionalLong.empty();
  }

  /**
   * Translates gRPC-over-UDS {@link StatusRuntimeException} / {@link StatusException} errors into
   * {@link StorageException}s carrying the SDK's canonical HTTP status code mapping.
   */
  public static StorageException toStorageException(Throwable t) {
    if (t instanceof StorageException) {
      return (StorageException) t;
    }
    if (t instanceof StatusRuntimeException
        || t instanceof StatusException
        || t.getCause() instanceof StatusRuntimeException
        || t.getCause() instanceof StatusException) {
      Status status = Status.fromThrowable(t);
      int httpCode = GrpcToHttpStatusCodeTranslation.grpcCodeToHttpStatusCode(status.getCode());
      String message =
          status.getDescription() != null ? status.getDescription() : status.getCode().name();
      return new StorageException(httpCode, message, status.getCode().name(), t);
    }
    BaseServiceException coalesced = StorageException.coalesce(t);
    if (coalesced instanceof StorageException) {
      return (StorageException) coalesced;
    }
    return new StorageException(coalesced.getCode(), coalesced.getMessage(), coalesced);
  }
}
