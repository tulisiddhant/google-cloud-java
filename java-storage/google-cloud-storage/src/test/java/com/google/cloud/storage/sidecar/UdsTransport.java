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

import com.google.cloud.storage.sidecar.proto.Transport;

/** Upstream GCS wire protocol selector forwarded on {@code gcscore.v1} data-plane requests. */
public enum UdsTransport {
  GRPC(Transport.GRPC),
  JSON(Transport.JSON);

  private final Transport protoValue;

  UdsTransport(Transport protoValue) {
    this.protoValue = protoValue;
  }

  public Transport toProto() {
    return protoValue;
  }
}
