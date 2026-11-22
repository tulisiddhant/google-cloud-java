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

package com.google.cloud.storage.sidecar.benchmark;

import com.google.cloud.storage.sidecar.UdsTransport;

/** Contract implemented by each benchmark workload ({@code WL1}–{@code WL7b}). */
public interface Workload {

  /** Canonical workload identifier (e.g., {@code "WL1"}, {@code "WL7a"}). */
  String id();

  /** Human-readable title matching Section 6.2 table headings. */
  String displayName();

  /** Returns {@code true} if this workload is {@code REQUIRED} per the Section 3 matrix. */
  boolean isApplicable(UdsTransport protocol, BenchmarkConfig.BucketType bucketType);

  /** Executes untimed warmup followed by the timed steady-state measurement window. */
  WorkloadResult execute(ClientProvider clients, BenchmarkConfig config) throws Exception;
}
