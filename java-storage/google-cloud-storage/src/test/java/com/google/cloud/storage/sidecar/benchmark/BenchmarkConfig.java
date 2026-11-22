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

import static java.util.Objects.requireNonNull;

import com.google.cloud.storage.sidecar.UdsTransport;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

/** Immutable configuration for a single isolated benchmark suite process. */
public final class BenchmarkConfig {

  public static final String DEFAULT_PROJECT_ID = "vaibhavpratap-sdk-test";
  public static final String DEFAULT_REGIONAL_BUCKET = "siddhanttuli-java-uds-bench-regional";
  public static final String DEFAULT_ZONAL_BUCKET = "siddhanttuli-java-uds-bench-zonal";
  public static final int DEFAULT_NATIVE_POOL_SIZE = 16;
  public static final int DEFAULT_QUIESCENCE_SEC = 75;

  /** Client driver under test. */
  public enum DriverType {
    NATIVE("native"),
    UDS_SIDECAR("uds-sidecar"),
    UDS_INPROC("uds-inproc");

    private final String cliName;

    DriverType(String cliName) {
      this.cliName = cliName;
    }

    public String cliName() {
      return cliName;
    }

    public static DriverType fromCli(String value) {
      for (DriverType d : values()) {
        if (d.cliName.equalsIgnoreCase(value) || d.name().equalsIgnoreCase(value)) {
          return d;
        }
      }
      throw new IllegalArgumentException("Unknown driver: " + value);
    }
  }

  /** Target GCS bucket topology. */
  public enum BucketType {
    REGIONAL("regional"),
    ZONAL("zonal");

    private final String cliName;

    BucketType(String cliName) {
      this.cliName = cliName;
    }

    public String cliName() {
      return cliName;
    }

    public static BucketType fromCli(String value) {
      for (BucketType b : values()) {
        if (b.cliName.equalsIgnoreCase(value) || b.name().equalsIgnoreCase(value)) {
          return b;
        }
      }
      throw new IllegalArgumentException("Unknown bucket-type: " + value);
    }
  }

  private final DriverType driver;
  private final UdsTransport protocol;
  private final BucketType bucketType;
  private final String projectId;
  private final String bucketName;
  private final int nativePoolSize;
  private final int quiescenceSec;
  private final String sidecarBinaryPath;
  private final String inprocLibraryPath;
  private final String socketPath;
  private final Set<String> workloadFilter;
  private final String outputJsonPath;

  private BenchmarkConfig(Builder b) {
    this.driver = requireNonNull(b.driver, "driver");
    this.protocol = requireNonNull(b.protocol, "protocol");
    this.bucketType = requireNonNull(b.bucketType, "bucketType");
    this.projectId = requireNonNull(b.projectId, "projectId");
    this.bucketName =
        (b.bucketName != null && !b.bucketName.isEmpty())
            ? b.bucketName
            : (b.bucketType == BucketType.ZONAL ? DEFAULT_ZONAL_BUCKET : DEFAULT_REGIONAL_BUCKET);
    this.nativePoolSize = b.nativePoolSize;
    this.quiescenceSec = b.quiescenceSec;
    this.sidecarBinaryPath = b.sidecarBinaryPath;
    this.inprocLibraryPath = b.inprocLibraryPath;
    this.socketPath = b.socketPath;
    this.workloadFilter = Collections.unmodifiableSet(new LinkedHashSet<String>(b.workloadFilter));
    this.outputJsonPath = b.outputJsonPath;

    if (this.bucketType == BucketType.ZONAL && this.protocol == UdsTransport.JSON) {
      throw new IllegalArgumentException(
          "Zonal Rapid buckets are gRPC-only; JSON + ZONAL is N/A per Section 3 matrix");
    }
    if (this.nativePoolSize <= 0) {
      throw new IllegalArgumentException("nativePoolSize must be > 0");
    }
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /** Parses {@code --key=value} CLI arguments into a {@link BenchmarkConfig}. */
  public static BenchmarkConfig fromArgs(String[] args) {
    Builder b = newBuilder();
    for (String arg : args) {
      if (!arg.startsWith("--")) {
        continue;
      }
      int eq = arg.indexOf('=');
      if (eq < 0) {
        throw new IllegalArgumentException("Expected --key=value argument, got: " + arg);
      }
      String key = arg.substring(2, eq).trim().toLowerCase(Locale.US);
      String val = arg.substring(eq + 1).trim();
      switch (key) {
        case "driver":
          b.setDriver(DriverType.fromCli(val));
          break;
        case "protocol":
          b.setProtocol(UdsTransport.valueOf(val.toUpperCase(Locale.US)));
          break;
        case "bucket-type":
          b.setBucketType(BucketType.fromCli(val));
          break;
        case "project":
        case "project-id":
          b.setProjectId(val);
          break;
        case "bucket":
          b.setBucketName(val);
          break;
        case "native-pool-size":
          b.setNativePoolSize(Integer.parseInt(val));
          break;
        case "quiescence-sec":
          b.setQuiescenceSec(Integer.parseInt(val));
          break;
        case "sidecar-bin":
          b.setSidecarBinaryPath(val);
          break;
        case "inproc-lib":
          b.setInprocLibraryPath(val);
          break;
        case "socket-path":
          b.setSocketPath(val);
          break;
        case "workloads":
          if (!val.isEmpty()) {
            b.setWorkloadFilter(Arrays.asList(val.split(",")));
          }
          break;
        case "output-json":
          b.setOutputJsonPath(val);
          break;
        default:
          throw new IllegalArgumentException("Unknown CLI flag: --" + key);
      }
    }
    return b.build();
  }

  public DriverType driver() {
    return driver;
  }

  public UdsTransport protocol() {
    return protocol;
  }

  public BucketType bucketType() {
    return bucketType;
  }

  public String projectId() {
    return projectId;
  }

  public String bucketName() {
    return bucketName;
  }

  public int nativePoolSize() {
    return nativePoolSize;
  }

  public int quiescenceSec() {
    return quiescenceSec;
  }

  public String sidecarBinaryPath() {
    return sidecarBinaryPath;
  }

  public String inprocLibraryPath() {
    return inprocLibraryPath;
  }

  public String socketPath() {
    return socketPath;
  }

  public Set<String> workloadFilter() {
    return workloadFilter;
  }

  public String outputJsonPath() {
    return outputJsonPath;
  }

  public boolean shouldRunWorkload(String workloadId) {
    if (workloadFilter.isEmpty()) {
      return true;
    }
    for (String f : workloadFilter) {
      if (f.equalsIgnoreCase(workloadId)) {
        return true;
      }
    }
    return false;
  }

  /** Builder for {@link BenchmarkConfig}. */
  public static final class Builder {
    private DriverType driver = DriverType.NATIVE;
    private UdsTransport protocol = UdsTransport.GRPC;
    private BucketType bucketType = BucketType.REGIONAL;
    private String projectId = DEFAULT_PROJECT_ID;
    private String bucketName;
    private int nativePoolSize =
        Integer.getInteger("gcs.native.channel.pool.size", DEFAULT_NATIVE_POOL_SIZE);
    private int quiescenceSec = DEFAULT_QUIESCENCE_SEC;
    private String sidecarBinaryPath = System.getProperty("uds.sidecar.bin", "");
    private String inprocLibraryPath = System.getProperty("uds.inproc.lib", "");
    private String socketPath = System.getProperty("uds.socket.path", "");
    private final Set<String> workloadFilter = new LinkedHashSet<String>();
    private String outputJsonPath = "";

    private Builder() {}

    public Builder setDriver(DriverType driver) {
      this.driver = requireNonNull(driver, "driver");
      return this;
    }

    public Builder setProtocol(UdsTransport protocol) {
      this.protocol = requireNonNull(protocol, "protocol");
      return this;
    }

    public Builder setBucketType(BucketType bucketType) {
      this.bucketType = requireNonNull(bucketType, "bucketType");
      return this;
    }

    public Builder setProjectId(String projectId) {
      this.projectId = requireNonNull(projectId, "projectId");
      return this;
    }

    public Builder setBucketName(String bucketName) {
      this.bucketName = bucketName;
      return this;
    }

    public Builder setNativePoolSize(int nativePoolSize) {
      this.nativePoolSize = nativePoolSize;
      return this;
    }

    public Builder setQuiescenceSec(int quiescenceSec) {
      this.quiescenceSec = quiescenceSec;
      return this;
    }

    public Builder setSidecarBinaryPath(String sidecarBinaryPath) {
      this.sidecarBinaryPath = sidecarBinaryPath;
      return this;
    }

    public Builder setInprocLibraryPath(String inprocLibraryPath) {
      this.inprocLibraryPath = inprocLibraryPath;
      return this;
    }

    public Builder setSocketPath(String socketPath) {
      this.socketPath = socketPath;
      return this;
    }

    public Builder setWorkloadFilter(Iterable<String> ids) {
      this.workloadFilter.clear();
      for (String id : ids) {
        String trimmed = id.trim();
        if (!trimmed.isEmpty()) {
          this.workloadFilter.add(trimmed);
        }
      }
      return this;
    }

    public Builder setOutputJsonPath(String outputJsonPath) {
      this.outputJsonPath = outputJsonPath == null ? "" : outputJsonPath;
      return this;
    }

    public BenchmarkConfig build() {
      return new BenchmarkConfig(this);
    }
  }
}
