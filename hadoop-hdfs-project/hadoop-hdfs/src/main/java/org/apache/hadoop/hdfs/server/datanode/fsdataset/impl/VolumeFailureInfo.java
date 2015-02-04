/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.server.datanode.fsdataset.impl;

final class VolumeFailureInfo {
  private final String failedStorageLocation;
  private final long failureDate;
  private final long estimatedCapacityLost;

  public VolumeFailureInfo(String failedStorageLocation, long failureDate,
      long estimatedCapacityLost) {
    this.failedStorageLocation = failedStorageLocation;
    this.failureDate = failureDate;
    this.estimatedCapacityLost = estimatedCapacityLost;
  }

  public String getFailedStorageLocation() {
    return this.failedStorageLocation;
  }

  public long getFailureDate() {
    return this.failureDate;
  }

  public long getEstimatedCapacityLost() {
    return this.estimatedCapacityLost;
  }
}
