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
package org.apache.hadoop.hdfs.server.protocol;

public class VolumeFailureSummary {
  private final String[] failedStorageLocations;
  private final long lastVolumeFailureDate;
  private final long estimatedCapacityLostTotal;

  public VolumeFailureSummary(String[] failedStorageLocations,
      long lastVolumeFailureDate, long estimatedCapacityLostTotal) {
    this.failedStorageLocations = failedStorageLocations;
    this.lastVolumeFailureDate = lastVolumeFailureDate;
    this.estimatedCapacityLostTotal = estimatedCapacityLostTotal;
  }

  public String[] getFailedStorageLocations() {
    return this.failedStorageLocations;
  }

  public long getLastVolumeFailureDate() {
    return this.lastVolumeFailureDate;
  }

  public long getEstimatedCapacityLostTotal() {
    return this.estimatedCapacityLostTotal;
  }
}
