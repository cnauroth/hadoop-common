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

package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.fs.azurenative.BandwidthThrottle.ThrottleType;


/**
 * Throttling feedback interface from AzureSDK to AzureNativeFileSystemStore.
 * The feedback interface contract is implemented by the AzureNativeFileSystemStore
 * class.
 *
 */
public interface BandwidthThrottleFeedback {
  /**
   * Azure throttling feedback method: Update transmission success counter by delta.
   * Also communicate the current latency for the successful request.
   */
  public long updateTransmissionSuccess (
      ThrottleType kindOfThrottle, long delta, long requestLatency);

  /**
   * Azure throttling feedback method: Update transmission failure by delta.
   */
  public long updateTransmissionFailure (
      ThrottleType kindOfThrottle, long delta, long requestLatency);
}
