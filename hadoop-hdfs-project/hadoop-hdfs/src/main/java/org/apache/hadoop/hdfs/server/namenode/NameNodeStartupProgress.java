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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;

import java.math.BigDecimal;

@InterfaceAudience.Private
public class NameNodeStartupProgress {

  public long loadedDelegationKeys;
  public long loadedDelegationTokens;
  public long loadedEditOps;
  public long loadedInodes;
  public NameNodeStartupState state = NameNodeStartupState.INITIALIZED;
  public long totalDelegationKeys;
  public long totalDelegationTokens;
  public long totalEditOps;
  public long totalInodes;

  public float getLoadingFsImagePercentComplete() {
    return getPercentComplete(loadedInodes, totalInodes);
  }

  public float getLoadingEditsPercentComplete() {
    return getPercentComplete(loadedEditOps, totalEditOps);
  }

  public float getLoadingDelegationKeysPercentComplete() {
    return getPercentComplete(loadedDelegationKeys, totalDelegationKeys);
  }

  public float getLoadingDelegationTokensPercentComplete() {
    return getPercentComplete(loadedDelegationTokens, totalDelegationTokens);
  }

  private static float getPercentComplete(long count, long total) {
    return total > 0 ? 1.0f * count / total : 0.0f;
  }
}
