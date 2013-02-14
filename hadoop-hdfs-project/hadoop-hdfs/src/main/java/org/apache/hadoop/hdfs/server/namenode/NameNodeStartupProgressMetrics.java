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

import static org.apache.hadoop.hdfs.server.namenode.NameNodeStartupProgress.Phase.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

@InterfaceAudience.Private
@Metrics(name="NameNodeStartupProgress", about="NameNode startup progress",
  context="dfs")
public class NameNodeStartupProgressMetrics {

  private final NameNodeStartupProgress startupProgress;

  public NameNodeStartupProgressMetrics(
      NameNodeStartupProgress startupProgress) {
    this.startupProgress = startupProgress;
    DefaultMetricsSystem.instance().register(this);
  }

  @Metric public String getCurrentPhase() {
    return String.valueOf(startupProgress.getCurrentPhase());
  }

  @Metric public long getLoadedDelegationKeys() {
    return startupProgress.getCount(LOADING_DELEGATION_KEYS);
  }

  @Metric public long getLoadedDelegationTokens() {
    return startupProgress.getCount(LOADING_DELEGATION_TOKENS);
  }

  @Metric public long getLoadedEditOps() {
    return startupProgress.getCount(LOADING_EDITS);
  }

  @Metric public long getLoadedInodes() {
    return startupProgress.getCount(LOADING_FSIMAGE);
  }

  @Metric public long getLoadingDelegationKeysElapsedTime() {
    return startupProgress.getElapsedTime(LOADING_DELEGATION_KEYS);
  }

  @Metric public float getLoadingDelegationKeysPercentComplete() {
    return startupProgress.getPercentComplete(LOADING_DELEGATION_KEYS);
  }

  @Metric public long getLoadingDelegationTokensElapsedTime() {
    return startupProgress.getElapsedTime(LOADING_DELEGATION_TOKENS);
  }

  @Metric public float getLoadingDelegationTokensPercentComplete() {
    return startupProgress.getPercentComplete(LOADING_DELEGATION_TOKENS);
  }

  @Metric public long getLoadingEditsElapsedTime() {
    return startupProgress.getElapsedTime(LOADING_EDITS);
  }

  @Metric public float getLoadingEditsPercentComplete() {
    return startupProgress.getPercentComplete(LOADING_EDITS);
  }

  @Metric public long getLoadingFsImageElapsedTime() {
    return startupProgress.getElapsedTime(LOADING_FSIMAGE);
  }

  @Metric public float getLoadingFsImagePercentComplete() {
    return startupProgress.getPercentComplete(LOADING_FSIMAGE);
  }

  @Metric public long getTotalDelegationKeys() {
    return startupProgress.getTotal(LOADING_DELEGATION_KEYS);
  }

  @Metric public long getTotalDelegationTokens() {
    return startupProgress.getTotal(LOADING_DELEGATION_TOKENS);
  }

  @Metric public long getTotalEditOps() {
    return startupProgress.getTotal(LOADING_EDITS);
  }

  @Metric public long getTotalInodes() {
    return startupProgress.getTotal(LOADING_FSIMAGE);
  }
}
