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

import static org.apache.hadoop.hdfs.server.namenode.NameNodeStartupProgress.Step.*;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.EnumMap;
import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

@InterfaceAudience.Private
@Metrics(name="NameNodeStartupProgress", about="NameNode startup progress",
  context="dfs")
public class NameNodeStartupProgress {

  public enum Step {
    INITIALIZED,
    LOADING_FSIMAGE,
    LOADING_EDITS,
    LOADING_DELEGATION_KEYS,
    LOADING_DELEGATION_TOKENS,
    CHECKPOINTING,
    SAFEMODE,
    COMPLETE;

    public boolean isNowOrAfter(Step other) {
      return ordinal() >= other.ordinal();
    }
  }

  private static EnumSet<Step> VISIBLE_STEPS = EnumSet.range(LOADING_FSIMAGE,
    SAFEMODE);

  private EnumMap<Step, Long> begin = new EnumMap<Step, Long>(Step.class);
  private EnumMap<Step, Long> count = new EnumMap<Step, Long>(Step.class);
  private Step currentStep;
  private EnumMap<Step, Long> end = new EnumMap<Step, Long>(Step.class);
  private EnumMap<Step, Long> total = new EnumMap<Step, Long>(Step.class);

  public static NameNodeStartupProgress create() {
    NameNodeStartupProgress startupProgress = new NameNodeStartupProgress();
    DefaultMetricsSystem.instance().register(startupProgress);
    return startupProgress;
  }

  public static Iterable<Step> getVisibleSteps() {
    return VISIBLE_STEPS;
  }

  public long getCount(Step step) {
    return getValue(count, step);
  }

  @Metric public String getCurrentStep() {
    return currentStep.toString();
  }

  public long getElapsedTime(Step step) {
    Long stepBegin = begin.get(step);
    Long stepEnd = end.get(step);
    if (stepBegin != null && stepEnd != null) {
      return stepEnd - stepBegin;
    } else if (stepBegin != null) {
      return monotonicNow() - stepBegin;
    } else {
      return 0;
    }
  }

  @Metric public long getLoadedDelegationKeys() {
    return getCount(LOADING_DELEGATION_KEYS);
  }

  @Metric public long getLoadedDelegationTokens() {
    return getCount(LOADING_DELEGATION_TOKENS);
  }

  @Metric public long getLoadedEditOps() {
    return getCount(LOADING_EDITS);
  }

  @Metric public long getLoadedInodes() {
    return getCount(LOADING_FSIMAGE);
  }

  @Metric public long getLoadingDelegationKeysElapsedTime() {
    return getElapsedTime(LOADING_DELEGATION_KEYS);
  }

  @Metric public float getLoadingDelegationKeysPercentComplete() {
    return getPercentComplete(LOADING_DELEGATION_KEYS);
  }

  @Metric public long getLoadingDelegationTokensElapsedTime() {
    return getElapsedTime(LOADING_DELEGATION_TOKENS);
  }

  @Metric public float getLoadingDelegationTokensPercentComplete() {
    return getPercentComplete(LOADING_DELEGATION_TOKENS);
  }

  @Metric public long getLoadingEditsElapsedTime() {
    return getElapsedTime(LOADING_EDITS);
  }

  @Metric public float getLoadingEditsPercentComplete() {
    return getPercentComplete(LOADING_EDITS);
  }

  @Metric public long getLoadingFsImageElapsedTime() {
    return getElapsedTime(LOADING_FSIMAGE);
  }

  @Metric public float getLoadingFsImagePercentComplete() {
    return getPercentComplete(LOADING_FSIMAGE);
  }

  public float getPercentComplete(Step step) {
    if (step.ordinal() < currentStep.ordinal()) {
      return 1.0f;
    } else {
      long stepTotal = getValue(total, step);
      long stepCount = getValue(count, step);
      return stepTotal > 0 ? 1.0f * stepCount / stepTotal : 0.0f;
    }
  }

  public Step getStep() {
    return currentStep;
  }

  public long getTotal(Step step) {
    return getValue(total, step);
  }

  @Metric public long getTotalDelegationKeys() {
    return getTotal(LOADING_DELEGATION_KEYS);
  }

  @Metric public long getTotalDelegationTokens() {
    return getTotal(LOADING_DELEGATION_TOKENS);
  }

  @Metric public long getTotalEditOps() {
    return getTotal(LOADING_EDITS);
  }

  @Metric public long getTotalInodes() {
    return getTotal(LOADING_FSIMAGE);
  }

  public void goToStep(Step next) {
    long now = monotonicNow();
    if (VISIBLE_STEPS.contains(currentStep)) {
      end.put(currentStep, now);
    }
    if (VISIBLE_STEPS.contains(next)) {
      begin.put(next, now);
    }
    currentStep = next;
  }

  public void incrementCount(Step step) {
    Long stepCount = count.get(step);
    if (stepCount == null) {
      stepCount = 0L;
    }
    count.put(step, stepCount + 1);
  }

  public void setTotal(Step step, long stepTotal) {
    total.put(step, stepTotal);
  }

  private NameNodeStartupProgress() {
    goToStep(INITIALIZED);
  }

  private static long getValue(EnumMap<Step, Long> map, Step step) {
    Long stepValue = map.get(step);
    return stepValue != null ? stepValue : 0;
  }
}
