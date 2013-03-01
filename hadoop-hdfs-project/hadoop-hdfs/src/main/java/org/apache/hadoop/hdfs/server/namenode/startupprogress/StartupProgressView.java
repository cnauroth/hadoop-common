/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Time;

@InterfaceAudience.Private
public class StartupProgressView {

  private final Map<Phase, PhaseTracking> phases;

  public long getCount(Phase phase) {
    long sum = 0;
    for (Step step: getSteps(phase)) {
      sum += getCount(phase, step);
    }
    return sum;
  }

  public long getCount(Phase phase, Step step) {
    StepTracking tracking = getStepTracking(phase, step);
    return tracking != null ? tracking.count.get() : 0;
  }

  public long getElapsedTime() {
    Long begin = phases.get(Phase.LOADING_FSIMAGE).beginTime;
    Long end = phases.get(Phase.SAFEMODE).endTime;
    return getElapsedTime(begin, end);
  }

  public long getElapsedTime(Phase phase) {
    PhaseTracking tracking = phases.get(phase);
    return getElapsedTime(tracking.beginTime, tracking.endTime);
  }

  public long getElapsedTime(Phase phase, Step step) {
    StepTracking tracking = getStepTracking(phase, step);
    Long begin = tracking != null ? tracking.beginTime : null;
    Long end = tracking != null ? tracking.endTime : null;
    return getElapsedTime(begin, end);
  }

  public String getFile(Phase phase) {
    return phases.get(phase).file;
  }

  public float getPercentComplete() {
    if (getStatus(Phase.STARTUP_COMPLETE) == Status.RUNNING) {
      return 1.0f;
    } else {
      float total = 0.0f;
      int numPhases = 0;
      for (Phase phase: phases.keySet()) {
        if (Phase.VISIBLE_PHASES.contains(phase)) {
          ++numPhases;
          total += getPercentComplete(phase);
        }
      }
      return getBoundedPercent(total / numPhases);
    }
  }

  public float getPercentComplete(Phase phase) {
    if (getStatus(phase) == Status.COMPLETE) {
      return 1.0f;
    } else {
      long total = getTotal(phase);
      long count = 0;
      for (Step step: getSteps(phase)) {
        count += getCount(phase, step);
      }
      return total > 0 ? getBoundedPercent(1.0f * count / total) : 0.0f;
    }
  }

  public float getPercentComplete(Phase phase, Step step) {
    if (getStatus(phase) == Status.COMPLETE) {
      return 1.0f;
    } else {
      long total = getTotal(phase, step);
      long count = getCount(phase, step);
      return total > 0 ? getBoundedPercent(1.0f * count / total) : 0.0f;
    }
  }

  public Iterable<Phase> getPhases() {
    return Phase.VISIBLE_PHASES;
  }

  public Iterable<Step> getSteps(Phase phase) {
    return new TreeSet(phases.get(phase).steps.keySet());
  }

  public Long getSize(Phase phase) {
    return phases.get(phase).size;
  }

  public Status getStatus(Phase phase) {
    PhaseTracking tracking = phases.get(phase);
    if (tracking.beginTime == null) {
      return Status.PENDING;
    } else if (tracking.endTime == null) {
      return Status.RUNNING;
    } else {
      return Status.COMPLETE;
    }
  }

  public long getTotal(Phase phase) {
    long sum = 0;
    for (StepTracking tracking: phases.get(phase).steps.values()) {
      if (tracking.total != null) {
        sum += tracking.total;
      }
    }
    return sum;
  }

  public long getTotal(Phase phase, Step step) {
    StepTracking tracking = getStepTracking(phase, step);
    Long total = tracking != null ? tracking.total : null;
    return total != null ? total : 0;
  }

  StartupProgressView(StartupProgress prog) {
    phases = new HashMap<Phase, PhaseTracking>();
    for (Map.Entry<Phase, PhaseTracking> entry: prog.phases.entrySet()) {
      phases.put(entry.getKey(), entry.getValue().clone());
    }
  }

  private long getElapsedTime(Long begin, Long end) {
    final long elapsed;
    if (begin != null && end != null) {
      elapsed = end - begin;
    } else if (begin != null) {
      elapsed = Time.monotonicNow() - begin;
    } else {
      elapsed = 0;
    }
    return Math.max(0, elapsed);
  }

  private StepTracking getStepTracking(Phase phase, Step step) {
    PhaseTracking phaseTracking = phases.get(phase);
    Map<Step, StepTracking> steps = phaseTracking != null ?
      phaseTracking.steps : null;
    return steps != null ? steps.get(step) : null;
  }

  private static float getBoundedPercent(float percent) {
    return Math.max(0.0f, Math.min(1.0f, percent));
  }
}
