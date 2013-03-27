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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.Time;

/**
 * StartupProgressView is an immutable, consistent, read-only view of namenode
 * startup progress.  Callers obtain an instance by calling
 * {@link StartupProgress#createView()} to clone current startup progress state.
 * Subsequent updates to startup progress will not alter the view.  This isolates
 * the reader from ongoing updates and establishes a guarantee that the values
 * returned by the view are consistent and unchanging across multiple related
 * read operations.  Calculations that require aggregation, such as overall
 * percent complete, will not be impacted by mutations performed in other threads
 * mid-way through the calculation.
 */
@InterfaceAudience.Private
public class StartupProgressView {

  private final Map<Phase, PhaseTracking> phases;

  /**
   * Returns the sum of the counter values for all steps in the specified phase.
   * 
   * @param phase Phase to get
   * @return long sum of counter values for all steps
   */
  public long getCount(Phase phase) {
    long sum = 0;
    for (Step step: getSteps(phase)) {
      sum += getCount(phase, step);
    }
    return sum;
  }

  /**
   * Returns the counter value for the specified phase and step.
   * 
   * @param phase Phase to get
   * @param step Step to get
   * @return long counter value for phase and step
   */
  public long getCount(Phase phase, Step step) {
    StepTracking tracking = getStepTracking(phase, step);
    return tracking != null ? tracking.count.get() : 0;
  }

  /**
   * Returns overall elapsed time, calculated as time between start of loading
   * fsimage and end of safemode.
   * 
   * @return long elapsed time
   */
  public long getElapsedTime() {
    Long begin = phases.get(Phase.LOADING_FSIMAGE).beginTime;
    Long end = phases.get(Phase.SAFEMODE).endTime;
    return getElapsedTime(begin, end);
  }

  /**
   * Returns elapsed time for the specified phase, calculated as (end - begin) if
   * phase is complete or (now - begin) if phase is running or 0 if the phase is
   * still pending.
   * 
   * @param phase Phase to get
   * @return long elapsed time
   */
  public long getElapsedTime(Phase phase) {
    PhaseTracking tracking = phases.get(phase);
    return getElapsedTime(tracking.beginTime, tracking.endTime);
  }

  /**
   * Returns elapsed time for the specified phase and step, calculated as
   * (end - begin) if step is complete or (now - begin) if step is running or 0
   * if the step is still pending.
   * 
   * @param phase Phase to get
   * @param step Step to get
   * @return long elapsed time
   */
  public long getElapsedTime(Phase phase, Step step) {
    StepTracking tracking = getStepTracking(phase, step);
    Long begin = tracking != null ? tracking.beginTime : null;
    Long end = tracking != null ? tracking.endTime : null;
    return getElapsedTime(begin, end);
  }

  /**
   * Returns the optional file name associated with the specified phase, possibly
   * null.
   * 
   * @param phase Phase to get
   * @return String optional file name, possibly null
   */
  public String getFile(Phase phase) {
    return phases.get(phase).file;
  }

  /**
   * Returns overall percent complete, calculated by aggregating percent complete
   * of all phases.  This is an approximation that assumes all phases have equal
   * running time.  In practice, this isn't true, but there isn't sufficient
   * information available to predict proportional weights for each phase.
   * 
   * @return float percent complete
   */
  public float getPercentComplete() {
    if (getStatus(Phase.SAFEMODE) == Status.COMPLETE) {
      return 1.0f;
    } else {
      float total = 0.0f;
      int numPhases = 0;
      for (Phase phase: phases.keySet()) {
        ++numPhases;
        total += getPercentComplete(phase);
      }
      return getBoundedPercent(total / numPhases);
    }
  }

  /**
   * Returns percent complete for the specified phase, calculated by aggregating
   * the counter values and totals for all steps within the phase.
   * 
   * @param phase Phase to get
   * @return float percent complete
   */
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

  /**
   * Returns percent complete for the specified phase and step, calculated as
   * counter value divided by total.
   * 
   * @param phase Phase to get
   * @param step Step to get
   * @return float percent complete
   */
  public float getPercentComplete(Phase phase, Step step) {
    if (getStatus(phase) == Status.COMPLETE) {
      return 1.0f;
    } else {
      long total = getTotal(phase, step);
      long count = getCount(phase, step);
      return total > 0 ? getBoundedPercent(1.0f * count / total) : 0.0f;
    }
  }

  /**
   * Returns all phases.
   * 
   * @return Iterable<Phase> containing all phases
   */
  public Iterable<Phase> getPhases() {
    return EnumSet.allOf(Phase.class);
  }

  /**
   * Returns all steps within a phase.
   * 
   * @param phase Phase to get
   * @return Iterable<Step> all steps
   */
  public Iterable<Step> getSteps(Phase phase) {
    return new TreeSet<Step>(phases.get(phase).steps.keySet());
  }

  /**
   * Returns the optional size in bytes associated with the specified phase,
   * possibly null.
   * 
   * @param phase Phase to get
   * @return Long optional size in bytes, possibly null
   */
  public Long getSize(Phase phase) {
    return phases.get(phase).size;
  }

  /**
   * Returns the current run status of the specified phase.
   * 
   * @param phase Phase to get
   * @return Status run status of phase
   */
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

  /**
   * Returns the sum of the totals for all steps in the specified phase.
   * 
   * @param phase Phase to get
   * @return long sum of totals for all steps
   */
  public long getTotal(Phase phase) {
    long sum = 0;
    for (StepTracking tracking: phases.get(phase).steps.values()) {
      if (tracking.total != null) {
        sum += tracking.total;
      }
    }
    return sum;
  }

  /**
   * Returns the total for the specified phase and step.
   * 
   * @param phase Phase to get
   * @param step Step to get
   * @return long total
   */
  public long getTotal(Phase phase, Step step) {
    StepTracking tracking = getStepTracking(phase, step);
    Long total = tracking != null ? tracking.total : null;
    return total != null ? total : 0;
  }

  /**
   * Creates a new StartupProgressView by cloning data from the specified
   * StartupProgress.
   * 
   * @param prog StartupProgress to clone
   */
  StartupProgressView(StartupProgress prog) {
    phases = new HashMap<Phase, PhaseTracking>();
    for (Map.Entry<Phase, PhaseTracking> entry: prog.phases.entrySet()) {
      phases.put(entry.getKey(), entry.getValue().clone());
    }
  }

  /**
   * Returns elapsed time, calculated as (end - begin) if both are defined or
   * (now - begin) if end is undefined or 0 if both are undefined.
   */
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

  /**
   * Returns the StepTracking internal data structure for the specified phase
   * and step, possibly null if not found.
   * 
   * @param phase Phase to get
   * @param step Step to get
   * @return StepTracking for phase and step, possibly null
   */
  private StepTracking getStepTracking(Phase phase, Step step) {
    PhaseTracking phaseTracking = phases.get(phase);
    Map<Step, StepTracking> steps = phaseTracking != null ?
      phaseTracking.steps : null;
    return steps != null ? steps.get(step) : null;
  }

  /**
   * Returns the given value restricted to the range [0.0, 1.0].
   * 
   * @param percent float value to restrict
   * @return float value restricted to range [0.0, 1.0]
   */
  private static float getBoundedPercent(float percent) {
    return Math.max(0.0f, Math.min(1.0f, percent));
  }
}
