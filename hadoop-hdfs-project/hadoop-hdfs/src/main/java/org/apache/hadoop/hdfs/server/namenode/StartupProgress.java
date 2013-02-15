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

import static org.apache.hadoop.hdfs.server.namenode.StartupProgress.Phase.*;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class StartupProgress {

  public enum Phase {
    INITIALIZED("Initialized", "Initialized"),
    LOADING_FSIMAGE("LoadingFsImage", "Loading fsimage"),
    LOADING_EDITS("LoadingEdits", "Loading edits"),
    SAVING_CHECKPOINT("SavingCheckpoint", "Saving checkpoint"),
    SAFEMODE("SafeMode", "Safe mode"),
    COMPLETE("Complete", "Complete");

    final String name, description;

    private Phase(String name, String description) {
      this.name = name;
      this.description = description;
    }

    public String getDescription() {
      return description;
    }

    public String getName() {
      return name;
    }
  }

  private static EnumSet<Phase> VISIBLE_PHASES = EnumSet.range(LOADING_FSIMAGE,
    SAFEMODE);

  private Map<Phase, Long> phaseBeginTime = new ConcurrentHashMap<Phase, Long>();
  private Map<Phase, Long> phaseEndTime = new ConcurrentHashMap<Phase, Long>();

  private Map<Phase, Map<String, Long>> stepBeginTime =
    new ConcurrentHashMap<Phase, Map<String, Long>>();
  private Map<Phase, Map<String, Long>> stepCount =
    new ConcurrentHashMap<Phase, Map<String, Long>>();
  private Map<Phase, Map<String, Long>> stepEndTime =
    new ConcurrentHashMap<Phase, Map<String, Long>>();
  private Map<Phase, Map<String, Long>> stepTotal =
    new ConcurrentHashMap<Phase, Map<String, Long>>();

  private Phase currentPhase;
  private String currentPhaseTag;

  public StartupProgress() {
    beginPhase(INITIALIZED);
  }

  public static Iterable<Phase> getVisiblePhases() {
    return VISIBLE_PHASES;
  }

  public void beginPhase(Phase phase) {
    if (VISIBLE_PHASES.contains(phase)) {
      phaseBeginTime.put(phase, monotonicNow());
      stepBeginTime.put(phase, new ConcurrentHashMap<String, Long>());
      stepCount.put(phase, new ConcurrentHashMap<String, Long>());
      stepEndTime.put(phase, new ConcurrentHashMap<String, Long>());
      stepTotal.put(phase, new ConcurrentHashMap<String, Long>());
    }
    currentPhase = phase;
  }

  public void beginStep(Phase phase, String step) {
    if (VISIBLE_PHASES.contains(phase)) {
      stepBeginTime.get(phase).put(step, monotonicNow());
    }
  }

  public void endPhase(Phase phase) {
    if (VISIBLE_PHASES.contains(phase)) {
      phaseEndTime.put(phase, monotonicNow());
    }
  }

  public void endStep(Phase phase, String step) {
    if (VISIBLE_PHASES.contains(phase)) {
      stepEndTime.get(phase).put(step, monotonicNow());
    }
  }

  public long getCount(Phase phase) {
    long count = 0;
    Map<String, Long> stepsInPhase = stepCount.get(phase);
    if (stepsInPhase != null) {
      for (long stepCount: stepsInPhase.values()) {
        count += stepCount;
      }
    }
    return count;
  }

  public long getCount(Phase phase, String step) {
    Map<String, Long> stepsInPhase = stepCount.get(phase);
    Long count = stepsInPhase != null ? stepsInPhase.get(step) : null;
    return count != null ? count : 0;
  }

  public Phase getCurrentPhase() {
    return currentPhase;
  }

  public long getElapsedTime(Phase phase) {
    Long begin = phaseBeginTime.get(phase);
    Long end = phaseEndTime.get(phase);
    if (begin != null && end != null) {
      return end - begin;
    } else if (begin != null) {
      return monotonicNow() - begin;
    } else {
      return 0;
    }
  }

  public long getElapsedTime(Phase phase, String step) {
    Map<String, Long> stepBeginTimesInPhase = stepBeginTime.get(phase);
    Long begin = stepBeginTimesInPhase != null ? stepBeginTimesInPhase.get(step) : null;
    Map<String, Long> stepEndTimesInPhase = stepEndTime.get(phase);
    Long end = stepEndTimesInPhase != null ? stepEndTimesInPhase.get(step) : null;
    if (begin != null && end != null) {
      return end - begin;
    } else if (begin != null) {
      return monotonicNow() - begin;
    } else {
      return 0;
    }
  }

  public float getPercentComplete(Phase phase) {
    if (phase.ordinal() < currentPhase.ordinal()) {
      return 1.0f;
    } else {
      long total = getTotal(phase);
      long count = 0;
      for (String step: getSteps(phase)) {
        count += getCount(phase, step);
      }
      return total > 0 ? 1.0f * count / total : 0.0f;
    }
  }

  public float getPercentComplete(Phase phase, String step) {
    if (phase.ordinal() < currentPhase.ordinal()) {
      return 1.0f;
    } else {
      long total = getTotal(phase, step);
      long count = getCount(phase, step);
      return total > 0 ? 1.0f * count / total : 0.0f;
    }
  }

  public Iterable<String> getSteps(Phase phase) {
    Map<String, Long> stepsInPhase = stepBeginTime.get(phase);
    return stepsInPhase != null ? new TreeSet(stepsInPhase.keySet()) :
      Collections.<String>emptyList();
  }

  public long getTotal(Phase phase) {
    long total = 0;
    Map<String, Long> stepsInPhase = stepTotal.get(phase);
    if (stepsInPhase != null) {
      for (long stepTotal: stepsInPhase.values()) {
        total += stepTotal;
      }
    }
    return total;
  }

  public long getTotal(Phase phase, String step) {
    Map<String, Long> stepsInPhase = stepTotal.get(phase);
    Long total = stepsInPhase.get(step);
    return total != null ? total : 0;
  }

  public void incrementCount(Phase phase, String step) {
    Map<String, Long> stepsInPhase = stepCount.get(phase);
    Long count = stepsInPhase.get(step);
    if (count == null) {
      count = 0L;
    }
    stepsInPhase.put(step, count + 1);
  }

  public void setTotal(Phase phase, String step, long total) {
    Map<String, Long> stepsInPhase = stepTotal.get(phase);
    stepsInPhase.put(step, total);
  }
}
