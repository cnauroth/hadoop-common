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
import java.util.HashMap;
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

  public enum Status {
    PENDING,
    RUNNING,
    COMPLETE
  }

  private static EnumSet<Phase> VISIBLE_PHASES = EnumSet.range(LOADING_FSIMAGE,
    SAFEMODE);

  private Map<Phase, Long> phaseBeginTime = newConcurrentMap();
  private Map<Phase, Long> phaseEndTime = newConcurrentMap();
  private Map<Phase, String> phaseTag = newConcurrentMap();
  private Map<Phase, Map<String, Long>> stepBeginTime = newConcurrentMap();
  private Map<Phase, Map<String, Long>> stepCount = newConcurrentMap();
  private Map<Phase, Map<String, Long>> stepEndTime = newConcurrentMap();
  private Map<Phase, Map<String, Long>> stepTotal = newConcurrentMap();

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
  }

  public void beginPhase(Phase phase, String tag) {
    beginPhase(phase);
    phaseTag.put(phase, tag);
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

  public Status getStatus(Phase phase) {
    if (phaseBeginTime.get(phase) == null) {
      return Status.PENDING;
    } else if (phaseEndTime.get(phase) == null) {
      return Status.RUNNING;
    } else {
      return Status.COMPLETE;
    }
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

  public View createView() {
    return new View(this);
  }

  private <K, V> Map<K, V> newConcurrentMap() {
    return new ConcurrentHashMap<K, V>();
  }

  public static class View {
    private final Map<Phase, Long> viewPhaseBeginTime;
    private final Map<Phase, Long> viewPhaseEndTime;
    private final Map<Phase, String> viewPhaseTag;
    private final Map<Phase, Map<String, Long>> viewStepBeginTime;
    private final Map<Phase, Map<String, Long>> viewStepCount;
    private final Map<Phase, Map<String, Long>> viewStepEndTime;
    private final Map<Phase, Map<String, Long>> viewStepTotal;

    public long getCount(Phase phase) {
      return sumValues(viewStepCount.get(phase));
    }

    public long getCount(Phase phase, String step) {
      Map<String, Long> stepsInPhase = viewStepCount.get(phase);
      Long count = stepsInPhase != null ? stepsInPhase.get(step) : null;
      return count != null ? count : 0;
    }

    public long getElapsedTime() {
      Long begin = viewPhaseBeginTime.get(Phase.LOADING_FSIMAGE);
      Long end = viewPhaseEndTime.get(Phase.SAFEMODE);
      return getElapsedTime(begin, end);
    }

    public long getElapsedTime(Phase phase) {
      Long begin = viewPhaseBeginTime.get(phase);
      Long end = viewPhaseEndTime.get(phase);
      return getElapsedTime(begin, end);
    }

    public long getElapsedTime(Phase phase, String step) {
      Map<String, Long> stepBeginTimesInPhase = viewStepBeginTime.get(phase);
      Long begin = stepBeginTimesInPhase != null ?
        stepBeginTimesInPhase.get(step) : null;
      Map<String, Long> stepEndTimesInPhase = viewStepEndTime.get(phase);
      Long end = stepEndTimesInPhase != null ? stepEndTimesInPhase.get(step) :
        null;
      return getElapsedTime(begin, end);
    }

    public float getPercentComplete() {
      if (getStatus(Phase.COMPLETE) == Status.RUNNING) {
        return 1.0f;
      } else {
        float total = 0.0f;
        int count = 0;
        for (Phase phase: VISIBLE_PHASES) {
          ++count;
          total += getPercentComplete(phase);
        }
        return Math.max(0.0f, Math.min(1.0f, total / count));
      }
    }

    public float getPercentComplete(Phase phase) {
      if (getStatus(phase) == Status.COMPLETE) {
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
      if (getStatus(phase) == Status.COMPLETE) {
        return 1.0f;
      } else {
        long total = getTotal(phase, step);
        long count = getCount(phase, step);
        return total > 0 ? 1.0f * count / total : 0.0f;
      }
    }

    public String getPhaseTag(Phase phase) {
      return viewPhaseTag.get(phase);
    }

    public Iterable<String> getSteps(Phase phase) {
      Map<String, Long> stepsInPhase = viewStepBeginTime.get(phase);
      return stepsInPhase != null ? new TreeSet(stepsInPhase.keySet()) :
        Collections.<String>emptyList();
    }

    public Status getStatus(Phase phase) {
      if (viewPhaseBeginTime.get(phase) == null) {
        return Status.PENDING;
      } else if (viewPhaseEndTime.get(phase) == null) {
        return Status.RUNNING;
      } else {
        return Status.COMPLETE;
      }
    }

    public long getTotal(Phase phase) {
      return sumValues(viewStepTotal.get(phase));
    }

    public long getTotal(Phase phase, String step) {
      Map<String, Long> stepsInPhase = viewStepTotal.get(phase);
      Long total = stepsInPhase.get(step);
      return total != null ? total : 0;
    }

    private View(StartupProgress prog) {
      viewPhaseBeginTime = copyMap(prog.phaseBeginTime);
      viewPhaseEndTime = copyMap(prog.phaseEndTime);
      viewPhaseTag = copyMap(prog.phaseTag);
      viewStepBeginTime = copyMap(prog.stepBeginTime);
      viewStepCount = copyMap(prog.stepCount);
      viewStepEndTime = copyMap(prog.stepEndTime);
      viewStepTotal = copyMap(prog.stepTotal);
    }

    private <K, V> Map<K, V> copyMap(Map<K, V> source) {
      return new HashMap<K, V>(source);
    }

    private long getElapsedTime(Long begin, Long end) {
      if (begin != null && end != null) {
        return end - begin;
      } else if (begin != null) {
        return monotonicNow() - begin;
      } else {
        return 0;
      }
    }

    private long sumValues(Map<String, Long> map) {
      long sum = 0;
      if (map != null) {
        for (Long value: map.values()) {
          if (value != null) {
            sum += value;
          }
        }
      }
      return sum;
    }
  }
}
