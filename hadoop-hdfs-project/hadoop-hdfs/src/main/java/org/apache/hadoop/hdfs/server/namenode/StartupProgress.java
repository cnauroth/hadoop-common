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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class StartupProgress {

  public enum Phase {
    LOADING_FSIMAGE("LoadingFsImage", "Loading fsimage"),
    LOADING_EDITS("LoadingEdits", "Loading edits"),
    SAVING_CHECKPOINT("SavingCheckpoint", "Saving checkpoint"),
    SAFEMODE("SafeMode", "Safe mode"),
    COMPLETE("Complete", "Complete");

    private final String name, description;

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

  public enum StepType {
    AWAITING_REPORTED_BLOCKS("AwaitingReportedBlocks",
      "awaiting reported blocks"),
    DELEGATION_KEYS("DelegationKeys", "delegation keys"),
    DELEGATION_TOKENS("DelegationTokens", "delegation tokens"),
    INODES("Inodes", "inodes");

    private final String name, description;

    private StepType(String name, String description) {
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

  public static class Step implements Comparable<Step> {
    private static final AtomicInteger SEQUENCE = new AtomicInteger();

    private final String file;
    private final int sequenceNumber;
    private final Long size;
    private final StepType type;

    public Step(StepType type) {
      this(null, null, type);
    }

    public Step(String file, long size) {
      this(file, size, null);
    }

    public Step(StepType type, String file) {
      this(file, null, type);
    }

    @Override
    public int compareTo(Step other) {
      return new CompareToBuilder().append(file, other.file)
        .append(sequenceNumber, other.sequenceNumber).toComparison();
    }

    @Override
    public boolean equals(Object otherObj) {
      if (otherObj == null || otherObj.getClass() != getClass()) {
        return false;
      }
      Step other = (Step)otherObj;
      return new EqualsBuilder().append(this.file, other.file)
        .append(this.size, other.size).append(this.type, other.type).isEquals();
    }

    public String getFile() {
      return file;
    }

    public Long getSize() {
      return size;
    }

    public StepType getType() {
      return type;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder().append(file).append(size).append(type)
        .toHashCode();
    }

    private Step(String file, Long size, StepType type) {
      this.file = file;
      this.sequenceNumber = SEQUENCE.incrementAndGet();
      this.size = size;
      this.type = type;
    }
  }

  public enum Status {
    PENDING,
    RUNNING,
    COMPLETE
  }

  public interface Counter {
    void increment();
  }

  private static class PhaseTracking implements Cloneable {
    Long beginTime;
    Long endTime;
    String file;
    Long size;
    ConcurrentMap<Step, StepTracking> steps =
      new ConcurrentHashMap<Step, StepTracking>();

    @Override
    public PhaseTracking clone() {
      PhaseTracking clone = new PhaseTracking();
      clone.beginTime = beginTime;
      clone.endTime = endTime;
      clone.file = file;
      clone.size = size;
      for (Map.Entry<Step, StepTracking> entry: steps.entrySet()) {
        clone.steps.put(entry.getKey(), entry.getValue().clone());
      }
      return clone;
    }
  }

  private static class StepTracking implements Cloneable {
    Long beginTime;
    AtomicLong count = new AtomicLong();
    Long endTime;
    int sequenceNumber;
    Long total;

    @Override
    public StepTracking clone() {
      StepTracking clone = new StepTracking();
      clone.beginTime = beginTime;
      clone.count = new AtomicLong(count.get());
      clone.endTime = endTime;
      clone.sequenceNumber = sequenceNumber;
      clone.total = total;
      return clone;
    }
  }

  private Map<Phase, PhaseTracking> phases =
    new ConcurrentHashMap<Phase, PhaseTracking>();

  private static EnumSet<Phase> VISIBLE_PHASES = EnumSet.range(LOADING_FSIMAGE,
    SAFEMODE);

  public StartupProgress() {
    for (Phase phase: EnumSet.allOf(Phase.class)) {
      phases.put(phase, new PhaseTracking());
    }
  }

  public void beginPhase(Phase phase) {
    phases.get(phase).beginTime = monotonicNow();
  }

  public void beginStep(Phase phase, Step step) {
    lazyInitStep(phase, step).beginTime = monotonicNow();
  }

  public void endPhase(Phase phase) {
    phases.get(phase).endTime = monotonicNow();
  }

  public void endStep(Phase phase, Step step) {
    lazyInitStep(phase, step).endTime = monotonicNow();
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

  public Counter getCounter(Phase phase, Step step) {
    final StepTracking tracking = lazyInitStep(phase, step);
    return new Counter() {
      @Override
      public void increment() {
        tracking.count.incrementAndGet();
      }
    };
  }

  public void setFile(Phase phase, String file) {
    phases.get(phase).file = file;
  }

  public void setSize(Phase phase, long size) {
    phases.get(phase).size = size;
  }

  public void setTotal(Phase phase, Step step, long total) {
    lazyInitStep(phase, step).total = total;
  }

  public View createView() {
    return new View(this);
  }

  public static class View {
    private final Map<Phase, PhaseTracking> viewPhases;

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
      Long begin = viewPhases.get(Phase.LOADING_FSIMAGE).beginTime;
      Long end = viewPhases.get(Phase.SAFEMODE).endTime;
      return getElapsedTime(begin, end);
    }

    public long getElapsedTime(Phase phase) {
      PhaseTracking tracking = viewPhases.get(phase);
      return getElapsedTime(tracking.beginTime, tracking.endTime);
    }

    public long getElapsedTime(Phase phase, Step step) {
      StepTracking tracking = getStepTracking(phase, step);
      Long begin = tracking != null ? tracking.beginTime : null;
      Long end = tracking != null ? tracking.endTime : null;
      return getElapsedTime(begin, end);
    }

    public String getFile(Phase phase) {
      return viewPhases.get(phase).file;
    }

    public float getPercentComplete() {
      if (getStatus(Phase.COMPLETE) == Status.RUNNING) {
        return 1.0f;
      } else {
        float total = 0.0f;
        int count = 0;
        for (Phase phase: viewPhases.keySet()) {
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
        for (Step step: getSteps(phase)) {
          count += getCount(phase, step);
        }
        return total > 0 ? 1.0f * count / total : 0.0f;
      }
    }

    public float getPercentComplete(Phase phase, Step step) {
      if (getStatus(phase) == Status.COMPLETE) {
        return 1.0f;
      } else {
        long total = getTotal(phase, step);
        long count = getCount(phase, step);
        return total > 0 ? 1.0f * count / total : 0.0f;
      }
    }

    public Iterable<Phase> getPhases() {
      return VISIBLE_PHASES;
    }

    public Iterable<Step> getSteps(Phase phase) {
      return new TreeSet(viewPhases.get(phase).steps.keySet());
    }

    public Long getSize(Phase phase) {
      return viewPhases.get(phase).size;
    }

    public Status getStatus(Phase phase) {
      PhaseTracking tracking = viewPhases.get(phase);
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
      for (StepTracking tracking: viewPhases.get(phase).steps.values()) {
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

    private View(StartupProgress prog) {
      viewPhases = new HashMap<Phase, PhaseTracking>();
      for (Map.Entry<Phase, PhaseTracking> entry: prog.phases.entrySet()) {
        viewPhases.put(entry.getKey(), entry.getValue().clone());
      }
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

    private StepTracking getStepTracking(Phase phase, Step step) {
      PhaseTracking phaseTracking = viewPhases.get(phase);
      Map<Step, StepTracking> steps = phaseTracking != null ?
        phaseTracking.steps : null;
      return steps != null ? steps.get(step) : null;
    }
  }

  private StepTracking lazyInitStep(Phase phase, Step step) {
    ConcurrentMap<Step, StepTracking> steps = phases.get(phase).steps;
    if (!steps.containsKey(step)) {
      steps.putIfAbsent(step, new StepTracking());
    }
    return steps.get(step);
  }
}
