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
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase.*;
import static org.apache.hadoop.util.Time.monotonicNow;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.hadoop.classification.InterfaceAudience;

@InterfaceAudience.Private
public class StartupProgress {

  Map<Phase, PhaseTracking> phases =
    new ConcurrentHashMap<Phase, PhaseTracking>();

  public interface Counter {
    void increment();
  }

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

  public StartupProgressView createView() {
    return new StartupProgressView(this);
  }


  private StepTracking lazyInitStep(Phase phase, Step step) {
    ConcurrentMap<Step, StepTracking> steps = phases.get(phase).steps;
    if (!steps.containsKey(step)) {
      steps.putIfAbsent(step, new StepTracking());
    }
    return steps.get(step);
  }
}
