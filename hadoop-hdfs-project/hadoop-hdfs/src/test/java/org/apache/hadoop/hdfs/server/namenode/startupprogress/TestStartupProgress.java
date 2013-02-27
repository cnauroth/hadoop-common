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
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.Status.*;
import static org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

public class TestStartupProgress {

  private StartupProgress startupProgress;

  @Before
  public void setUp() {
    startupProgress = new StartupProgress();
  }

  @Test(timeout=10000)
  public void testCounter() {
    startupProgress.beginPhase(LOADING_FSIMAGE);
    Step loadingFsImageInodes = new Step(INODES);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageInodes);
    startupProgress.setTotal(LOADING_FSIMAGE, loadingFsImageInodes, 1000L);
    incrementCounter(LOADING_FSIMAGE, loadingFsImageInodes, 100L);
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageInodes);
    Step loadingFsImageDelegationKeys = new Step(DELEGATION_KEYS);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.setTotal(LOADING_FSIMAGE, loadingFsImageDelegationKeys,
      800L);
    incrementCounter(LOADING_FSIMAGE, loadingFsImageDelegationKeys, 200L);
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.endPhase(LOADING_FSIMAGE);

    startupProgress.beginPhase(LOADING_EDITS);
    Step loadingEditsFile = new Step("file", 1000L);
    startupProgress.beginStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.setTotal(LOADING_EDITS, loadingEditsFile, 10000L);
    incrementCounter(LOADING_EDITS, loadingEditsFile, 5000L);
    startupProgress.endStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.endPhase(LOADING_EDITS);

    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(100L, view.getCount(LOADING_FSIMAGE, loadingFsImageInodes));
    assertEquals(200L, view.getCount(LOADING_FSIMAGE,
      loadingFsImageDelegationKeys));
    assertEquals(5000L, view.getCount(LOADING_EDITS, loadingEditsFile));
    assertEquals(0L, view.getCount(SAVING_CHECKPOINT,
      new Step(StepType.INODES)));
  }

  @Test(timeout=10000)
  public void testElapsedTime() throws Exception {
    startupProgress.beginPhase(LOADING_FSIMAGE);
    Step loadingFsImageInodes = new Step(INODES);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageInodes);
    Thread.sleep(50L); // brief sleep to fake elapsed time
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageInodes);
    Step loadingFsImageDelegationKeys = new Step(DELEGATION_KEYS);
    startupProgress.beginStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    Thread.sleep(50L); // brief sleep to fake elapsed time
    startupProgress.endStep(LOADING_FSIMAGE, loadingFsImageDelegationKeys);
    startupProgress.endPhase(LOADING_FSIMAGE);

    startupProgress.beginPhase(LOADING_EDITS);
    Step loadingEditsFile = new Step("file", 1000L);
    startupProgress.beginStep(LOADING_EDITS, loadingEditsFile);
    startupProgress.setTotal(LOADING_EDITS, loadingEditsFile, 10000L);
    incrementCounter(LOADING_EDITS, loadingEditsFile, 5000L);
    Thread.sleep(50L); // brief sleep to fake elapsed time

    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertTrue(view.getElapsedTime() > 0);

    assertTrue(view.getElapsedTime(Phase.LOADING_FSIMAGE) > 0);
    assertTrue(view.getElapsedTime(Phase.LOADING_FSIMAGE,
      loadingFsImageInodes) > 0);
    assertTrue(view.getElapsedTime(Phase.LOADING_FSIMAGE,
      loadingFsImageDelegationKeys) > 0);

    assertTrue(view.getElapsedTime(LOADING_EDITS) > 0);
    assertTrue(view.getElapsedTime(LOADING_EDITS, loadingEditsFile) > 0);

    assertTrue(view.getElapsedTime(SAVING_CHECKPOINT) == 0);
    assertTrue(view.getElapsedTime(SAVING_CHECKPOINT,
      new Step(StepType.INODES)) == 0);

    // Brief sleep, then check that completed phases/steps have the same elapsed
    // time, but running phases/steps have updated elapsed time.
    long totalTime = view.getElapsedTime();
    long loadingFsImageTime = view.getElapsedTime(LOADING_FSIMAGE);
    long loadingFsImageInodesTime = view.getElapsedTime(LOADING_FSIMAGE,
      loadingFsImageInodes);
    long loadingFsImageDelegationKeysTime = view.getElapsedTime(LOADING_FSIMAGE,
      loadingFsImageInodes);
    long loadingEditsTime = view.getElapsedTime(LOADING_EDITS);
    long loadingEditsFileTime = view.getElapsedTime(LOADING_EDITS,
      loadingEditsFile);

    Thread.sleep(50L);

    assertTrue(totalTime < view.getElapsedTime());
    assertEquals(loadingFsImageTime, view.getElapsedTime(LOADING_FSIMAGE));
    assertEquals(loadingFsImageInodesTime, view.getElapsedTime(LOADING_FSIMAGE,
      loadingFsImageInodes));
    assertTrue(loadingEditsTime < view.getElapsedTime(LOADING_EDITS));
    assertTrue(loadingEditsFileTime < view.getElapsedTime(LOADING_EDITS,
      loadingEditsFile));
  }

  @Test(timeout=10000)
  public void testInitialState() {
    StartupProgressView view = startupProgress.createView();
    assertEquals(0L, view.getElapsedTime());
    assertEquals(0.0f, view.getPercentComplete(), 0.001f);
    List<Phase> phases = new ArrayList<Phase>();

    for (Phase phase: view.getPhases()) {
      phases.add(phase);
      assertEquals(0L, view.getElapsedTime(phase));
      assertNull(view.getFile(phase));
      assertEquals(0.0f, view.getPercentComplete(phase), 0.001f);
      assertNull(view.getSize(phase));
      assertEquals(PENDING, view.getStatus(phase));
      assertEquals(0L, view.getTotal(phase));

      for (Step step: view.getSteps(phase)) {
        fail(String.format("unexpected step %s in phase %s at initial state",
          step, phase));
      }
    }

    assertArrayEquals(Phase.VISIBLE_PHASES.toArray(), phases.toArray());
  }

  @Test(timeout=10000)
  public void testMultiplePhasesAndSteps() {
  }

  @Test(timeout=10000)
  public void testPercentComplete() {
  }

  /**
   * Tests reporting of a phase's status.
   */
  @Test(timeout=10000)
  public void testStatus() {
    startupProgress.beginPhase(LOADING_FSIMAGE);
    startupProgress.endPhase(LOADING_FSIMAGE);
    startupProgress.beginPhase(LOADING_EDITS);
    StartupProgressView view = startupProgress.createView();
    assertNotNull(view);
    assertEquals(Status.COMPLETE, view.getStatus(LOADING_FSIMAGE));
    assertEquals(Status.RUNNING, view.getStatus(LOADING_EDITS));
    assertEquals(Status.PENDING, view.getStatus(SAVING_CHECKPOINT));
  }

  @Test(timeout=10000)
  public void testSteps() {
  }

  @Test(timeout=10000)
  public void testThreadSafety() {
  }

  @Test(timeout=10000)
  public void testTotal() {
  }

  private void incrementCounter(Phase phase, Step step, long delta) {
    StartupProgress.Counter counter = startupProgress.getCounter(phase, step);
    for (long i = 0; i < delta; ++i) {
      counter.increment();
    }
  }
}
