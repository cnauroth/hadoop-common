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

package org.apache.hadoop.fs.azurenative;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.azurenative.AzureFileSystemTimer.AzureFileSystemTimerCallbacks;
import org.apache.hadoop.fs.azurenative.BandwidthThrottle.ThrottleType;

/**
 * Task to calculate success rates on every timer success rate timer tick.
 *
 */
public class ThrottleStateMachine implements BandwidthThrottleFeedback,
                                             AzureFileSystemTimerCallbacks {

  /**
   * Enum defining throttling types.
   */
  public enum ThrottleState {
    NORMAL(0) , THROTTLING_STABLE (2), THROTTLING_ADJUST_BANDWIDTH (3);

    private final int value;
    private ThrottleState(int value) { this.value = value; }

    public int getValue() {
      return this.value;
    }
  }

  // Constant strings
  //
  private static final String THROTTLE_STATE_MACHINE_SUFFIX = "ThrottleStateMachine";

  // Member variables
  //
  private static final int CURRENT_INTERVAL  = 0; // Transmission success.
  private static final int PREVIOUS_INTERVAL = 1; // Transmission failure.
  private static final long LATENCY_DELTA    = 100; // Sampled/request latency tolerance.
  private AzureFileSystemTimer throttleTimers[] = {null, null}; // Upload/download timers.

  // Throttling state for uploads and downloads.
  //
  private ThrottleState throttleState[] = new ThrottleState[] {
      ThrottleState.NORMAL, ThrottleState.NORMAL};

  // Two dimensional accumulators.  Rows correspond to downloads and uploads, and the
  // columns keep track of recent history from most recent to oldest.
  //
  private final AtomicLong succTxCount[][] = new AtomicLong [][] {
      {new AtomicLong (0), new AtomicLong (0)},
      {new AtomicLong (0), new AtomicLong (0)}};

  private final AtomicLong failTxCount[][] = new AtomicLong [][] {
      {new AtomicLong (0), new AtomicLong (0)},
      {new AtomicLong (0), new AtomicLong (0)}};

  private final AtomicLong succLatency[][] = new AtomicLong [][] {
      {new AtomicLong (0), new AtomicLong (0)},
      {new AtomicLong (0), new AtomicLong (0)}};

  /**
   * Throttle state machine constructor allocates and creates throttle
   * timers.
   */
  public ThrottleStateMachine (String timerName, ScheduledExecutorService timerScheduler,
      final int period, final int stopAfter) {

    // Create interval timer to count successful and unsuccessful upload/download
    // transmissions. The timer has no delay and automatically stops
    //
    throttleTimers = new AzureFileSystemTimer [] {
        new AzureFileSystemTimer(
            timerName + "_" + ThrottleType.UPLOAD + "_" + THROTTLE_STATE_MACHINE_SUFFIX,
            timerScheduler, 0, period, stopAfter),

        new AzureFileSystemTimer(
            timerName + "_" + ThrottleType.DOWNLOAD + "_" + THROTTLE_STATE_MACHINE_SUFFIX,
            timerScheduler, 0, period, stopAfter)
    };
  }

  /**
   * Accumulate successful transmissions with the success rate timer interval.
   *
   * @param successCount - delta in the number of successful transmissions
   * @param latency - latency for current success feedback
   * @return total number of successful transmissions within the interval
   */
  @Override
  public long updateTransmissionSuccess(
      ThrottleType kindOfThrottle, final long successCount, final long reqLatency) {

    // Accumulate latency for average latency of successful transmissions over
    // over this period.
    //
    succLatency[kindOfThrottle.getValue()][CURRENT_INTERVAL].addAndGet(reqLatency);

    // The state machine is a state where it is still making bandwidth adjustments,
    // set the throttling state to a stabilizing state, since there are successful
    // transmissions.
    //
    // Note: There may be a race between testing with the get and setting the state.
    //       However, this is expected to be rare and the resulting condition should
    //       not affect performance.
    //
    if (getState (kindOfThrottle) != ThrottleState.THROTTLING_ADJUST_BANDWIDTH){
      setState(kindOfThrottle, ThrottleState.THROTTLING_STABLE);
    }

    // Accumulate success count and return.
    //
    return succTxCount[kindOfThrottle.getValue()][CURRENT_INTERVAL].addAndGet(successCount);
  }
  
  /**
   * Accumulate failed transmissions within the success rate timer interval.
   * Synchronized method for safe state queries and state changes.
   *
   * @param failureCount - delta in the number of successful transmissions
   * @return total number of failed transmissions within the interval
   */
  @Override
  public synchronized long updateTransmissionFailure(ThrottleType kindOfThrottle,
      final long failureCount, final long reqLatency) {

    // Change throttling state with bandwidth adjustment and start timer if it is
    // not already started.
    //
    setState(kindOfThrottle, ThrottleState.THROTTLING_ADJUST_BANDWIDTH);

    // Turn on throttling timer if it is not already turned on.
    //
    if (throttleTimers[kindOfThrottle.getValue()].isOff ()) {

      // Timer ticks are callback events into the state machine. Ticks are
      // discriminated based on their upload/download throttle types in the
      // callback context.
      //
      throttleTimers[kindOfThrottle.getValue()].turnOnTimer(this, kindOfThrottle);

      // Roll success rate metrics by overwriting previous values with current
      // values and resetting previous values.
      //
      rollSuccessRateMetrics(kindOfThrottle);
    }
    
    // Capture the sampled latency.
    //
    long sampledLatency = getCurrentLatency(kindOfThrottle);
    if (sampledLatency <= 0) {
      sampledLatency = getPreviousLatency(kindOfThrottle);
    }
    
    // Sleep for the difference between to compensate for low latency of
    // failed requests and only if sleep times differ by a LATENCY_DELTA margin.
    //
    try {
      long delayMs = sampledLatency - reqLatency;
      if (delayMs > LATENCY_DELTA) {
        Thread.sleep(delayMs);
      }
    } catch (InterruptedException e) {
      // Do nothing on interrupted sleeps.
    }
    
    // Accumulate success count and return.
    //
    return failTxCount[kindOfThrottle.getValue()][CURRENT_INTERVAL].
        addAndGet(failureCount);
  }

  /**
   * A utility method to roll success rate values by overwriting previous values
   * with current values and resetting current values.
   *
   * @parameter kindOfThrottle - upload/download throttling
   */
  private synchronized void rollSuccessRateMetrics (ThrottleType kindOfThrottle) {
    // Roll current values over previous values.
    //
    succTxCount[kindOfThrottle.getValue()][PREVIOUS_INTERVAL].
      set(succTxCount[kindOfThrottle.getValue()][CURRENT_INTERVAL].get());

    failTxCount[kindOfThrottle.getValue()][PREVIOUS_INTERVAL].
      set(failTxCount[kindOfThrottle.getValue()][CURRENT_INTERVAL].get());

    succLatency[kindOfThrottle.getValue()][PREVIOUS_INTERVAL].
      set(succLatency[kindOfThrottle.getValue()][CURRENT_INTERVAL].get());

    // Reset current values/
    //
    succTxCount[kindOfThrottle.getValue()][CURRENT_INTERVAL].set(0);
    failTxCount[kindOfThrottle.getValue()][CURRENT_INTERVAL].set(0);
    succLatency[kindOfThrottle.getValue()][CURRENT_INTERVAL].set(0);
  }

  /**
   * Query the instantaneous success rate instead of waiting for the
   * expiration of the ramp-down interval.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return the current transmission success rate for this epoch.
   */
  public float getCurrentTxSuccessRate(ThrottleType kindOfThrottle) {
    float currSuccessRate = 1.0f;
    float succCount = (float) succTxCount[kindOfThrottle.getValue()][0].get();
    float failCount = (float) failTxCount[kindOfThrottle.getValue()][0].get();

    if (succCount == 0.0f && failCount == 0.0f) {
      return currSuccessRate;
    }

    // Calculate the current success rate.
    //
    currSuccessRate = succCount / (succCount + failCount);
    return currSuccessRate;
  }

  /**
   * Query the success during the previous timer interval interval.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return the previous transmission success rate for this epoch.
   */
  public float getPreviousTxSuccessRate(ThrottleType kindOfThrottle) {
    float prevSuccessRate = 1.0f;
    float succCount =
        (float) succTxCount[kindOfThrottle.getValue()][PREVIOUS_INTERVAL].get();
    float failCount =
        (float) failTxCount[kindOfThrottle.getValue()][PREVIOUS_INTERVAL].get();

    if (succCount == 0.0f && failCount == 0.0f) {
      return prevSuccessRate;
    }

    // Calculate the current success rate.
    //
    prevSuccessRate = succCount / (succCount + failCount);
    return prevSuccessRate;
  }

  /**
   * Query the current average latency for all successful requests.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return - positive value for valid latencies, otherwise a non-positive value.
   */
  public long getCurrentLatency (ThrottleType kindOfThrottle) {
    long succCount = succTxCount[kindOfThrottle.getValue()][0].get();
    if (0 == succCount) {
      return -1;
    }

    // Return current average latency over interval.
    //
    return succLatency[kindOfThrottle.getValue()][0].get() / succCount;
  }

  /**
   * Query the previous average latency for all successful requests.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return - positive value for valid latencies, otherwise a non-positive value.
   */
  public long getPreviousLatency (ThrottleType kindOfThrottle) {
    long succCount = succTxCount[kindOfThrottle.getValue()][1].get();
    if (0 == succCount) {
      return -1;
    }

    // Return current average latency over interval.
    //
    return succLatency[kindOfThrottle.getValue()][1].get() / succCount;
  }

  /**
   * Set the state of the simple upload/download state machine.
   */
  /**
   * Get the state of the simple upload/download state machine.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @param newState - set the state of the simple state machine to this state.
   * @returns previous state of the state machine
   */
  public synchronized ThrottleState setState (ThrottleType kindOfThrottle,
      ThrottleState newState) {
    // Return the previous state.
    //
    ThrottleState prevState = throttleState[kindOfThrottle.getValue()];
    throttleState[kindOfThrottle.getValue()] = newState;
    return prevState;
  }

  /**
   * Get the state of the simple upload/download state machine.
   */
  public synchronized ThrottleState getState (ThrottleType kindOfThrottle) {
    // Return the current state.
    //
    return throttleState[kindOfThrottle.getValue()];
  }

  /**
   * Implementation of the tickEvent for the AzureFileSytemTimerCallback interface.
   */
  @Override
  public void tickEvent(Object timerCallbackContext) {

    // First determine if this tick callback for downloads or upload from
    // the timer callback context.
    //
    ThrottleType kindOfThrottle = (ThrottleType) timerCallbackContext;

    // Determine whether this timer event corresponds to the expiration of a
    // throttling interval.
    //
    // Note: The state assertions are not factored out for the expired and non-expired
    //       cases below since we want to get the current throttle state in one
    //       atomic operation.
    //
    ThrottleState prevState;

    if (throttleTimers[kindOfThrottle.getValue()].isExpired()) {
      // Expiration of a throttling interval. Set the throttling state to NORMAL.
      //
      prevState = setState(kindOfThrottle, ThrottleState.NORMAL);
    } else {
      // Throttling interval is not expired, simply capture state prior to tick.
      //
      prevState = getState(kindOfThrottle);
    }

    // Assertion: Previous state of the state machine must be throttling in
    //            if it is receiving timer ticks.
    //
    if (ThrottleState.NORMAL == prevState) {
      throw new AssertionError (prevState +
          " state unexpected for " + kindOfThrottle + " throttling state machine." +
          " Expected state: " + ThrottleState.THROTTLING_STABLE + " or " +
          ThrottleState.THROTTLING_ADJUST_BANDWIDTH);
    }

    // Roll over success rate metrics and start fresh for the next throttling epoch.
    //
    rollSuccessRateMetrics(kindOfThrottle);
  }
}
