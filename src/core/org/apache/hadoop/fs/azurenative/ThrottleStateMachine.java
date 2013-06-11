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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.azurenative.AzureFileSystemTimer.AzureFileSystemTimerCallbacks;
import org.apache.hadoop.fs.azurenative.BandwidthThrottle.ThrottleType;

/**
 * Task to calculate success rates on every timer success rate timer tick.
 *
 */
public class ThrottleStateMachine implements BandwidthThrottleFeedback,
                                             AzureFileSystemTimerCallbacks {

  public static final Log LOG = LogFactory.getLog(ThrottleStateMachine.class);

  /**
   * Enum defining throttling types.
   */
  public enum ThrottleState {
    THROTTLE_NONE(0) , THROTTLE_RAMPDOWN(1), THROTTLE_RAMPUP(2);

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
      ThrottleState.THROTTLE_NONE, ThrottleState.THROTTLE_NONE};

  // Throttling adjustment can be made for uploads and downloads.
  //
  private AtomicBoolean[] canAdjustBandwidth   = new AtomicBoolean[] {
      new AtomicBoolean (false), new AtomicBoolean (false)};

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

  private final AtomicLong sampleLatency[] = new AtomicLong [] {
      new AtomicLong (0), new AtomicLong(0)};

  /**
   * Throttle state machine constructor allocates and creates throttle
   * timers.
   */
  public ThrottleStateMachine (String timerName, ScheduledExecutorService timerScheduler,
      final int period, final int stopAfter) {

    // Create interval timer to count successful and unsuccessful upload/download
    // transmissions. The timer has no delay and only stops after there are no
    // throttling events in the current period. After a throttling event, timers
    // are guaranteed to run for two throttling intervals (denoted by the period).
    // The first interval is guaranteed to have a success rate less than one and
    // and the timer only stops after seeing an interval with a success rate of 1.
    //
    // Note: The idea here is to ensure that timer threads should not be wasting
    //       CPU and should only run when something needs timing.
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
   * @param reqLatency - latency for current success feedback
   * @return total number of successful transmissions within the interval
   */
  @Override
  public synchronized long updateTransmissionSuccess(
      ThrottleType kindOfThrottle, final long successCount, final long reqLatency) {

    LOG.info("updateTransmissionSuccess event in state" + getState(kindOfThrottle));

    // Respond to the transmission success event depending on the current state.
    //
    switch (getState(kindOfThrottle)) {
    case THROTTLE_NONE:
    case THROTTLE_RAMPDOWN:
    case THROTTLE_RAMPUP:
      // Accumulate latency for average latency of successful transmissions over
      // over this period.
      //
      succLatency[kindOfThrottle.getValue()][CURRENT_INTERVAL].addAndGet(reqLatency);
      LOG.info("Leaving updateTransmissionSuccess event in state" + getState(kindOfThrottle));
      // Accumulate success count and return.
      //
      return succTxCount[kindOfThrottle.getValue()][CURRENT_INTERVAL].addAndGet(successCount);
    default:
      throw new AssertionError(
        "Received updateTransmissionSuccess event in an unknown state");
    }
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

    LOG.info("updateTransmissionFailure event in state" + getState(kindOfThrottle));

    // Use synchronize block to avoid sleeping while holding a monitor.
    //
    synchronized (this) {
      // Respond to the transmission failure event depending on the current state.
      //
      switch (getState(kindOfThrottle)) {
      case THROTTLE_NONE:
      case THROTTLE_RAMPUP:
        // Set the state to reflect ramp-down. Bandwidth can be adjusted
        // when entering te ramp down state.
        //
        setState(kindOfThrottle, ThrottleState.THROTTLE_RAMPDOWN);
        canAdjustBandwidth[kindOfThrottle.getValue()].set(true);
      case THROTTLE_RAMPDOWN:
        break;
      default:
        throw new AssertionError(
          "Received updateTransmission event in an unknown state");
      }

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

      LOG.info("Leaving updateTransmissionFailure event in state" + getState(kindOfThrottle));
    }

    // Sleep for the difference between to compensate for low latency of
    // failed requests and only if sleep times differ by a LATENCY_DELTA margin.
    //
    try {
      long delayMs = getLatency(kindOfThrottle) - reqLatency;
      
      final String infoMsg =
          String.format ("Thread Id: %d, Period Latency: %d, SampleLatency: %d, " +
                         "RequestLatency: %d, Delay: %d",
                         Thread.currentThread().getId(), getLatency(kindOfThrottle), 
                         getSampleLatency(kindOfThrottle),  reqLatency, delayMs);
      LOG.info(infoMsg);
      
      if (delayMs > LATENCY_DELTA) {
        Thread.sleep(delayMs);
      }
    } catch (InterruptedException e) {
      // Do nothing on interrupted sleeps.
    }

    // Accumulate failure count and return.
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
  public synchronized float getCurrentTxSuccessRate(ThrottleType kindOfThrottle) {
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
  public synchronized float getPreviousTxSuccessRate(ThrottleType kindOfThrottle) {
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
   * Query the average latency for all successful requests. This chooses the valid
   * latency from either the current latency values or the previous latency.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return - positive value for valid latencies, otherwise a non-positive value.
   */
  public synchronized long getLatency (ThrottleType kindOfThrottle) {
    long latency = getCurrentLatency(kindOfThrottle);
    if (latency <= 0) {
      latency = getPreviousLatency(kindOfThrottle);
    }

    // Return valid average latency over either the previous interval or
    // the current interval.
    //
    return latency;
  }

  /**
   * Query the current average latency for all successful requests.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return - positive value for valid latencies, otherwise a non-positive value.
   */
  public synchronized long getCurrentLatency (ThrottleType kindOfThrottle) {
    long succCount = succTxCount[kindOfThrottle.getValue()][0].get();
    if (0 == succCount) {
      return -1;
    }

    // Return current average latency over interval.
    //
    return succLatency[kindOfThrottle.getValue()][0].get() / succCount;
  }

  /**
   * Query the current sample latency for all successful requests.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return - positive value for valid latencies, otherwise a non-positive value.
   */
  public synchronized long getSampleLatency (ThrottleType kindOfThrottle) {
    // Return current average latency over interval.
    //
    return sampleLatency[kindOfThrottle.getValue()].get();
  }

  /**
   * Query the previous average latency for all successful requests.
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return - positive value for valid latencies, otherwise a non-positive value.
   */
  public synchronized long getPreviousLatency (ThrottleType kindOfThrottle) {
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
   * @param kindOfThrottle - denotes download or upload throttling.
   * @param newState - set the state of the simple state machine to this state.
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
   * @param kindOfThrottle - denotes download or upload throttling.
   * @return current state of the state machine
   */
  public synchronized ThrottleState getState (ThrottleType kindOfThrottle) {
    // Return the current state.
    //
    return throttleState[kindOfThrottle.getValue()];
  }

  /**
   * Implementation of the tickEvent for the AzureFileSytemTimerCallback interface.
   * @param timerCallbackContext - context to discriminate between timers.
   */
  @Override
  public synchronized void tickEvent(Object timerCallbackContext) {

    // First determine if this tick callback for downloads or upload from
    // the timer callback context.
    //
    ThrottleType kindOfThrottle = (ThrottleType) timerCallbackContext;

    LOG.info("Received tickEvent event in state" + getState(kindOfThrottle));

    // Respond to the transmission success event depending on the current state.
    //
    switch (getState(kindOfThrottle)) {
    case THROTTLE_NONE:
      throw new AssertionError(
          "Received timerTick event when not in a throttling state");
    case THROTTLE_RAMPDOWN:
    case THROTTLE_RAMPUP:
      break;

    default:
      throw new AssertionError(
        "Received updateTransmissionSuccess event in an unknown state");
    }

    // At this point the state machine is either ramping up or ramping down
    // bandwidth.
    //
    // Roll over success rate metrics and start fresh for the next
    // throttling interval. Remain in the throttling state.
    //
    rollSuccessRateMetrics(kindOfThrottle);

    // Check for throttling events over the current throttling interval.
    //
    if (getPreviousTxSuccessRate(kindOfThrottle) < 1.0) {
      // Since failure were incurred in the last timer period, change state
      // to reflect ramp-down.
      //
      setState (kindOfThrottle, ThrottleState.THROTTLE_RAMPDOWN);
    } else {
      // All transmissions in the previous interval have been successful.
      //
      // Note: If there were not transmissions in the current interval, that is also
      //       treated as 100% successful.
      //
      ThrottleState prevState = setState(kindOfThrottle, ThrottleState.THROTTLE_RAMPUP);

      // Assertion: Ramp-up never occurs from a stable THROTTLE_NONE state.
      //
      if (ThrottleState.THROTTLE_NONE == prevState) {
        throw new AssertionError(
            "Unexpected transition from " + prevState + " to " +
                   ThrottleState.THROTTLE_RAMPUP);
      }
    }

    // Check if the timer has ticked over two intervals and expired.
    //
    if (throttleTimers[kindOfThrottle.getValue()].isExpired()) {
      // Timer expired, restart timer.
      //
      throttleTimers[kindOfThrottle.getValue()].turnOnTimer(this, kindOfThrottle);

      // At the end of a throttling interval, bandwidth adjustments can be made.
      //
      canAdjustBandwidth[kindOfThrottle.getValue()].set(true);
    }

    LOG.info("Leaving tickEvent event in state" + getState(kindOfThrottle));
  }

  /**
   * Ramp-up upcall from the throttling engine polling to determine whether or not
   * can ramp-up bandwidth. This is not a simple query since it flips the
   * canAdjustBandwidth state to false.
   *
   * Note: Introduced polling function to keep the simple state machine completely
   *       passive and improve safety by not calling back on notifications from other
   *       potentially unsafe method objects.
   *
   * @param kindOfThrottle - upload/download throttle.
   */
  public synchronized boolean rampUp (ThrottleType kindOfThrottle) {

    LOG.info("Received rampUp event in state" + getState(kindOfThrottle));

    if (getState(kindOfThrottle) == ThrottleState.THROTTLE_RAMPUP &&
        canAdjustBandwidth[kindOfThrottle.getValue()].get()) {

      // Bandwidth adjustments prevented from now on.
      //
      canAdjustBandwidth[kindOfThrottle.getValue()].set(false);

      LOG.info("Leaving rampUp event (1) in state" + getState(kindOfThrottle));

      // It is OK to ramp-up.
      //
      return true;
    }

    LOG.info("Leaving rampUp event (2) in state" + getState(kindOfThrottle));

    // It is not OK to ramp-up.
    //
    return false;
  }

  /**
   * Ramp-down upcall from the throttling engine polling to determine whether or not
   * can ramp-down bandwidth. This is not a simple query since it flips the
   * canAdjustBandwidth state to false.
   *
   * Note: Introduced polling function to keep the simple state machine completely
   *       passive and improve safety by not calling back on notifications from other
   *       potentially unsafe method objects.
   *
   * @param kindOfThrottle - upload/download throttle.
   */
  public synchronized boolean rampDown (ThrottleType kindOfThrottle) {

    LOG.info("Received rampDown event in state" + getState(kindOfThrottle));

    if (getState(kindOfThrottle) == ThrottleState.THROTTLE_RAMPDOWN &&
        canAdjustBandwidth[kindOfThrottle.getValue()].get()) {

      // Bandwidth adjustments prevented from now on.
      //
      canAdjustBandwidth[kindOfThrottle.getValue()].set(false);

      LOG.info("Leaving rampDown event (1) in state" + getState(kindOfThrottle));

      // It is OK to ramp-up.
      //
      return true;
    }
    LOG.info("Leaving rampDown event (2) in state" + getState(kindOfThrottle));

    // It is not OK to ramp-up.
    //
    return false;
  }

  /**
   * Tell the state machine to stop throttling and return it to the virgin
   * non-throttling state.
   *
   * @param kindOfThrottle - upload/download throttle.
   */
  public synchronized void stopThrottling (ThrottleType kindOfThrottle) {

    LOG.info("Received stopThrottling event in state" + getState(kindOfThrottle));

    // Respond to the transmission success event depending on the current state.
    //
    switch (getState(kindOfThrottle)) {
    case THROTTLE_NONE:
      // Assert that the timer is turned off then return immediately. This makes
      // stopThrottling idempotent.
      //
      if (throttleTimers[kindOfThrottle.getValue()].isOn()) {
        throw new AssertionError(
            "Throttling timer is on when throttling state machine in non-throttling mode.");
      }
      break;
    case THROTTLE_RAMPDOWN:
      // Assertion failed. stopThrottling events are not expected in the ramp down state.
      //
      throw new AssertionError(
         "Unexpected stopThrotlling event in the RAMPDOWN state.");
    case THROTTLE_RAMPUP:
      // Turn off throttling timer and rollover metrics.  Note there is no need to
      // rollover metrics over the current interval. A rollover will occur on the next
      // throttling event.
      //
      throttleTimers[kindOfThrottle.getValue()].turnOffTimer();

      // Make sure no bandwidth adjustments can be made.
      //
      canAdjustBandwidth[kindOfThrottle.getValue()].set(false);
      break;
    default:
      throw new AssertionError(
        "Received stopThrottlingEvent event in an unknown state");
    }

    // Set the throttling state THROTTLE_NONE.
    //
    setState(kindOfThrottle, ThrottleState.THROTTLE_NONE);

    // Save current latency as the sample latency.
    //
    sampleLatency[kindOfThrottle.getValue()].set(getLatency(kindOfThrottle));

    LOG.info("Leaving stopThrottling event in state" + getState(kindOfThrottle));
  }
}
