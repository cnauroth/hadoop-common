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

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureException;
import com.microsoft.windowsazure.services.core.ConfigurationException;

/**
 * ThrottleSendRequest object implementing the ThrottleSendRequestCallback and the
 * BandwidthThrottleFeedback interfaces.  These interfaces are used to calculate the
 * throttling delays and track send transmission successes/failures, respectively.
 */
public class BandwidthThrottle implements ThrottleSendRequestCallback {

  /**
   * Enumerator defining throttling types.
   */
  public enum ThrottleType {
    UPLOAD(0) , DOWNLOAD (1);

    private final int value;
    private ThrottleType(int value) { this.value = value; }

    public int getValue() {
      return value;
    }
  }

  public static final Log LOG = LogFactory.getLog(BandwidthThrottle.class);

  // Scheduler for timer threads.
  //
  private static ScheduledExecutorService timerScheduler;

  // String constants.
  //
  private static final String ASV_TIMER_NAME = "AsvStream";
  private static final String KEY_THROTTLING_PERIOD = "fs.azure.success.throttling.period";
  private static final String KEY_MAX_THROTTLE_DELAY = "fs.azure.max.throttle.delay";
  private static final String KEY_BANDWIDTH_RAMPUP_MULTIPLIER = "fs.azure.rampup.multiplier";
  private static final String KEY_BANDWIDTH_RAMPDOWN_MULTIPLIER = "fs.azure.rampdown.multiplier";

  private static final long[] DEFAULT_LATENCY =
    {500, /*500 ms upload*/ 300 /*300ms download*/};

  private static int ONE_SECOND = 1000;  // 1000 milliseconds
  private static final int DEFAULT_THROTTLING_PERIOD = 30 * ONE_SECOND;  // 30 seconds
  private static final int DEFAULT_MAX_THROTTLE_DELAY = 10 * ONE_SECOND; // 10 seconds.
  private static final float DEFAULT_BANDWIDTH_RAMPUP_MULTIPLIER  = 1.15f;
  private static final float DEFAULT_BANDWIDTH_RAMPDOWN_MULTIPLIER = 0.75f;
  private static final float DEFAULT_SUCCESS_RATE = 0.50f; // Default success rate.

  private static final int THROTTLING_INTERVALS = 2;//Sample for two throttling intervals.

  // Other constants.
  //

  private static final int TIMER_SCHEDULER_CONCURRENCY = 2; // Number of scheduler threads.
  private Configuration sessionConfiguration;

  // Member variables capturing throttle parameters.
  // Note: There is no need for member variables for the download and upload block sizes
  //       since they are captured by the storage session context.
  //
  private int    maxThrottleDelay;
  private int    throttlingPeriod;
  private long   minBandwidthRampupDelta;
  private double bandwidthRampupMultiplier;
  private double bandwidthRampdownMultiplier;

  private long[] blockSize;
  private long[] bandwidthTarget;
  private long[] maxBandwidthTarget;
  private long[] bandwidthRampupDelta;

  private ThrottleStateMachine throttleSM;

  /**
   * Constructor for the ThrottleSendRequest object.
   * @throws AzureException
   */
  public BandwidthThrottle (int uploadBlockSize, int downloadBlockSize,
      Configuration sessionConfiguration) throws ConfigurationException {
    this.sessionConfiguration = sessionConfiguration;

    // Initialize throttling parameters and variables.
    //
    configureSessionThrottling(uploadBlockSize, downloadBlockSize);
  }

  /**
   * Configure the throttling parameters. The throttling parameters are set
   * in the core-site.xml configuration file. If the parameter is not set in
   * the configuration object the default is used.
   *
   * @param concurrentReads - number of concurrent read threads for overlapping I/O.
   * @param concurrentWrites - number of concurrent write threads for overlapping I/O.
   * @throws ConfigurationException if there are errors in configuration.
   */
  private void configureSessionThrottling(int uploadBlockSize, int downloadBlockSize)
      throws ConfigurationException {

    // Minimum allowable bandwidth consumption.
    //
    maxThrottleDelay = sessionConfiguration.getInt(
        KEY_MAX_THROTTLE_DELAY,
        DEFAULT_MAX_THROTTLE_DELAY);
    if (maxThrottleDelay <= 0) {
      // Throw a configuration exception. The cluster should have at least 1
      // node.
      //
      String errMsg =
          String.format(
              "Maximum throttling delay must be greater than 0." +
              "Current throttling delay is: %d", maxThrottleDelay);
      throw new ConfigurationException(errMsg);
    }

    // Initialize the upload and download block sizes.
    //
    blockSize = new long [] {(long) uploadBlockSize, (long) downloadBlockSize};

    // Initialize maximum upload and download target bandwidths.
    //
    maxBandwidthTarget = new long [] {
        blockSize[0] / DEFAULT_LATENCY[0],
        blockSize[1] / DEFAULT_LATENCY[1]};

    // Initially set the current target bandwidths to the maximum target bandwidths.
    //
    bandwidthTarget = Arrays.copyOf(maxBandwidthTarget, maxBandwidthTarget.length);

    // Interval where throttling events and request latencies are sampled.
    //
    throttlingPeriod = sessionConfiguration.getInt(
        KEY_THROTTLING_PERIOD, DEFAULT_THROTTLING_PERIOD);
    if (throttlingPeriod <= 0) {
      // The throttling period configuration must be at least 1 second.
      //
      String errMsg =
          String.format(
              "Cluster is configured with a throttling period period of %d" +
                  " The failure tolerance period should be at least 1 ms.",
                  throttlingPeriod);
      throw new ConfigurationException(errMsg);
    }

    // Create and initialize the bandwidth ramp-up delta for both uploads
    // and downloads.
    //
    bandwidthRampupDelta =
        new long [] {minBandwidthRampupDelta, minBandwidthRampupDelta};

    // Multiplier on the ramp up delta. This coefficient determines whether ramp
    // up is sub-linear, linear, or non-linear determining whether ramp-up rate of
    // change decreases, remains the same, or increases with time across ramp up
    // intervals.
    //
    bandwidthRampupMultiplier = sessionConfiguration.getFloat(
        KEY_BANDWIDTH_RAMPUP_MULTIPLIER,
        DEFAULT_BANDWIDTH_RAMPUP_MULTIPLIER);
    if (bandwidthRampupMultiplier < 0) {
      // The bandwidth ramp-up multiplier configuration be greater than one.
      //
      String errMsg =
          String.format(
              "Cluster is configured with a bandwidth ramp-up multiplier of %f" +
                  " The bandwidth ramp-up multiplier should be greater than one.",
                  bandwidthRampupMultiplier);
      throw new ConfigurationException(errMsg);
    }

    // Multiplier weighting the ramp-down rate.
    //
    bandwidthRampdownMultiplier = sessionConfiguration.getFloat(
        KEY_BANDWIDTH_RAMPDOWN_MULTIPLIER,
        DEFAULT_BANDWIDTH_RAMPDOWN_MULTIPLIER);
    if (bandwidthRampdownMultiplier < 0) {
      // The bandwidth ramp-down multiplier configuration be greater than one.
      //
      String errMsg =
          String.format(
              "Cluster is configured with a bandwidth ramp-down multiplier of %f" +
                  " The bandwidth ramp-down multiplier should be greater than one.",
                  bandwidthRampdownMultiplier);
      throw new ConfigurationException(errMsg);
    }

    // Start bandwidth throttling.
    //
    throttleBandwidth();

    // Create throttling state machine to drive throttling events on the bandwidth
    // throttling engine but only if throttling is enabled.
    //
    throttleSM = new ThrottleStateMachine(
        ASV_TIMER_NAME, getTimerScheduler(), throttlingPeriod, THROTTLING_INTERVALS);
  }

  /**
   * Query the timer scheduler.
   * @return - current timer scheduler if started, null otherwise.
   */
  private static ScheduledExecutorService getTimerScheduler() {
    return timerScheduler;
  }

  /**
   * Bandwidth throttling initializer to start success rate interval timer and
   * begin accumulated transmission success rates. This method is synchronized
   * since it is called by all instances of the AzureNativeFileSystemStore class
   * for this process. Only the first instance of the of this class would perform
   * the initialization.
   *
   * @param period - frequency in seconds at which success rate is calculated.
   */
  private static synchronized void throttleBandwidth() {
    // Start up timer scheduler if one does not already exist.
    //
    if (null == timerScheduler){
      timerScheduler = Executors.newScheduledThreadPool(TIMER_SCHEDULER_CONCURRENCY);
    }
  }

  /**
   * Return the bandwidth throttling feedback interface on the state machine. This
   * method is used when binding to receive replay callbacks.
   *
   * @return BandwidthThrottleFeedback interface implemented by the state machine.
   */
  public BandwidthThrottleFeedback getBandwidthThrottleFeedback (){
    // Return the throttling state machine.
    //
    return throttleSM;
  }

  /**
   * Determine if the send request should be throttled.
   *
   * @param payloadSize - size of the read or write payload on connection.
   */
  @Override
  public synchronized void throttleSendRequest(
      ThrottleType kindOfThrottle, long payloadSize) {

    LOG.info("Entering throttleSendRequest...");

    // Determine the current sampled latency.
    //
    long latency = throttleSM.getSampleLatency(kindOfThrottle);
    if (latency <= 0){
      // Set latency should never be less than or equal to zero.  Set the latency
      // to the default latency.
      //
      latency = DEFAULT_LATENCY[kindOfThrottle.getValue()];
    }

    // Calculate the maximum bandwidth target.
    //
    maxBandwidthTarget[kindOfThrottle.getValue()] =
                  blockSize[kindOfThrottle.getValue()] / latency;

    // Check if to ramp-up, ramp-down, or stop throttling.
    //
    if (throttleSM.rampUp(kindOfThrottle)) {
      // The throttling state machine is in the ramp-up state and the bandwidth
      // has not yet been adjusted.
      //

      // Adjust the bandwidth upward.
      //
      bandwidthTarget[kindOfThrottle.getValue()] =
          Math.min(maxBandwidthTarget[kindOfThrottle.getValue()],
              (bandwidthTarget[kindOfThrottle.getValue()] +
                  bandwidthRampupDelta[kindOfThrottle.getValue()]));

      // Calculate the next bandwidth delta.
      //
      bandwidthRampupDelta[kindOfThrottle.getValue()] *= bandwidthRampupMultiplier;
    } else if (throttleSM.rampDown(kindOfThrottle)) {
      // The bandwidth throttler is handling throttle events and is in the throttling
      // state where bandwidth has still needs to be determined. Capture the previous
      // success rate and ramp down.
      //
      float successRate = throttleSM.getPreviousTxSuccessRate(kindOfThrottle);
      long tmpBandwidth = 0;
      if (0 == successRate) {
        tmpBandwidth =
          Math.min(maxBandwidthTarget[kindOfThrottle.getValue()],
            (long) (DEFAULT_SUCCESS_RATE * 
                    bandwidthTarget[kindOfThrottle.getValue()]));
      } else {
        tmpBandwidth =
            Math.min(maxBandwidthTarget[kindOfThrottle.getValue()],
              (long) (successRate * 
                      bandwidthTarget[kindOfThrottle.getValue()]));
      }

      // Trace bandwidth metrics.
      //
      final String infoMsg = String.format(
          "Max Bandwidth %d Old bandwidth %d, New Bandwidth %d, SuccessRate %f.",
          maxBandwidthTarget[kindOfThrottle.getValue()],
          bandwidthTarget[kindOfThrottle.getValue()], tmpBandwidth, successRate);
      LOG.info (infoMsg);

      long deltaBandwidth = bandwidthTarget[kindOfThrottle.getValue()] - tmpBandwidth;
      bandwidthTarget[kindOfThrottle.getValue()] = tmpBandwidth;

      // Trace ramp down bandwidth.
      //
      LOG.info(
        "Throttle: ramping down to new bandwidth =" +
             bandwidthTarget[kindOfThrottle.getValue()]);

      // Set the ramp-up delta to a fraction of the range the bandwidth was ramped down.
      //
      if (deltaBandwidth / 2 > 0){
        bandwidthRampupDelta[kindOfThrottle.getValue()] = deltaBandwidth / 2;
      } else {
        // The calculated new bandwidth delta is either 0 or negative. Set the bandwidth
        // ramp-up delta to the default bandwidth ramp-up delta.
        //
        bandwidthRampupDelta[kindOfThrottle.getValue()] = minBandwidthRampupDelta;
      }
    } else {
      // The state machine stabilizing at the current bandwidth. Trace the current
      // bandwidth.
      //
      LOG.info(
        "Throttle: stabilizing at new bandwidth =" +
            bandwidthTarget[kindOfThrottle.getValue()]);
    }

    // Calculate the delay for the send request. Notice that the bandwidth is never
    // zero since it is always greater than the non-negative bandwidthMinThreshold.
    //
    long delayMs =
        blockSize[kindOfThrottle.getValue()] / bandwidthTarget[kindOfThrottle.getValue()]
        - latency;

    // The delay should be no greater than the maxThrottleDelay.
    //
    delayMs = Math.min(delayMs, maxThrottleDelay);

    // Trace the throttling delay.
    //
    LOG.info("Throttle:  delay send by " + delayMs + " ms.");

    // Pause the thread only if its delay is greater than zero. Otherwise do not delay.
    //
    try {
        Thread.sleep(Math.max (0, delayMs));
    } catch (InterruptedException e) {
      // Thread pause interrupted. Ignore and continue.
      //
    }

    // Turn off throttling by setting the state machine to the non-throttled state
    // if the bandwidth target matches the maximum bandwidth target.
    //
    if (bandwidthTarget[kindOfThrottle.getValue()] >=
        maxBandwidthTarget[kindOfThrottle.getValue()]) {
      throttleSM.stopThrottling (kindOfThrottle);
      bandwidthTarget[kindOfThrottle.getValue()] =
          maxBandwidthTarget[kindOfThrottle.getValue()];
    }

    LOG.info ("Leaving throttleSendRequest...");
  }
}