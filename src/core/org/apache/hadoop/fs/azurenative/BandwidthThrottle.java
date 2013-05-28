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
import org.apache.hadoop.fs.azurenative.ThrottleStateMachine.ThrottleState;

import com.microsoft.windowsazure.services.core.ConfigurationException;

/**
 * ThrottleSendRequest object implementing the ThrottleSendRequestCallback and the
 * BandwidthThrottleFeedback interfaces.  These interfaces are used to calculate the
 * throttling delays and track send transmission successes/failures, respectively.
 */
public class BandwidthThrottle implements ThrottleSendRequestCallback {

  /**
   * Enum defining throttling types.
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

  // Variables counting the successful and unsuccessful transmissions
  // process wide.
  //
  private static boolean isThrottling = false;
  private static ThrottleStateMachine throttleSM;
  private static ScheduledExecutorService timerScheduler;

  // String constants.
  //
  private static final String ASV_TIMER_NAME = "AsvStream";
  private static final String ASV_RAMPUP_TIMER_SUFFIX = "_Rampup";

  private static final String KEY_INITIAL_DOWNLOAD_BANDWIDTH = "fs.azure.initial.download.bandwidth"; // Initial download bandwidth per process
  private static final String KEY_INITIAL_UPLOAD_BANDWIDTH = "fs.azure.initial.upload.bandwidth"; // Initial upload bandwidth per process.
  private static final String KEY_SUCCESS_RATE_INTERVAL = "fs.azure.success.rate.interval";
  private static final String KEY_MIN_BANDWIDTH_THRESHOLD = "fs.azure.min.bandwidth.threshold";
  private static final String KEY_FAILURE_TOLERANCE_PERIOD = "fs.azure.failure.tolerance.period";
  private static final String KEY_BANDWIDTH_RAMPUP_DELTA = "fs.azure.rampup.delta";
  private static final String KEY_BANDWIDTH_RAMPUP_MULTIPLIER = "fs.azure.rampup.multiplier";
  private static final String KEY_BANDWIDTH_RAMPDOWN_MULTIPLIER = "fs.azure.rampdown.multiplier";

  private static final int DEFAULT_INITIAL_DOWNLOAD_BANDWIDTH = 10*2*1024*1024; //10Gbps / 64 nodes
  private static final int DEFAULT_INITIAL_UPLOAD_BANDWIDTH = 5*2*1024*1024; // 5Gbps / 64 nodes
  private static final int DEFAULT_SUCCESS_RATE_INTERVAL = 60; // 60 seconds
  private static final int DEFAULT_MIN_BANDWIDTH_THRESHOLD = 1 * 1024 * 1024; // 1MB/s
  private static final int DEFAULT_FAILURE_TOLERANCE_PERIOD = 60; // 1 tick period = 60s
  private static final long DEFAULT_BANDWIDTH_RAMPUP_DELTA = 1024*1024; // MB/s
  private static final float DEFAULT_BANDWIDTH_RAMPUP_MULTIPLIER  = 1.2f;
  private static final float DEFAULT_BANDWIDTH_RAMPDOWN_MULTIPLIER = 1.0f;
  private static final long DEFAULT_LATENCY [] = {500 /* upload */, 250 /* download */};

  private static final int FAILURE_TOLERANCE_INTERVALS = 2;// Ramp-up/down after 2 ticks.

  // Other constants.
  //
  private static int ONE_SECOND = 1000;  // 1000 milliseconds
  private static final int TIMER_SCHEDULER_CONCURRENCY = 2; // Number of scheduler threads.

  private Configuration sessionConfiguration;

  // Member variables capturing throttle parameters.
  // Note: There is no need for member variables for the download and upload block sizes
  //       since they are captured by the storage session context.
  //
  private int successRateInterval;
  private int bandwidthMinThreshold;
  private int failureTolerancePeriod;
  private long minBandwidthRampupDelta;
  private double bandwidthRampupMultiplier;
  private double bandwidthRampdownMultiplier;

  private long[] bandwidth;
  private long[] maxBandwidth;
  private long[] bandwidthRampupDelta;
  private AzureFileSystemTimer bandwidthRampupTimer[];

  /**
   * Constructor for the ThrottleSendRequest object.
   * @throws AzureException
   */
  public BandwidthThrottle (int concurrentReads, int concurrentWrites,
      Configuration sessionConfiguration) throws ConfigurationException {
    this.sessionConfiguration = sessionConfiguration;

    // Initialize throttling parameters and variables.
    //
    configureSessionThrottling(concurrentReads, concurrentWrites);
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
  private void configureSessionThrottling(int concurrentReads, int concurrentWrites)
      throws ConfigurationException {


    // Minimum allowable bandwidth consumption.
    //
    bandwidthMinThreshold = sessionConfiguration.getInt(
        KEY_MIN_BANDWIDTH_THRESHOLD,
        DEFAULT_MIN_BANDWIDTH_THRESHOLD);
    if (bandwidthMinThreshold <= 0) {
      // Throw a configuration exception. The cluster should have at least 1
      // node.
      //
      String errMsg =
          String.format("Minimum throttled bandwidth threshold is %d bytes/s." ,
              bandwidthMinThreshold);
      throw new ConfigurationException(errMsg);
    }

    // Capture the theoretical download bandwidth from the Azure store.
    //
    long storageDownloadBandwidth = sessionConfiguration.getLong(
        KEY_INITIAL_DOWNLOAD_BANDWIDTH,
        DEFAULT_INITIAL_DOWNLOAD_BANDWIDTH);
    if (storageDownloadBandwidth < bandwidthMinThreshold) {
      // The storage download bandwidth should be at least the minimum bandwidth
      // threshold.
      //
      String errMsg =
          String.format("Storage download bandwidth is %d bytes/s." +
              " It should be greater than minimum bandwidth threshold of %d.",
              storageDownloadBandwidth, bandwidthMinThreshold);
      throw new ConfigurationException(errMsg);
    }

    // Capture the theoretical upload bandwidth to the Azure store.
    //
    long storageUploadBandwidth = sessionConfiguration.getLong(
        KEY_INITIAL_UPLOAD_BANDWIDTH,
        DEFAULT_INITIAL_UPLOAD_BANDWIDTH);
    if (storageUploadBandwidth < bandwidthMinThreshold) {
      // The storage upload bandwidth should be at least the minimum bandwidth
      // threshold.
      //
      String errMsg =
          String.format("Storage upload bandwidth is %d bytes/s." +
              " It should be greater than minimum bandwidth threshold of %d.",
              storageUploadBandwidth, bandwidthMinThreshold);
      throw new ConfigurationException(errMsg);
    }

    // Initialize upload and download bandwidths.
    //
    maxBandwidth = new long [] {
        Math.max(bandwidthMinThreshold, storageUploadBandwidth / concurrentWrites),
        Math.max(bandwidthMinThreshold, storageDownloadBandwidth / concurrentReads)
    };

    // Initially set the current bandwidth to the maxBandwidth.
    //
    bandwidth = Arrays.copyOf(maxBandwidth, maxBandwidth.length);

    // Interval during which the number of I/O request successes and failures
    // are computed.
    //
    successRateInterval = sessionConfiguration.getInt(
        KEY_SUCCESS_RATE_INTERVAL,
        DEFAULT_SUCCESS_RATE_INTERVAL);
    if (successRateInterval <= 0) {
      // The success rate interval configuration must be at least 1 second.
      //
      String errMsg =
          String.format("Cluster is configured with a success rate interval of %d" +
              " The success rate interval should be at least 1 second.",
              successRateInterval);
      throw new ConfigurationException(errMsg);
    }

    // Start bandwidth throttling if it is not disabled.
    //
    throttleBandwidth(ASV_TIMER_NAME, successRateInterval);

    // A thread must see no failures over this tolerance period before ramping up.
    //
    failureTolerancePeriod = sessionConfiguration.getInt(
        KEY_FAILURE_TOLERANCE_PERIOD,
        DEFAULT_FAILURE_TOLERANCE_PERIOD);
    if (failureTolerancePeriod <= 0) {
      // The failure tolerance period configuration must be at least 1 second.
      //
      String errMsg =
          String.format(
              "Cluster is configured with a failure tolerance period of %d" +
                  " The failure tolerance period should be at least 1 second.",
                  failureTolerancePeriod);
      throw new ConfigurationException(errMsg);
    }

    // Delta in bandwidth when ramping up.  The size of this value controls the
    // ramp up rate.
    //
    minBandwidthRampupDelta = sessionConfiguration.getLong(
        KEY_BANDWIDTH_RAMPUP_DELTA,
        DEFAULT_BANDWIDTH_RAMPUP_DELTA);
    if (minBandwidthRampupDelta <= 0) {
      // The bandwidth ramp-up delta configuration be greater than zero.
      //
      String errMsg =
          String.format(
              "Cluster is configured with a bandwidth ramp-up delta of %d" +
                  " The bandwidth ramp-up delta should be greater than zero.",
                  minBandwidthRampupDelta);
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
              "Cluster is configured with a bandwidth ramp-up multiplier of %d" +
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
              "Cluster is configured with a bandwidth ramp-down multiplier of %d" +
                  " The bandwidth ramp-down multiplier should be greater than one.",
                  bandwidthRampupMultiplier);
      throw new ConfigurationException(errMsg);
    }

    // Create and initialize ramp-up timers.
    //
    bandwidthRampupTimer = new AzureFileSystemTimer [] {
        new AzureFileSystemTimer(
            ASV_TIMER_NAME + "_" + ThrottleType.UPLOAD + "_" + ASV_RAMPUP_TIMER_SUFFIX,
            getTimerScheduler(), 0, failureTolerancePeriod, FAILURE_TOLERANCE_INTERVALS),

        new AzureFileSystemTimer(
            ASV_TIMER_NAME + "_" + ThrottleType.DOWNLOAD + "_" + ASV_RAMPUP_TIMER_SUFFIX,
            getTimerScheduler(), 0, failureTolerancePeriod, FAILURE_TOLERANCE_INTERVALS)
    };
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
  private static synchronized void throttleBandwidth(
      String timerName, final int period) {
    // Check if the process is already throttling bandwidth.
    //
    if (isBandwidthThrottled()) {
      // Bandwidth throttling is already turned on, return to caller.
      //
      return;
    }

    // Start up timer scheduler if one does not already exist.
    //
    if (null == timerScheduler){
      timerScheduler = Executors.newScheduledThreadPool(TIMER_SCHEDULER_CONCURRENCY);
    }

    // Create throttling state machine to drive throttling events on the bandwidth
    // throttling engine.
    //
    throttleSM = new ThrottleStateMachine(
        timerName, getTimerScheduler(), period, FAILURE_TOLERANCE_INTERVALS);

    // Throttling has been turned on.
    //
    isThrottling = true;
  }

  /**
   * Check if bandwidth throttling is turned on for ASV file system store instances
   * for this process.
   *
   * @return - true if bandwidth throttling is turned on, false otherwise
   */
  public static boolean isBandwidthThrottled() {
    return isThrottling;
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
   * @param payloadSize
   *          - size of the read or write payload on connection.
   */
  @Override
  public void throttleSendRequest(ThrottleType kindOfThrottle, long payloadSize) {
    // Get the latency and current bandwidth from the instrumentation. If there has
    // been no activity, the latency would be zero. Set the latency to the default
    // upload/download latency.
    //
    long latency = throttleSM.getCurrentLatency(kindOfThrottle);
    if (0 == latency) {
      latency = throttleSM.getPreviousLatency(kindOfThrottle);
    }

    // If the latency is still zero use the default latency. This typically occurs at
    // startup when there is no history of previous latencies.
    //
    if (0 == latency) {
      latency = DEFAULT_LATENCY[kindOfThrottle.getValue()];
    }

    // If the current throttling state is NORMAL (no throttling events) then check if
    // to remain at the current bandwidth or ramp up.
    //
    if (throttleSM.getState(kindOfThrottle) == ThrottleState.NORMAL) {
      // There have been no throttling events in the last timer epoch. If the
      // ramp up timer is turned on not need to change the bandwidth. The current
      // bandwidth level is still being stabilized.
      //
      // Note: Ramp-up stabilization interval should be a state in the throttling
      //       state machine. The timer rightfully belongs there but this works also.
      //
      if (bandwidthRampupTimer[kindOfThrottle.getValue()].isOff()) {
        LOG.info("Throttle: Transmission ramp-up wait period expired, increasing current "+
                 "bandwidth = " + bandwidth[kindOfThrottle.getValue()] +
                 " by " + bandwidthRampupDelta[kindOfThrottle.getValue()] + "b/s.");

        // Set the new bandwidth and turn on ramp-up interval timer.
        //
        bandwidth[kindOfThrottle.getValue()] =
            Math.min(maxBandwidth[kindOfThrottle.getValue()],
                (bandwidth[kindOfThrottle.getValue()] +
                    bandwidthRampupDelta[kindOfThrottle.getValue()]));

        bandwidthRampupTimer[kindOfThrottle.getValue()].turnOnTimer();

        // Calculate the next bandwidth delta.
        //
        bandwidthRampupDelta[kindOfThrottle.getValue()] *= bandwidthRampupMultiplier;
      }
    } else if (throttleSM.getState(kindOfThrottle) ==
                  ThrottleState.THROTTLING_ADJUST_BANDWIDTH) {
      // The bandwidth throttler is handling throttle events and is in the throttling
      // state where bandwidth has still needs to be determined. Capture the current
      // success rate and ramp down.
      //
      float successRate = throttleSM.getCurrentTxSuccessRate(kindOfThrottle);
      long tmpBandwidth =
          Math.min(maxBandwidth[kindOfThrottle.getValue()],
              Math.max(bandwidthMinThreshold,
                   (long) (successRate * bandwidthRampdownMultiplier *
                           bandwidth[kindOfThrottle.getValue()])));

      // Trace bandwidth metrics.
      //
      final String infoMsg = String.format(
          "Min Bandwidth %d Old bandwidth %d, New Bandwidth %d, SuccessRate %f.",
          bandwidthMinThreshold, bandwidth[kindOfThrottle.getValue()], tmpBandwidth,
          successRate);
      LOG.info (infoMsg);

      long deltaBandwidth = bandwidth[kindOfThrottle.getValue()] - tmpBandwidth;
      bandwidth[kindOfThrottle.getValue()] = tmpBandwidth;

      // Trace ramp down bandwidth.
      //
      LOG.info(
        "Throttle: ramping down to new bandwidth =" +
             bandwidth[kindOfThrottle.getValue()]);

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

      // Throttling, so turn off the ramp up timer if it is on.
      //
      if (bandwidthRampupTimer[kindOfThrottle.getValue()].isOn()){
        bandwidthRampupTimer[kindOfThrottle.getValue()].isOff();
      }
    } else {
      // The state machine stabilizing at the current bandwidth. Trace the current
      // bandwidth.
      //
      LOG.info(
        "Throttle: stabilizing at new bandwidth =" +
            bandwidth[kindOfThrottle.getValue()]);

      // Throttling, so turn off the ramp up timer if it is on.
      //
      if (bandwidthRampupTimer[kindOfThrottle.getValue()].isOn()){
        bandwidthRampupTimer[kindOfThrottle.getValue()].isOff();
      }
    }

    // Calculate the delay for the send request. Notice that the bandwidth is never
    // zero since it is always greater than the non-negative bandwidthMinThreshold.
    //
    long delayMs =
        payloadSize * ONE_SECOND / bandwidth[kindOfThrottle.getValue()] - latency;

    // Trace the throttling delay.
    //
    LOG.info("Throttle:  delay send by " + delayMs + " ms.");

    // Pause the thread only if its delay is greater than zero. Otherwise do not delay.
    //
    if (0 < delayMs) {
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException e) {
        // Thread pause interrupted. Ignore and continue.
        //
      }
    }
  }
}
