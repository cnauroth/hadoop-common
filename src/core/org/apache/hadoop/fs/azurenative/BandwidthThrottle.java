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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azurenative.AzureFileSystemTimer.DefaultTimerTask;
import com.microsoft.windowsazure.services.core.ConfigurationException;

/**
 * ThrottleSendRequest object implementing the ThrottleSendRequestCallback and the
 * BandwidthThrottleFeedback interfaces.  These interfaces are used to calculate the
 * throttling delays and track send transmission successes/failures, respectively.
 */
public class BandwidthThrottle implements ThrottleSendRequestCallback, BandwidthThrottleFeedback {

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
  private static AzureFileSystemTimer[] txSuccessRateTimer;
  private static ThrottlerTxSuccessRateTask[] txSuccessRateTask;
  private static ScheduledExecutorService timerScheduler;

  // String constants.
  //
  private static final String ASV_TIMER_NAME = "AsvStream";
  private static final String ASV_RATE_TIMER_SUFFIX = "SuccessRate";
  private static final String ASV_RAMPUP_TIMER_SUFFIX = "_Rampup";
  private static final String ASV_RAMPDOWN_TIMER_SUFFIX = "_Rampdown";

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
  private static final int DEFAULT_MIN_BANDWIDTH_THRESHOLD = 4 * 1024 * 1024; // 4MB/s
  private static final int DEFAULT_FAILURE_TOLERANCE_PERIOD = 15; // 1 tick period = 30 seconds
  private static final long DEFAULT_BANDWIDTH_RAMPUP_DELTA = 1024*1024; // MB/s
  private static final float DEFAULT_BANDWIDTH_RAMPUP_MULTIPLIER  = 1.2f;
  private static final float DEFAULT_BANDWIDTH_RAMPDOWN_MULTIPLIER = 1.0f;
  private static final long DEFAULT_DOWNLOAD_LATENCY = 500; // 500 milliseconds.
  private static final long DEFAULT_UPLOAD_LATENCY = 500; // 500 milliseconds.

  private static final int FAILURE_TOLERANCE_INTERVAL = 2;// Ramp-up/down after 2 ticks.

  // Default timer constant.
  //
  private static final DefaultTimerTask THROTTLE_DEFAULT_TIMER_TASK = new DefaultTimerTask ();

  // Other constants.
  //
  private static int ONE_SECOND = 1000;  // 1000 milliseconds
  private static final int TIMER_SCHEDULER_CONCURRENCY = 2; // Number of scheduler threads.

  private Configuration sessionConfiguration;
  private AzureFileSystemInstrumentation instrumentation;

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
  private long [] bandwidthRampupDelta;
  private AzureFileSystemTimer[] txRampupTimer;
  private AzureFileSystemTimer[] txRampdownTimer;
  private DefaultTimerTask[] txRampupTask;
  private DefaultTimerTask[] txRampdownTask;

  /**
   * Constructor for the ThrottleSendRequest object.
   * @throws AzureException
   */
  public BandwidthThrottle (int concurrentReads, int concurrentWrites,
      AzureFileSystemInstrumentation instrumentation,
      Configuration sessionConfiguration) throws ConfigurationException {
    this.instrumentation = instrumentation;
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
    bandwidth = new long [] {
        Math.max(bandwidthMinThreshold, storageUploadBandwidth / concurrentWrites),
        Math.max(bandwidthMinThreshold, storageDownloadBandwidth / concurrentReads)
    };

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

    // A thread must see no failures over this tolerance period before ramping
    // up.
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

    // Create default timer tasks for both ramp-up and ramp-down.
    //
    txRampupTask = new DefaultTimerTask[] {
        THROTTLE_DEFAULT_TIMER_TASK,
        THROTTLE_DEFAULT_TIMER_TASK
    };

    txRampdownTask = new DefaultTimerTask[] {
        THROTTLE_DEFAULT_TIMER_TASK,
        THROTTLE_DEFAULT_TIMER_TASK
    };

    // Create and initialize ramp-up timers.
    //
    txRampupTimer = new AzureFileSystemTimer [] {
        new AzureFileSystemTimer(
            ASV_TIMER_NAME + "_" + ThrottleType.UPLOAD + "_" + ASV_RAMPUP_TIMER_SUFFIX,
            getTimerScheduler(), 0, failureTolerancePeriod, FAILURE_TOLERANCE_INTERVAL),

        new AzureFileSystemTimer(
            ASV_TIMER_NAME + "_" + ThrottleType.DOWNLOAD + "_" + ASV_RAMPUP_TIMER_SUFFIX,
            getTimerScheduler(), 0, failureTolerancePeriod, FAILURE_TOLERANCE_INTERVAL)
    };

    // Create and initialize ramp-down timers.
    //
    txRampdownTimer = new AzureFileSystemTimer [] {
        new AzureFileSystemTimer(
            ASV_TIMER_NAME +  "_" + ThrottleType.UPLOAD + "_" + ASV_RAMPDOWN_TIMER_SUFFIX,
            getTimerScheduler(), 0, failureTolerancePeriod, FAILURE_TOLERANCE_INTERVAL),

        new AzureFileSystemTimer(
            ASV_TIMER_NAME +  "_" + ThrottleType.DOWNLOAD + "_" + ASV_RAMPDOWN_TIMER_SUFFIX,
            getTimerScheduler(), 0, failureTolerancePeriod, FAILURE_TOLERANCE_INTERVAL)
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

    // Create interval timer to count successful and unsuccessful upload/download
    // transmissions. The timer has no delay time and no automatic cancellation
    //
    txSuccessRateTimer = new AzureFileSystemTimer [] {
        new AzureFileSystemTimer(
            timerName + "_" + ThrottleType.UPLOAD + "_" + ASV_RATE_TIMER_SUFFIX,
            getTimerScheduler(), 0, period),

        new AzureFileSystemTimer(
            timerName + "_" + ThrottleType.DOWNLOAD + "_" + ASV_RATE_TIMER_SUFFIX,
            getTimerScheduler(), 0, period),
    };

    // Create task to calculate success rates. Initialize throttling by starting
    // the ASV success rate timer
    //
    txSuccessRateTask = new ThrottlerTxSuccessRateTask [] {
        new ThrottlerTxSuccessRateTask(),
        new ThrottlerTxSuccessRateTask()
    };

    // Turn on the timers.
    //
    for (ThrottleType kindOfThrottle : ThrottleType.values()) {
      txSuccessRateTimer[kindOfThrottle.getValue()].turnOnTimer(
          txSuccessRateTask[kindOfThrottle.getValue()]);
    }

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
   * Update transmission success counter by delta.
   * @param delta -- amount to add to successful transmissions for this interval.
   * @return total accumulated count of successful transmissions for interval.
   */
  @Override
  public int updateTransmissionSuccess(ThrottleType kindOfThrottle, int delta) {

    if (!isBandwidthThrottled()){
      // This callback should not be called if throttling is not turned on.
      //
      throw new AssertionError(
            "Throttling callback when bandwidth throttling is not turned on.");
    }

    // Update the number of successful transmission request by delta and return
    // with the accumulated successes.
    //
    return txSuccessRateTask[kindOfThrottle.getValue()].updateTxSuccess(delta);
  }

  /**
   * Update transmission failure by delta.
   * @param delta -- amount to add to failed transmissions for this interval.
   * @return total accumulated count of failed transmissions for interval.
   */
  @Override
  public int updateTransmissionFailure (ThrottleType kindOfThrottle, int delta) {
    if (!isBandwidthThrottled()){
      // This callback should not be called if throttling is not turned on.
      //
      throw new AssertionError(
            "Throttling callback when bandwidth throttling is not turned on.");
    }

    // Update the total number of failed transmission requests by delta and
    // return with the accumulated failures.
    //
    return txSuccessRateTask[kindOfThrottle.getValue()].updateTxFailure(delta);
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
    long latency = 0;
    switch(kindOfThrottle) {
      case UPLOAD:
        // Get upload latency.
        //
        latency = instrumentation.getBlockUploadLatency();
        if (0 == latency) {
          latency = DEFAULT_UPLOAD_LATENCY;
        }
        break;
      case DOWNLOAD:
        // Get download latency.
        //
        latency = instrumentation.getBlockDownloadLatency();
        if (0 == latency) {
          latency = DEFAULT_DOWNLOAD_LATENCY;
        }
        break;
      default:
        // The kind of throttle is unexpected or undefined.
        //
        throw new AssertionError("Unexpected throttle type in SendRequest callback.");
    }

    // Capture the current success rate. If the success rate is less than 1 ramp
    // down the bandwidth, otherwise ramp up the bandwidth.
    //
    float successRate =
        txSuccessRateTask[kindOfThrottle.getValue()].getCurrentTxSuccessRate();

    // Calculate the new bandwidth.
    //
    long tmpBandwidth = bandwidth[kindOfThrottle.getValue()];
    if (successRate < 1.0f) {
      tmpBandwidth = (long) (successRate * bandwidthRampdownMultiplier *
                                bandwidth[kindOfThrottle.getValue()]);
    }

    if (tmpBandwidth < bandwidthMinThreshold) {
      // The bandwidth cannot be throttled below the minimum bandwidth threshold.
      // Set the bandwidth to the minimum bandwidth threshold and return.
      //
      tmpBandwidth = bandwidthMinThreshold;
    }

    // Main throttling logic to determine whether to ramp-up, ramp-down or leave things the
    // the same. In all cases, an attempt is made to throttle when a positive delay period
    // is calculated.
    //
    // If the bandwidth is greater or equal to the current bandwidth, attempt to ramp-up.
    //
    if (tmpBandwidth >= bandwidth[kindOfThrottle.getValue()]) {
      // Ramp-up only if the bandwidth ramp-up interval has expired.
      //
      if (txRampupTimer[kindOfThrottle.getValue()].isOff()) {
        LOG.info("Throttle: Transmission ramp-up wait period expired, increasing current "+
                 "bandwidth = " + bandwidth[kindOfThrottle.getValue()] +
                 " by " + bandwidthRampupDelta[kindOfThrottle.getValue()] + "b/s.");
        // Set the new bandwidth and turn on ramp-up interval timer.
        //
        bandwidth[kindOfThrottle.getValue()] =
            (bandwidth[kindOfThrottle.getValue()] +
                bandwidthRampupDelta[kindOfThrottle.getValue()]);
        txRampupTimer[kindOfThrottle.getValue()].turnOnTimer(
            txRampupTask[kindOfThrottle.getValue()]);

        // Calculate the next bandwidth delta.
        //
        bandwidthRampupDelta[kindOfThrottle.getValue()] *= bandwidthRampupMultiplier;
      }
    } else {
      // The new bandwidth is at less than the current bandwidth. Ramp-down,
      // but only if the  failure tolerance period has expired.
      //
      if (txRampdownTimer[kindOfThrottle.getValue()].isOn()) {
        LOG.info("Throttle: Transmission ramp-down tolerance period not expired, old bandwidth ="
            + bandwidth[kindOfThrottle.getValue()]);
      } else {
        long deltaBandwidth = bandwidth[kindOfThrottle.getValue()] - tmpBandwidth;
        bandwidth[kindOfThrottle.getValue()] = tmpBandwidth;

        // Set the new bandwidth and turn on the ramp-down interval timer.
        //
        // bandwidth[kindOfThrottle.getValue()] = tmpBandwidth;
        txRampdownTimer[kindOfThrottle.getValue()].turnOnTimer(
          txRampdownTask[kindOfThrottle.getValue()]);

        LOG.info(
          "Throttle: Transmission ramp-down tolerance period expired, "+
          "ramping down to new bandwidth =" + bandwidth[kindOfThrottle.getValue()]);

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
      }
    }

    // Calculate the delay for the send request. Notice that the bandwidth is never
    // zero since it is always greater than the non-negative bandwidthMinThreshold.
    //
    long delayMs = latency - payloadSize * ONE_SECOND / bandwidth[kindOfThrottle.getValue()];
    if (bandwidth[kindOfThrottle.getValue()] == bandwidthMinThreshold) {
      switch(kindOfThrottle) {
        case UPLOAD:
          if (latency == DEFAULT_UPLOAD_LATENCY) delayMs = 3000;
          break;
        case DOWNLOAD:
          if (latency == DEFAULT_DOWNLOAD_LATENCY) delayMs = 3000;
          break;
        default:
          // The kind of throttle is unexpected or undefined.
          //
          throw new AssertionError("Unexpected throttle type in SendRequest callback.");
      }
    }

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
