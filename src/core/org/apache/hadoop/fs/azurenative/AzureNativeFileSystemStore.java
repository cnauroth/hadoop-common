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

import static org.apache.hadoop.fs.azurenative.NativeAzureFileSystem.PATH_DELIMITER;

import java.io.*;
import java.net.*;
import java.security.InvalidKeyException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azure.KeyProviderException;
import org.apache.hadoop.fs.azurenative.AzureFileSystemTimer.DefaultTimerTask;
import org.apache.hadoop.fs.permission.*;
import org.mortbay.util.ajax.JSON;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.ConfigurationException;
import com.microsoft.windowsazure.services.core.storage.*;

import static org.apache.hadoop.fs.azurenative.StorageInterface.*;

class AzureNativeFileSystemStore implements NativeFileSystemStore,
        BandwidthThrottleFeedback, ThrottleSendRequestCallback {

  /**
   * Configuration knob on whether we do block-level MD5 validation on
   * upload/download.
   */
  static final String KEY_CHECK_BLOCK_MD5 = "fs.azure.check.block.md5";
  /**
   * Configuration knob on whether we store blob-level MD5 on upload.
   */
  static final String KEY_STORE_BLOB_MD5 = "fs.azure.store.blob.md5";
  static final String DEFAULT_STORAGE_EMULATOR_ACCOUNT_NAME =
      "storageemulator";
  static final String STORAGE_EMULATOR_ACCOUNT_NAME_PROPERTY_NAME =
      "fs.azure.storage.emulator.account.name";

  public static final Log LOG = LogFactory.getLog(AzureNativeFileSystemStore.class);

  private StorageInterface storageInteractionLayer;
  private CloudBlobDirectoryWrapper rootDirectory;
  private CloudBlobContainerWrapper container;

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

  // Member variables capturing throttle parameters.
  // Note: There is no need for member variables for the download and upload block sizes
  //       since they are captured by the storage session context.
  //
  private boolean disableBandwidthThrottling = true;
  private boolean concurrentReadWrite;
  private int mapredTaskTimeout;
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

  // Member variables to unit test throttling feedback
  //
  private ThrottleSendRequestCallback throttleSendCallback;
  private BandwidthThrottleFeedback throttleBandwidthFeedback;

  // Constants local to this class.
  //
  private static final String KEY_ACCOUNT_KEYPROVIDER_PREFIX =
      "fs.azure.account.keyprovider.";
  private static final String KEY_ACCOUNT_SAS_PREFIX = "fs.azure.sas.";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_OUT =
      "fs.azure.concurrentRequestCount.out";
  private static final String KEY_STREAM_MIN_READ_SIZE = "fs.azure.read.request.size";
  private static final String KEY_STORAGE_CONNECTION_TIMEOUT = "fs.azure.storage.timeout";
  private static final String KEY_WRITE_BLOCK_SIZE = "fs.azure.write.request.size";

  private static final String KEY_CONCURRENT_READ_WRITE = "fs.azure.concurrent.read-write";
  private static final String KEY_MAPRED_TASK_TIMEOUT = "mapred.task.timeout";
  private static final int FAILURE_TOLERANCE_INTERVAL = 2;// Ramp-up/down after 2 ticks.
  private static final int ONE_SECOND = 1000; // One second is 1000 ms.

  // Configurable throttling parameter properties. These properties are located in the
  // core-site.xml configuration file.
  //
  private static final String KEY_DISABLE_THROTTLING = "fs.azure.disable.bandwidth.throttling";
  private static final String KEY_INITIAL_DOWNLOAD_BANDWIDTH = "fs.azure.initial.download.bandwidth";
  private static final String KEY_INITIAL_UPLOAD_BANDWIDTH = "fs.azure.initial.upload.bandwidth";
  private static final String KEY_NUMBER_OF_SLOTS = "fs.azure.slots";
  private static final String KEY_SUCCESS_RATE_INTERVAL = "fs.azure.success.rate.interval";
  private static final String KEY_MIN_BANDWIDTH_THRESHOLD = "fs.azure.min.bandwidth.threshold";
  private static final String KEY_FAILURE_TOLERANCE_PERIOD = "fs.azure.failure.tolerance.period";
  private static final String KEY_BANDWIDTH_RAMPUP_DELTA = "fs.azure.rampup.delta";
  private static final String KEY_BANDWIDTH_RAMPUP_MULTIPLIER = "fs.azure.rampup.multiplier";
  private static final String KEY_BANDWIDTH_RAMPDOWN_MULTIPLIER = "fs.azure.rampdown.multiplier";

  private static final String PERMISSION_METADATA_KEY = "asv_permission";
  private static final String IS_FOLDER_METADATA_KEY = "asv_isfolder";
  static final String VERSION_METADATA_KEY = "asv_version";
  static final String CURRENT_ASV_VERSION = "2013-01-01";
  static final String LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY = "asv_tmpupload";

  private static final String HTTP_SCHEME = "http";
  private static final String HTTPS_SCHEME = "https";
  private static final String ASV_SCHEME = "asv";
  private static final String ASV_SECURE_SCHEME = "asvs";
  private static final String ASV_AUTHORITY_DELIMITER = "@";
  private static final String AZURE_ROOT_CONTAINER = "$root";

  private static final String ASV_TIMER_NAME = "AsvStream";
  private static final String ASV_RATE_TIMER_SUFFIX = "SuccessRate";
  private static final String ASV_RAMPUP_TIMER_SUFFIX = "_Rampup";
  private static final String ASV_RAMPDOWN_TIMER_SUFFIX = "_Rampdown";

  // DEFAULT concurrency for reads and writes.
  //
  private static final int DEFAULT_CONCURRENT_READS  = 1;
  private static final int DEFAULT_CONCURRENT_WRITES = 8;
  private static final boolean DEFAULT_CONCURRENT_READ_WRITE = false;

  // Set throttling parameter defaults.
  //
  private static final int DEFAULT_MAPRED_TASK_TIMEOUT = 300000;
  private static final int DEFAULT_DOWNLOAD_BLOCK_SIZE = 4 * 1024 * 1024;
  private static final int DEFAULT_UPLOAD_BLOCK_SIZE = 4 * 1024 * 1024;


  private static final boolean DEFAULT_DISABLE_THROTTLING = true;
  private static final int DEFAULT_NUMBER_OF_SLOTS = 8; // concurrent processes
  private static final int DEFAULT_INITIAL_DOWNLOAD_BANDWIDTH = 10*2*1024*1024; //10Gbps / 64 nodes
  private static final int DEFAULT_INITIAL_UPLOAD_BANDWIDTH = 5*2*1024*1024; // 5Gbps / 64 nodes
  private static final int DEFAULT_SUCCESS_RATE_INTERVAL = 60; // 60 seconds
  private static final int DEFAULT_MIN_BANDWIDTH_THRESHOLD = 4 * 1024 * 1024; // 4MB/s
  private static final int DEFAULT_FAILURE_TOLERANCE_PERIOD = 15; // 1 tick period = 30 seconds
  private static final long DEFAULT_BANDWIDTH_RAMPUP_DELTA = 1024*1024; // MB/s
  private static final float DEFAULT_BANDWIDTH_RAMPUP_MULTIPLIER  = 1.2f;
  private static final float DEFAULT_BANDWIDTH_RAMPDOWN_MULTIPLIER = 1.0f;
  private static final long DEFAULT_DOWNLOAD_LATENCY = 1000; // 1000 milliseconds.
  private static final long DEFAULT_UPLOAD_LATENCY = 1000; // 1000 milliseconds.
  private static final int TIMER_SCHEDULER_CONCURRENCY = 5;

  // Default timer constant.
  //
  private static final DefaultTimerTask THROTTLE_DEFAULT_TIMER_TASK = new DefaultTimerTask ();

  /**
   * CLASS VARIBLES
   */

  // Variables counting the successful and unsuccessful transmissions
  // process wide.
  //
  private static boolean isThrottling = false;
  private static AzureFileSystemTimer[] txSuccessRateTimer;
  private static CalculateTxSuccessRateTask[] txSuccessRateTask;
  private static ScheduledExecutorService timerScheduler;

  /**
   * MEMBER VARIABLES
   */

  private URI sessionUri;
  private Configuration sessionConfiguration;
  private int concurrentReads = DEFAULT_CONCURRENT_READS;
  private int concurrentWrites = DEFAULT_CONCURRENT_WRITES;
  private boolean isAnonymousCredentials = false;
  private AzureFileSystemInstrumentation instrumentation;
  private BandwidthGaugeUpdater bandwidthGaugeUpdater;
  private final static JSON permissionJsonSerializer =
      createPermissionJsonSerializer();
  private boolean suppressRetryPolicy = false;
  private boolean canCreateOrModifyContainer = false;
  private ContainerState currentKnownContainerState = ContainerState.Unknown;
  private final Object containerStateLock = new Object();

  private TestHookOperationContext testHookOperationContext = null;

  /**
    * A test hook interface that can modify the operation context
    * we use for Azure Storage operations, e.g. to inject errors.
    */
   interface TestHookOperationContext {
     OperationContext modifyOperationContext(OperationContext original);
   }

  /**
   * Suppress the default retry policy for the Storage, useful in unit
   * tests to test negative cases without waiting forever.
   */
  void suppressRetryPolicy() {
    suppressRetryPolicy = true;
  }

  /**
  * Add a test hook to modify the operation context we use for Azure Storage
  * operations.
  * @param testHook The test hook, or null to unset previous hooks.
  */
 void addTestHookToOperationContext(TestHookOperationContext testHook) {
   this.testHookOperationContext = testHook;
 }

 /**
   * If we're asked by unit tests to not retry, set the retry policy factory
   * in the client accordingly.
   */
  private void suppressRetryPolicyInClientIfNeeded() {
    if (suppressRetryPolicy) {
      storageInteractionLayer.setRetryPolicyFactory(new RetryNoRetry());
    }
  }

  /**
   * Creates a JSON serializer that can serialize a PermissionStatus object
   * into the JSON string we want in the blob metadata.
   * @return The JSON serializer.
   */
  private static JSON createPermissionJsonSerializer() {
    JSON serializer = new JSON();
    serializer.addConvertor(PermissionStatus.class,
        new PermissionStatusJsonSerializer());
    return serializer;
  }

  /**
   * A convertor for PermissionStatus to/from JSON as we want it in the blob
   * metadata.
   */
  private static class PermissionStatusJsonSerializer implements JSON.Convertor {
    private static final String OWNER_TAG = "owner";
    private static final String GROUP_TAG = "group";
    private static final String PERMISSIONS_TAG = "permissions";

    @Override
    public void toJSON(Object obj, JSON.Output out) {
      PermissionStatus permissionStatus = (PermissionStatus)obj;
      // Don't store group as null, just store it as empty string
      // (which is FileStatus behavior).
      String group = permissionStatus.getGroupName() == null ?
          "" : permissionStatus.getGroupName();
      out.add(OWNER_TAG, permissionStatus.getUserName());
      out.add(GROUP_TAG, group);
      out.add(PERMISSIONS_TAG, permissionStatus.getPermission().toString());
    }

    @Override
    public Object fromJSON(@SuppressWarnings("rawtypes") Map object) {
      return PermissionStatusJsonSerializer.fromJSONMap(object);
    }

    @SuppressWarnings("rawtypes")
    public static PermissionStatus fromJSONString(String jsonString) {
      // The JSON class can only find out about an object's class (and call me)
      // if we store the class name in the JSON string. Since I don't want to
      // do that (it's an implementation detail), I just deserialize as a
      // the default Map (JSON's default behavior) and parse that.
      return fromJSONMap(
          (Map)permissionJsonSerializer.fromJSON(jsonString));
    }

    private static PermissionStatus fromJSONMap(
        @SuppressWarnings("rawtypes") Map object) {
      return new PermissionStatus(
          (String)object.get(OWNER_TAG),
          (String)object.get(GROUP_TAG),
          // The initial - below is the Unix file type,
          // which FsPermission needs there but ignores.
          FsPermission.valueOf("-" + (String)object.get(PERMISSIONS_TAG))
          );
    }
  }

  void setAzureStorageInteractionLayer(
      StorageInterface storageInteractionLayer) {
    this.storageInteractionLayer = storageInteractionLayer;
  }

  BandwidthGaugeUpdater getBandwidthGaugeUpdater() {
    return bandwidthGaugeUpdater;
  }

  /**
   * Check if concurrent reads and writes on the same blob are allowed.
   *
   * @return true if concurrent reads and writes have been requested and false otherwise.
   */
  private boolean isConcurrentReadWriteAllowed() {
    return concurrentReadWrite;
  }

  /**
   * Capture the bandwidth throttling feedback interface from this object.
   * Note: This method is not an overkill. It exists to decouple how interface
   *       is implemented and created from the rest of the code.
   *
   * @return the bandwidth throttling feedback interface.
   */
  private BandwidthThrottleFeedback getBandwidthThrottleFeedback() {

    // Only return interface if throttling is turned on.
    //
    if(!isBandwidthThrottled()) {
      return null;
    }

    // If the throttle bandwidth feedback is set by a unit test or external
    // plug-in return it.
    //
    if (null != throttleBandwidthFeedback) {
      return throttleBandwidthFeedback;
    }

    // Return the default interface on this object.
    //
    return this;
  }

  /**
   * Capture the the throttle send request callback interface.
   *
   * @return the throttle send request call back interface.
   */
  private ThrottleSendRequestCallback getThrottleSendRequestCallback() {
    // Only return interface if throttling is turned on.
    //
    if(!isBandwidthThrottled()) {
      return null;
    }

    if (null != throttleSendCallback) {
      // If the throttle send callback is set by a unit test or external
      // plug in return it in favor of the default interface implemented
      // on the AzureNativeFileSystemStore object.
      //
      return throttleSendCallback;
    }

    // Return the default interface on this object.
    //
    return this;
  }

  /**
   * Check if bandwidth throttling is turned on for ASV file system store instances
   * for this process.
   *
   * @return - true if bandwidth throttling is turned on, false otherwise
   */
  private static boolean isBandwidthThrottled() {
    return isThrottling;
  }

  /**
   * Query the timer scheduler.
   * @return - current timer scheduler if started, null otherwise.
   */
  private static ScheduledExecutorService getTimerScheduler() {
    return timerScheduler;
  }

  /**
   * Task to calculate success rates on every timer success rate timer tick.
   *
   */
  private static class CalculateTxSuccessRateTask implements Runnable {
    // Member variables
    //
    private static final Object lock = new Object();
    private final AtomicInteger succTxCount = new AtomicInteger(0);
    private final AtomicInteger failTxCount = new AtomicInteger(0);
    private final AtomicInteger inactiveTxCount = new AtomicInteger(0);

    /**
     * Accumulate successful transmissions with the success rate timer interval.
     *
     * @param successCount - delta in the number of successful transmissions
     * @return total number of successful transmissions within the interval
     */
    public int updateTxSuccess(final int successCount) {
      return succTxCount.addAndGet(successCount);
    }

    /**
     * Accumulate failed transmissions within the success rate timer interval.
     *
     * @param failureCount - delta in the number of successful transmissions
     * @return total number of failed transmissions within the interval
     */
    public int updateTxFailure(final int failureCount) {
      return failTxCount.addAndGet(failureCount);
    }

    /**
     * Query the instantaneous success rate instead of waiting for the
     * expiration of the ramp-down interval.
     */
    public float getCurrentTxSuccessRate() {
      float currSuccessRate = 1.0f;
      float succCount = (float) succTxCount.get();
      float failCount = (float) failTxCount.get();

      if (succCount == 0.0f && failCount == 0.0f) {
        return currSuccessRate;
      }

      // Calculate the current success rate.
      //
      currSuccessRate = succCount / (succCount + failCount);
      return currSuccessRate;
    }

    /**
     * Run method implementation for the CalculateTxSuccessRate task.
     */
    @Override
    public void run() {

      // If there has been no activity over the past interval, increment the
      // inactivity count and return. We will use this count later on to
      // quiesce the scheduler and timers if there is no I/O activity.
      //
      if (succTxCount.get() == 0 && failTxCount.get () == 0) {
        inactiveTxCount.incrementAndGet();
        return;
      }

      // There has been I/O activity. Calculate the success rate under the
      // protection of a lock and reset counters
      //
      synchronized(lock) {
        succTxCount.set(0);
        failTxCount.set(0);
      }
    }
  }

  /**
   * Bandwidth throttling initializer to start success rate interval timer and
   * begin accumulated transmission success rates. This method is synchronized
   * since it is called by all instances of the AzureNativeFileSystemStore class
   * for this process. Only the first instance of the of this class would perform
   * the initialization.
   *
   * @param period - frequency in seconds at which success rate is calculated.
   * @throws AzureException - on errors starting timer
   */
  private static synchronized void throttleBandwidth(
      String timerName, final int period) throws AzureException {
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
    txSuccessRateTask = new CalculateTxSuccessRateTask [] {
        new CalculateTxSuccessRateTask(),
        new CalculateTxSuccessRateTask()
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
   * Throttle send request by delaying this thread.
   *
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
    try{
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

      // Set a ceiling on the delay to the map reduce timeout period.
      //
      if (mapredTaskTimeout < delayMs) {
        delayMs = mapredTaskTimeout - ONE_SECOND;
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
    } catch (AzureException e) {
      // Log the exception then eat it up.  Do not re-throw and let the send request go
      // through.
      //
      LOG.info("Received unexpected when throttling send request. Excpetion message: " +
                e.toString());
    }
  }

  /**
   * Special initialization routine for testing the throttling callback interfaces.
   *
   * @param uri - URI of the target storage blob.
   * @param conf - reference to the configuration object.
   * @param instrumentation - the metrics source that will keep track of operations.
   * @param throttleSendCallback - throttling send callback interface
   * @param throttleBandwidthFeedback - throttling bandwidth feedback interface.
   * @throws IllegalArgumentException if URI or job object is null or invalid scheme
   * @throws AzureException if there is an error establish a connection with Azure
   * @throws IOException if there are errors reading from/writing to Azure storage.
   */
  public void initialize(URI uri, Configuration conf,
      AzureFileSystemInstrumentation instrumentation,
      ThrottleSendRequestCallback throttleSendCallback,
      BandwidthThrottleFeedback throttleBandwidthFeedback)
          throws IllegalArgumentException, AzureException, IOException {

    // Capture the throttling feedback interfaces and delegate the stoarage
    // initialization.
    //
    this.throttleSendCallback = throttleSendCallback;
    this.throttleBandwidthFeedback = throttleBandwidthFeedback;

    // Delegate call to initialize the store.
    //
    initialize(uri, conf, instrumentation);
  }

  /**
   * Method for the URI and configuration object necessary to create a storage
   * session with an Azure session. It parses the scheme to ensure it matches
   * the storage protocol supported by this file system.
   *
   * @param uri - URI for target storage blob.
   * @param conf- reference to configuration object.
   * @param instrumentation - the metrics source that will keep track of operations here.
   *
   * @throws IllegalArgumentException if URI or job object is null, or invalid scheme.
   */
  @Override
  public void initialize(URI uri, Configuration conf, AzureFileSystemInstrumentation instrumentation)
      throws IllegalArgumentException, AzureException, IOException  {

    if (null == instrumentation) {
      throw new IllegalArgumentException("Null instrumentation");
    }
    this.instrumentation = instrumentation;
    this.bandwidthGaugeUpdater = new BandwidthGaugeUpdater(instrumentation);
    if (null == this.storageInteractionLayer) {
      this.storageInteractionLayer = new StorageInterfaceImpl();
    }

    // Check that URI exists.
    //
    if (null == uri) {
      throw new IllegalArgumentException("Cannot initialize ASV file system, URI is null");
    }

    // Check scheme associated with the URI to ensure it supports one of the
    // protocols for this file system.
    //
    if (null == uri.getScheme() || (!ASV_SCHEME.equals(uri.getScheme().toLowerCase()) &&
        !ASV_SECURE_SCHEME.equals(uri.getScheme().toLowerCase()))) {
      final String errMsg =
          String.format(
              "Cannot initialize ASV file system. Scheme is not supported, " +
                  "expected '%s' scheme.", ASV_SCHEME);
      throw new  IllegalArgumentException(errMsg);
    }

    // Check authority for the URI to guarantee that it is non-null.
    //
    if (null == uri.getAuthority()) {
      final String errMsg =
          String.format(
              "Cannot initialize ASV file system, URI authority not recognized.");
      throw new IllegalArgumentException(errMsg);
    }

    // Check that configuration object is non-null.
    //
    if (null == conf) {
      throw new IllegalArgumentException("Cannot initialize ASV file system, URI is null");
    }

    // Incoming parameters validated.  Capture the URI and the job configuration object.
    //
    sessionUri = uri;
    sessionConfiguration = conf;

    // Start an Azure storage session.
    //
    createAzureStorageSession ();
  }

  /**
   * Method to extract the account name from an Azure URI.
   *
   * @param uri -- ASV blob URI
   * @returns accountName -- the account name for the URI.
   * @throws URISyntaxException if the URI does not have an authority it is badly formed.
   */
  private String getAccountFromAuthority(URI uri) throws URISyntaxException {

    // Check to make sure that the authority is valid for the URI.
    //
    String authority = uri.getRawAuthority();
    if (null == authority){
      // Badly formed or illegal URI.
      //
      throw new URISyntaxException(uri.toString(), "Expected URI with a valid authority");
    }

    // Check if authority container the delimiter separating the account name from the
    // the container.
    //
    if (!authority.contains (ASV_AUTHORITY_DELIMITER)) {
      return authority;
    }

    // Split off the container name and the authority.
    //
    String [] authorityParts = authority.split(ASV_AUTHORITY_DELIMITER, 2);

    // Because the string contains an '@' delimiter, a container must be specified.
    //
    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      // Badly formed ASV authority since there is no container.
      //
      final String errMsg =
          String.format("URI '%' has a malformed ASV authority, expected container name." +
              "Authority takes the form" + " asv://[<container name>@]<account name>",
              uri.toString());
      throw new IllegalArgumentException(errMsg);
    }

    // Return with the container name. It is possible that this name is NULL.
    //
    return authorityParts[1];
  }

  /**
   * Method to extract the container name from an Azure URI.
   *
   * @param uri -- ASV blob URI
   * @returns containerName -- the container name for the URI. May be null.
   * @throws URISyntaxException if the uri does not have an authority it is badly formed.
   */
  private String getContainerFromAuthority(URI uri) throws URISyntaxException {

    // Check to make sure that the authority is valid for the URI.
    //
    String authority = uri.getRawAuthority();
    if (null == authority){
      // Badly formed or illegal URI.
      //
      throw new URISyntaxException(uri.toString(), "Expected URI with a valid authority");
    }

    // The URI has a valid authority. Extract the container name. It is the second
    // component of the ASV URI authority.
    //
    if (!authority.contains(ASV_AUTHORITY_DELIMITER)) {
      // The authority does not have a container name. Use the default container by
      // setting the container name to the default Azure root container.
      //
      return AZURE_ROOT_CONTAINER;
    }

    // Split off the container name and the authority.
    //
    String [] authorityParts = authority.split(ASV_AUTHORITY_DELIMITER, 2);

    // Because the string contains an '@' delimiter, a container must be specified.
    //
    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      // Badly formed ASV authority since there is no container.
      //
      final String errMsg =
          String.format("URI '%' has a malformed ASV authority, expected container name." +
              "Authority takes the form" + " asv://[<container name>@]<account name>",
              uri.toString());
      throw new IllegalArgumentException(errMsg);
    }

    // Set the container name from the first entry for the split parts of the authority.
    //
    return authorityParts[0];
  }

  /**
   * Get the appropriate return the appropriate scheme for communicating with
   * Azure depending on whether asv or asvs is specified in the target URI.
   *
   * return scheme - HTTPS if asvs is specified or HTTP if asv is specified.
   * throws URISyntaxException if session URI does not have the appropriate
   * scheme.
   * @throws AzureException
   */
  private String getHTTPScheme () throws URISyntaxException, AzureException {
    // Determine the appropriate scheme for communicating with Azure storage.
    //
    String sessionScheme = sessionUri.getScheme();
    if (null == sessionScheme) {
      // The session URI has no scheme and is malformed.
      //
      final String errMsg =
          String.format("Session URI does not have a URI scheme as is malformed.", sessionUri);
      throw new URISyntaxException(sessionUri.toString(), errMsg);
    }

    if (ASV_SCHEME.equals(sessionScheme.toLowerCase())){
      return HTTP_SCHEME;
    }

    if (ASV_SECURE_SCHEME.equals(sessionScheme.toLowerCase())){
      return HTTPS_SCHEME;
    }

    final String errMsg =
        String.format ("Scheme '%s://' not recognized by Hadoop ASV file system",sessionScheme);
    throw new AzureException (errMsg);
  }

  /**
   * Set the configuration parameters for this client storage session with Azure.
   * @throws AzureException
   * @throws ConfigurationException
   *
   */
  private void configureAzureStorageSession() throws ConfigurationException, AzureException {

    // Assertion: Target session URI already should have been captured.
    //
    if (sessionUri == null) {
      throw new AssertionError(
          "Expected a non-null session URI when configuring storage session");
    }

    // Assertion: A client session already should have been established with Azure.
    //
    if (storageInteractionLayer == null) {
      throw new AssertionError(
        String.format("Cannot configure storage session for URI '%s' " +
            "if storage session has not been established.",
            sessionUri.toString()));
    }

    // Determine whether or not concurrent reads and write are allowed on a blob at the
    // same time.
    //
    concurrentReadWrite = sessionConfiguration.getBoolean(
                                    KEY_CONCURRENT_READ_WRITE,
                                    DEFAULT_CONCURRENT_READ_WRITE);

    // Set up the minimum stream read block size and the write block
    // size.
    //
    storageInteractionLayer.setStreamMinimumReadSizeInBytes(
        sessionConfiguration.getInt(
                KEY_STREAM_MIN_READ_SIZE, DEFAULT_DOWNLOAD_BLOCK_SIZE));

    storageInteractionLayer.setWriteBlockSizeInBytes(
        sessionConfiguration.getInt(
                KEY_WRITE_BLOCK_SIZE, DEFAULT_UPLOAD_BLOCK_SIZE));

    // The job may want to specify a timeout to use when engaging the
    // storage service. The default is currently 90 seconds. It may
    // be necessary to increase this value for long latencies in larger
    // jobs. If the timeout specified is greater than zero seconds use
    // it,
    // otherwise use the default service client timeout.
    //
    int storageConnectionTimeout =
        sessionConfiguration.getInt(KEY_STORAGE_CONNECTION_TIMEOUT, 0);

    if (0 < storageConnectionTimeout) {
      storageInteractionLayer.setTimeoutInMs(storageConnectionTimeout * 1000);
    }

    // Set the concurrency values equal to the that specified in the
    // configuration file. If it does not exist, set it to the default
    // value calculated as double the number of CPU cores on the client
    // machine. The concurrency value is minimum of double the cores and
    // the read/write property.
    //
    int cpuCores = 2 * Runtime.getRuntime().availableProcessors();

    concurrentWrites = sessionConfiguration.getInt(
        KEY_CONCURRENT_CONNECTION_VALUE_OUT,
        Math.min(cpuCores, DEFAULT_CONCURRENT_WRITES));

    // Configure Azure throttling parameters.
    //
    configureSessionThrottling();
  }


  /**
   * Configure the throttling parameters. The throttling parameters are set
   * in the core-site.xml configuration file. If the parameter is not set in
   * the configuration object the default is used.
   *
   * @throws ConfigurationException if there are errors in configuration.
   * @throws AzureException if there is a problem creating file system timers.
   */
  private void configureSessionThrottling() throws ConfigurationException, AzureException{

    // Disable/Enable throttling.
    //
    disableBandwidthThrottling = sessionConfiguration.getBoolean(
        KEY_DISABLE_THROTTLING, DEFAULT_DISABLE_THROTTLING);

    // Get map reduce task timeout.
    //
    mapredTaskTimeout = sessionConfiguration.getInt(
        KEY_MAPRED_TASK_TIMEOUT, DEFAULT_MAPRED_TASK_TIMEOUT);


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
          String.format("Minimum throttled bandwidth threshold is %d bytes/s." +
              " Node count should be greater than 0.",
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
              storageDownloadBandwidth, bandwidthMinThreshold);
      throw new ConfigurationException(errMsg);
    }

    // Capture the number of nodes in the cluster.
    //
    int numNodes = sessionConfiguration.getInt(
        KEY_NUMBER_OF_SLOTS,
        DEFAULT_NUMBER_OF_SLOTS);
    if (numNodes < 1) {
      // Throw a configuration exception. The cluster should have at least 1
      // node.
      //
      String errMsg =
          String.format("Cluster is configured with a node count of %d." +
              " Node count should be greater than or equal to one.",
              numNodes);
      throw new ConfigurationException(errMsg);
    }

    // Initialize upload and download bandwidths.
    //
    bandwidth = new long [] {
        Math.max(bandwidthMinThreshold,
            storageUploadBandwidth / (numNodes * concurrentWrites)),

        Math.max(bandwidthMinThreshold,
            storageDownloadBandwidth / (numNodes * concurrentReads))
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
    if (!disableBandwidthThrottling) {
      throttleBandwidth(ASV_TIMER_NAME, successRateInterval);

      // Set the up the throttling bandwidth retry policy.
      //
      storageInteractionLayer.setRetryPolicyFactory(new BandwidthThrottleRetry ());
    }

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

    // Create default timer tasks for both rampup and rampdown.
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
   * Connect to Azure storage using anonymous credentials.
   *
   * @param uri - URI to target blob (R/O access to public blob)
   *
   * @throws StorageException raised on errors communicating with Azure storage.
   * @throws IOException raised on errors performing I/O or setting up the session.
   * @throws URISyntaxExceptions raised on creating mal-formed URI's.
   */
  private void connectUsingAnonymousCredentials(final URI uri)
      throws StorageException, IOException, URISyntaxException {
    // Use an HTTP scheme since the URI specifies a publicly accessible
    // container. Explicitly create a storage URI corresponding to the URI
    // parameter for use in creating the service client.
    //
    String accountName = getAccountFromAuthority(uri);
    URI storageUri = new URI(getHTTPScheme() + ":" + PATH_DELIMITER + PATH_DELIMITER +
        accountName);

    // Create the service client with anonymous credentials.
    //
    storageInteractionLayer.createBlobClient(storageUri);
    suppressRetryPolicyInClientIfNeeded();

    // Extract the container name from the URI.
    //
    String containerName = getContainerFromAuthority (uri);
    String containerUri = storageUri.toString() + PATH_DELIMITER + containerName;
    rootDirectory = storageInteractionLayer.getDirectoryReference(containerUri);

    // Capture the container reference.
    //
    container = storageInteractionLayer.getContainerReference(containerName);

    // Check for container existence, and our ability to access it.
    //
    try {
      if (!container.exists(getInstrumentedContext())) {
        throw new AzureException("Container " + containerName +
            " in account " + accountName + " not found, and we can't create " +
            " it using anoynomous credentials.");
      }
    } catch (StorageException ex) {
      throw new AzureException("Unable to access container " + containerName +
          " in account " + accountName +
          " using anonymous credentials, and no credentials found for them " +
          " in the configuration.", ex);
    }

    // Accessing the storage server unauthenticated using
    // anonymous credentials.
    //
    isAnonymousCredentials = true;

    // Configure Azure storage session.
    //
    configureAzureStorageSession();
  }

  private void connectUsingCredentials(String accountName,
      StorageCredentials credentials,
      String containerName)
          throws URISyntaxException, StorageException, AzureException {

    URI blobEndPoint;
    if (isStorageEmulatorAccount(accountName)) {
      CloudStorageAccount account =
          CloudStorageAccount.getDevelopmentStorageAccount();
      blobEndPoint = account.getBlobEndpoint();
      storageInteractionLayer.createBlobClient(account);
    } else {
      blobEndPoint = new URI(getHTTPScheme() + "://" +
          accountName);
      storageInteractionLayer.createBlobClient(blobEndPoint, credentials);
    }
    suppressRetryPolicyInClientIfNeeded();

    // Set the root directory.
    //
    String containerUri = blobEndPoint +
        PATH_DELIMITER +
        containerName;
    rootDirectory = storageInteractionLayer.getDirectoryReference(containerUri);

    // Capture the container reference for debugging purposes.
    //
    container = storageInteractionLayer.getContainerReference(containerUri.toString());

    // Can only create container if using account key credentials
    canCreateOrModifyContainer =
        credentials instanceof StorageCredentialsAccountAndKey;

    // Configure Azure storage session.
    //
    configureAzureStorageSession();
  }

  /**
   * Connect to Azure storage using account key credentials.
   */
  private void connectUsingConnectionStringCredentials(
      final String accountName, final String containerName, final String accountKey)
          throws InvalidKeyException, StorageException, IOException, URISyntaxException {
    // If the account name is "acc.blob.core.windows.net", then the
    // rawAccountName is just "acc"
    String rawAccountName = accountName.split("\\.")[0];
    StorageCredentials credentials =
        new StorageCredentialsAccountAndKey(rawAccountName, accountKey);
    connectUsingCredentials(accountName, credentials, containerName);
  }

  /**
   * Connect to Azure storage using shared access signature credentials.
   */
  private void connectUsingSASCredentials(
      final String accountName, final String containerName, final String sas)
          throws InvalidKeyException, StorageException, IOException, URISyntaxException {
    StorageCredentials credentials =
        new StorageCredentialsSharedAccessSignature(sas);
    connectUsingCredentials(accountName, credentials, containerName);
  }

  private boolean isStorageEmulatorAccount(final String accountName) {
    return accountName.equalsIgnoreCase(
        sessionConfiguration.get(
            STORAGE_EMULATOR_ACCOUNT_NAME_PROPERTY_NAME,
            DEFAULT_STORAGE_EMULATOR_ACCOUNT_NAME));
  }

  static String getAccountKeyFromConfiguration(String accountName,
      Configuration conf) throws KeyProviderException {
    String key = null;
    String keyProviderClass = conf.get(KEY_ACCOUNT_KEYPROVIDER_PREFIX + accountName);
    KeyProvider keyProvider = null;

    if (keyProviderClass == null) {
      // No key provider was provided so use the provided key as is.
      keyProvider = new SimpleKeyProvider();
    } else {
      // create an instance of the key provider class and verify it
      // implements KeyProvider
      Object keyProviderObject = null;
      try {
        Class<?> clazz = conf.getClassByName(keyProviderClass);
        keyProviderObject = clazz.newInstance();
      } catch (Exception e) {
        throw new KeyProviderException("Unable to load key provider class.", e);
      }
      if (!(keyProviderObject instanceof KeyProvider)) {
        throw new KeyProviderException(keyProviderClass +
            " specified in config is not a valid KeyProvider class.");
      }
      keyProvider = (KeyProvider) keyProviderObject;
    }
    key = keyProvider.getStorageAccountKey(accountName, conf);

    return key;
  }

  /**
   * Establish a session with Azure blob storage based on the target URI. The
   * method determines whether or not the URI target contains an explicit
   * account or an implicit default cluster-wide account.
   *
   * @throws AzureException
   * @throws IOException
   */
  private void createAzureStorageSession ()
      throws AzureException, IOException {

    // Make sure this object was properly initialized with references to
    // the sessionUri and sessionConfiguration.
    //
    if (null == sessionUri || null == sessionConfiguration) {
      throw new AzureException(
          "Filesystem object not initialized properly." +
          "Unable to start session with Azure Storage server.");
    }

    // File system object initialized, attempt to establish a session
    // with the Azure storage service for the target URI string.
    //
    try {
      // Inspect the URI authority to determine the account and use the account to
      // start an Azure blob client session using an account key for the
      // the account or anonymously.
      // For all URI's do the following checks in order:
      // 1. Validate that <account> can be used with the current Hadoop
      //    cluster by checking it exists in the list of configured accounts
      //    for the cluster.
      // 2. Look up the AccountKey in the list of configured accounts for the cluster.
      // 3. If there is no AccountKey, assume anonymous public blob access
      //    when accessing the blob.
      //
      // If the URI does not specify a container use the default root container under
      // the account name.
      //

      // Assertion: Container name on the session Uri should be non-null.
      //
      if (getContainerFromAuthority(sessionUri) == null) {
        throw new AssertionError(String.format(
            "Non-null container expected from session URI: %s.",
            sessionUri.toString()));
      }

      // Get the account name.
      //
      String accountName = getAccountFromAuthority(sessionUri);
      if (null == accountName) {
        // Account name is not specified as part of the URI. Throw indicating
        // an invalid account name.
        //
        final String errMsg =
            String.format("Cannot load ASV file system account name not" +
                " specified in URI: %s.",
                sessionUri.toString());
        throw new AzureException(errMsg);
      }
      instrumentation.setAccountName(accountName);

      String containerName = getContainerFromAuthority(sessionUri);
      instrumentation.setContainerName(containerName);

      // Check whether this is a storage emulator account.
      if (isStorageEmulatorAccount(accountName)) {
        // It is an emulator account, connect to it with no credentials.
        connectUsingCredentials(accountName, null, containerName);
        return;
      }

      // Check whether we have a shared access signature for that container.
      String propertyValue = sessionConfiguration.get(
          KEY_ACCOUNT_SAS_PREFIX + containerName +
          "." + accountName);
      if (propertyValue != null) {
        // SAS was found. Connect using that.
        connectUsingSASCredentials(accountName, containerName, propertyValue);
        return;
      }

      // Check whether the account is configured with an account key.
      //
      propertyValue = getAccountKeyFromConfiguration(accountName,
          sessionConfiguration);
      if (propertyValue != null) {

        // Account key was found.
        // Create the Azure storage session using the account
        // key and container.
        //
        connectUsingConnectionStringCredentials(getAccountFromAuthority(sessionUri),
            getContainerFromAuthority(sessionUri), propertyValue);

        // Return to caller
        //
        return;
      }

      // The account access is not configured for this cluster. Try anonymous access.
      //
      connectUsingAnonymousCredentials(sessionUri);

    } catch (Exception e) {
      // Caught exception while attempting to initialize the Azure File
      // System store, re-throw the exception.
      //
      throw new AzureException(e);
    }
  }

  private enum ContainerState {
    /**
     * We haven't checked the container state yet.
     */
    Unknown,
    /**
     * We checked and the container doesn't exist.
     */
    DoesntExist,
    /**
     * The container exists and doesn't have an ASV version stamp on it.
     */
    ExistsNoVersion,
    /**
     * The container exists and has an unsupported ASV version stamped on it.
     */
    ExistsAtWrongVersion,
    /**
     * The container exists and has the proper ASV version stamped on it.
     */
    ExistsAtRightVersion
  }

  private enum ContainerAccessType {
    /**
     * We're accessing the container for a pure read operation,
     * e.g. read a file.
     */
    PureRead,
    /**
     * We're accessing the container purely to write something,
     * e.g. write a file.
     */
    PureWrite,
    /**
     * We're accessing the container to read something then write,
     * e.g. rename a file.
     */
    ReadThenWrite
  }

  /**
   * This should be called from any method that does any modifications
   * to the underlying container: it makes sure to put the ASV current
   * version in the container's metadata if it's not already there.
   */
  private ContainerState checkContainer(ContainerAccessType accessType)
      throws StorageException, AzureException {
    synchronized (containerStateLock) {
      if (isOkContainerState(accessType)) {
        return currentKnownContainerState;
      }
      if (currentKnownContainerState ==
          ContainerState.ExistsAtWrongVersion) {
        String containerVersion = retrieveVersionAttribute(container);
        throw wrongVersionException(containerVersion);
      }
      // This means I didn't check it before or it  didn't exist or
      // we need to stamp the version. Since things may have changed by
      // other machines since then, do the check again and don't depend
      // on past information.

      // Sanity check: we don't expect this at this point.
      if (currentKnownContainerState == ContainerState.ExistsAtRightVersion) {
        throw new AssertionError("Unexpected state: " +
            currentKnownContainerState);
      }

      // Download the attributes - doubles as an existence check with just
      // one service call
      try {
        container.downloadAttributes(getInstrumentedContext());
      } catch (StorageException ex) {
        if (ex.getErrorCode().equals(StorageErrorCode.RESOURCE_NOT_FOUND.toString())) {
          currentKnownContainerState = ContainerState.DoesntExist;
        } else {
          throw ex;
        }
      }

      if (currentKnownContainerState == ContainerState.DoesntExist) {
        // If the container doesn't exist and we intend to write to it,
        // create it now.
        if (needToCreateContainer(accessType)) {
          storeVersionAttribute(container);
          container.create(getInstrumentedContext());
          currentKnownContainerState = ContainerState.ExistsAtRightVersion;
        }
      } else {
        // The container exists, check the version.
        String containerVersion = retrieveVersionAttribute(container);
        if (containerVersion != null) {
          if (!containerVersion.equals(CURRENT_ASV_VERSION)) {
            // At this point in time we consider anything other than current
            // version a wrong version. In the future we may add back-compat
            // to previous version.
            currentKnownContainerState = ContainerState.ExistsAtWrongVersion;
            throw wrongVersionException(containerVersion);
          } else {
            // It's our correct version.
            currentKnownContainerState = ContainerState.ExistsAtRightVersion;
          }
        } else {
          // No version info exists.
          currentKnownContainerState = ContainerState.ExistsNoVersion;
          if (needToStampVersion(accessType)) {
            // Need to stamp the version
            storeVersionAttribute(container);
            container.uploadMetadata(getInstrumentedContext());
            currentKnownContainerState = ContainerState.ExistsAtRightVersion;
          }
        }
      }
      return currentKnownContainerState;
    }
  }

  private AzureException wrongVersionException(String containerVersion) {
    return new AzureException("The container " + container.getName() +
        " is at an unsupported version: " + containerVersion +
        ". Current supported version: " + CURRENT_ASV_VERSION);
  }

  private boolean needToStampVersion(ContainerAccessType accessType) {
    // We need to stamp the version on the container any time we write to
    // it and we have the correct credentials to be able to write container
    // metadata.
    return accessType != ContainerAccessType.PureRead &&
        canCreateOrModifyContainer;
  }

  private static boolean needToCreateContainer(ContainerAccessType accessType) {
    // We need to pro-actively create the container (if it doesn't exist) if
    // we're doing a pure write. No need to create it for pure read or read-
    // then-write access.
    return accessType == ContainerAccessType.PureWrite;
  }

  private boolean isOkContainerState(ContainerAccessType accessType) {
    switch (currentKnownContainerState) {
      case Unknown: return false;
      case DoesntExist: return !needToCreateContainer(accessType);
      case ExistsAtRightVersion: return true;
      case ExistsAtWrongVersion: return false;
      case ExistsNoVersion:
        // If there's no version, it's OK if we don't need to stamp the version
        // or we can't anyway even if we wanted to.
        return !needToStampVersion(accessType);
      default:
        throw new AssertionError("Unknown access type: " + accessType);
    }
  }

  private boolean getUseTransactionalContentMD5() {
    return sessionConfiguration.getBoolean(KEY_CHECK_BLOCK_MD5, true);
  }

  private BlobRequestOptions getUploadOptions() {
    BlobRequestOptions options = new BlobRequestOptions();
    options.setStoreBlobContentMD5(sessionConfiguration.getBoolean(KEY_STORE_BLOB_MD5, false));
    options.setUseTransactionalContentMD5(getUseTransactionalContentMD5());
    options.setConcurrentRequestCount(concurrentWrites);
    return options;
  }

  private BlobRequestOptions getDownloadOptions() {
    BlobRequestOptions options = new BlobRequestOptions();
    options.setUseTransactionalContentMD5(getUseTransactionalContentMD5());
    return options;
  }

  @Override
  public DataOutputStream storefile(String key, PermissionStatus permissionStatus)
      throws AzureException {
    try {

      // Check if a session exists, if not create a session with the
      // Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg =
            String.format(
                "Storage session expected for URI '%s' but does not exist.",
                sessionUri);
        throw new AzureException(errMsg);
      }

      // Check if there is an authenticated account associated with the
      // file this instance of the ASV file system. If not the file system
      // has not been authenticated and all access is anonymous.
      //
      if (!isAuthenticatedAccess()) {
        // Preemptively raise an exception indicating no uploads are
        // allowed to anonymous accounts.
        //
        throw new AzureException(new IOException(
            "Uploads to public accounts using anonymous "
                + "access is prohibited."));
      }

      checkContainer(ContainerAccessType.PureWrite);

      /**
       * Note: Windows Azure Blob Storage does not allow the creation of arbitrary directory
       *      paths under the default $root directory.  This is by design to eliminate
       *      ambiguity in specifying a implicit blob address. A blob in the $root conatiner
       *      cannot include a / in its name and must be careful not to include a trailing
       *      '/' when referencing  blobs in the $root container.
       *      A '/; in the $root container permits ambiguous blob names as in the following
       *      example involving two containers $root and mycontainer:
       *                http://myaccount.blob.core.windows.net/$root
       *                http://myaccount.blob.core.windows.net/mycontainer
       *      If the URL "mycontainer/somefile.txt were allowed in $root then the URL:
       *                http://myaccount.blob.core.windows.net/mycontainer/myblob.txt
       *      could mean either:
       *        (1) container=mycontainer; blob=myblob.txt
       *        (2) container=$root; blob=mycontainer/myblob.txt
       *
       *      To avoid this type of ambiguity the Azure blob storage prevents arbitrary path
       *      under $root.  For a simple and more consistent user experience it was decided
       *      to eliminate the opportunity for creating such paths by making the $root container
       *      read-only under ASV.  (cf. JIRA HADOOP-254).
       */

      //Check that no attempt is made to write to blobs on default
      //$root containers.
      //
      if (AZURE_ROOT_CONTAINER.equals(getContainerFromAuthority(sessionUri))){
        // Azure containers are restricted to non-root containers.
        //
        final String errMsg =
            String.format(
                "Writes to '%s' container for URI '%s' are prohibited, " +
                    "only updates on non-root containers permitted.",
                    AZURE_ROOT_CONTAINER, sessionUri.toString());
        throw new AzureException(errMsg);
      }

      // Get the block blob reference from the store's container and
      // return it.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);

      // Set up request options.
      //
      BlobRequestOptions options = new BlobRequestOptions();
      options.setStoreBlobContentMD5(true);
      options.setConcurrentRequestCount(concurrentWrites);
      if (isBandwidthThrottled()){
          options.setRetryPolicyFactory(new BandwidthThrottleRetry ());
      }

      // Create the output stream for the Azure blob.
      //
      OutputStream outputStream = blob.openOutputStream(
          getUploadOptions(), getInstrumentedContext());

      // Return to caller with DataOutput stream.
      //
      DataOutputStream dataOutStream = new DataOutputStream(outputStream);
      return dataOutStream;
    } catch (Exception e) {
      // Caught exception while attempting to open the blob output stream.
      // Re-throw
      // as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Default permission to use when no permission metadata is found.
   * @return The default permission to use.
   */
  private static PermissionStatus defaultPermissionNoBlobMetadata() {
    return new PermissionStatus("", "", FsPermission.getDefault());
  }

  private static void storeMetadataAttribute(CloudBlockBlobWrapper blob,
      String key, String value) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null == metadata) {
      metadata = new HashMap<String, String> ();
    }
    metadata.put(key, value);
    blob.setMetadata(metadata);
  }

  private static String getMetadataAttribute(CloudBlockBlobWrapper blob,
      String key) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null == metadata || !metadata.containsKey(key)) {
      return null;
    }
    return metadata.get(key);
  }

  private void storePermissionStatus(CloudBlockBlobWrapper blob,
      PermissionStatus permissionStatus) {
    storeMetadataAttribute(blob,
        PERMISSION_METADATA_KEY, permissionJsonSerializer.toJSON(permissionStatus));
  }

  private PermissionStatus getPermissionStatus(CloudBlockBlobWrapper blob) {
    String permissionMetadataValue = getMetadataAttribute(blob,
        PERMISSION_METADATA_KEY);
    if (permissionMetadataValue != null) {
      return PermissionStatusJsonSerializer.fromJSONString(
          permissionMetadataValue);
    } else {
      return defaultPermissionNoBlobMetadata();
    }
  }

  private static void storeFolderAttribute(CloudBlockBlobWrapper blob) {
    storeMetadataAttribute(blob, IS_FOLDER_METADATA_KEY, "true");
  }

  private static void storeLinkAttribute(CloudBlockBlobWrapper blob,
      String linkTarget) {
    storeMetadataAttribute(blob,
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY, linkTarget);
  }

  private static String getLinkAttributeValue(CloudBlockBlobWrapper blob) {
    return getMetadataAttribute(blob,
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY);
  }

  private static boolean retrieveFolderAttribute(CloudBlockBlobWrapper blob) {
    HashMap<String, String> metadata = blob.getMetadata();
    return null != metadata && metadata.containsKey(IS_FOLDER_METADATA_KEY);
  }

  private static void storeVersionAttribute(CloudBlobContainerWrapper container) {
    HashMap<String, String> metadata = container.getMetadata();
    if (null == metadata) {
      metadata = new HashMap<String, String> ();
    }
    metadata.put(VERSION_METADATA_KEY, CURRENT_ASV_VERSION);
    container.setMetadata(metadata);
  }

  private static String retrieveVersionAttribute(CloudBlobContainerWrapper container) {
    HashMap<String, String> metadata = container.getMetadata();
    return metadata == null || !metadata.containsKey(VERSION_METADATA_KEY) ?
        null : metadata.get(VERSION_METADATA_KEY);
  }

  @Override
  public void storeEmptyFolder(String key, PermissionStatus permissionStatus) throws AzureException {

    if (null == storageInteractionLayer) {
      final String errMsg =
          String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
      throw new AssertionError(errMsg);
    }

    // Check if there is an authenticated account associated with the file
    // this instance of the ASV file system. If not the file system has not
    // been authenticated and all access is anonymous.
    //
    if (!isAuthenticatedAccess()) {
      // Preemptively raise an exception indicating no uploads are
      // allowed to anonymous accounts.
      //
      throw new AzureException(
          "Uploads to to public accounts using anonymous access is prohibited.");
    }

    try {
      checkContainer(ContainerAccessType.PureWrite);

      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);
      storeFolderAttribute(blob);
      blob.upload(new ByteArrayInputStream(new byte[0]), getInstrumentedContext());
    } catch (Exception e) {
      // Caught exception while attempting upload. Re-throw as an Azure
      // storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Stores an empty blob that's linking to the temporary file where're we're
   * uploading the initial data.
   */
  @Override
  public void storeEmptyLinkFile(String key, String tempBlobKey,
      PermissionStatus permissionStatus) throws AzureException {
    if (null == storageInteractionLayer) {
      final String errMsg =
          String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
      throw new AssertionError(errMsg);
    }
    // Check if there is an authenticated account associated with the file
    // this instance of the ASV file system. If not the file system has not
    // been authenticated and all access is anonymous.
    //
    if (!isAuthenticatedAccess()) {
      // Preemptively raise an exception indicating no uploads are
      // allowed to anonymous accounts.
      //
      throw new AzureException(
          "Uploads to to public accounts using anonymous access is prohibited.");
    }

    try {
      checkContainer(ContainerAccessType.PureWrite);

      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);
      storeLinkAttribute(blob, tempBlobKey);
      blob.upload(new ByteArrayInputStream(new byte[0]), getInstrumentedContext());
    } catch (Exception e) {
      // Caught exception while attempting upload. Re-throw as an Azure
      // storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * If the blob with the given key exists and has a link in its metadata
   * to a temporary file (see storeEmptyLinkFile), this method returns
   * the key to that temporary file.
   * Otherwise, returns null.
   */
  @Override
  public String getLinkInFileMetadata(String key) throws AzureException {
    if (null == storageInteractionLayer) {
      final String errMsg =
          String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
      throw new AssertionError(errMsg);
    }

    try {
      checkContainer(ContainerAccessType.PureRead);

      CloudBlockBlobWrapper blob = getBlobReference(key);
      blob.downloadAttributes(getInstrumentedContext());
      return getLinkAttributeValue(blob);
    } catch (Exception e) {
      // Caught exception while attempting download. Re-throw as an Azure
      // storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Private method to check for authenticated access.
   *
   * @ returns boolean -- true if access is credentialed and authenticated and
   * false otherwise.
   */
  private boolean isAuthenticatedAccess() throws AzureException {

    if (isAnonymousCredentials) {
      // Access to this storage account is unauthenticated.
      //
      return false;
    }
    // Access is authenticated.
    //
    return true;
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container depending on whether the
   * original file system object was constructed with a short- or long-form
   * URI. If the root directory is non-null the URI in the file constructor
   * was in the long form.
   *
   * @param includeMetadata if set, the listed items will have their metadata
   *                        populated already.
   *
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   *
   */
  private Iterable<ListBlobItem> listRootBlobs(boolean includeMetadata)
      throws StorageException, URISyntaxException {
    return rootDirectory.listBlobs(
        null, false,
        includeMetadata ?
            EnumSet.of(BlobListingDetails.METADATA) :
              EnumSet.noneOf(BlobListingDetails.class),
              null,
              getInstrumentedContext());
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container given a specified prefix for
   * the directory depending on whether the original file system object was
   * constructed with a short- or long-form URI. If the root directory is
   * non-null the URI in the file constructor was in the long form.
   *
   * @param aPrefix
   *            : string name representing the prefix of containing blobs.
   * @param includeMetadata if set, the listed items will have their metadata
   *                        populated already.
   *
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   *
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix,
      boolean includeMetadata)
          throws StorageException, URISyntaxException {

    return rootDirectory.listBlobs(aPrefix,
        false,
        includeMetadata ?
            EnumSet.of(BlobListingDetails.METADATA) :
              EnumSet.noneOf(BlobListingDetails.class),
              null,
              getInstrumentedContext());
  }

  /**
   * Appends the given string to the root directory's URI, and returns
   * the new URI.
   * @param pathSuffix The suffix to append.
   * @return The URI with the suffix appended.
   */
  private URI appendToRootDirectoryPath(String pathSuffix)
      throws URISyntaxException {
    return new URI(
        rootDirectory.getUri().getScheme(),
        rootDirectory.getUri().getHost(),
        rootDirectory.getUri().getPath() + pathSuffix,
        rootDirectory.getUri().getQuery(),
        rootDirectory.getUri().getFragment());
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container given a specified prefix for
   * the directory depending on whether the original file system object was
   * constructed with a short- or long-form URI.  It also uses the specified
   * flat or hierarchical option, listing details options, request options,
   * and operation context.
   *
   * @param aPrefix string name representing the prefix of containing blobs.
   * @param useFlatBlobListing - the list is flat if true, or hierarchical otherwise.
   * @param listingDetails - determine whether snapshots, metadata, commmitted/uncommitted data
   * @param options - object specifying additional options for the request. null = default options
   * @param opContext - context of the current operation
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   *
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix, boolean useFlatBlobListing,
      EnumSet<BlobListingDetails> listingDetails, BlobRequestOptions options,
      OperationContext opContext) throws StorageException, URISyntaxException {

    CloudBlobDirectoryWrapper directory = storageInteractionLayer.getDirectoryReference(
        appendToRootDirectoryPath(aPrefix).toString());

    return directory.listBlobs(
        null,
        useFlatBlobListing,
        listingDetails,
        options,
        opContext);
  }

  /**
   * This private method uses the root directory or the original container to
   * get the block blob reference depending on whether the original file
   * system object was constructed with a short- or long-form URI. If the root
   * directory is non-null the URI in the file constructor was in the long
   * form.
   *
   * @param aKey
   *            : a key used to query Azure for the block blob.
   * @returns blob : a reference to the Azure block blob corresponding to the
   *          key.
   * @throws URISyntaxException
   *
   */
  private CloudBlockBlobWrapper getBlobReference(String aKey)
      throws StorageException, URISyntaxException {

    CloudBlockBlobWrapper blob = storageInteractionLayer.getBlockBlobReference(
        appendToRootDirectoryPath(aKey).toString());
    // Return with block blob.
    return blob;
  }

  /**
   * This private method normalizes the key by stripping the container
   * name from the path and returns a path relative to the root directory
   * of the container.
   *
   * @param keyUri - adjust this key to a path relative to the root directory
   *
   * @returns normKey
   */
  private String normalizeKey(URI keyUri) {

    String normKey;

    // Strip the container name from the path and return the path
    // relative to the root directory of the container.
    //
    normKey = keyUri.getPath().split(PATH_DELIMITER, 3)[2];

    // Return the fixed key.
    //
    return normKey;
  }

  /**
   * This private method normalizes the key by stripping the container
   * name from the path and returns a path relative to the root directory
   * of the container.
   *
   * @param blob - adjust the key to this blob to a path relative to the root
   *               directory
   *
   * @returns normKey
   */
  private String normalizeKey(CloudBlockBlobWrapper blob) {
    return normalizeKey(blob.getUri());
  }

  /**
   * This private method normalizes the key by stripping the container
   * name from the path and returns a path relative to the root directory
   * of the container.
   *
   * @param blob - adjust the key to this directory to a path relative to the
   *               root directory
   *
   * @returns normKey
   */
  private String normalizeKey(CloudBlobDirectoryWrapper directory) {
    String dirKey = normalizeKey(directory.getUri());
    // Strip the last /
    if (dirKey.endsWith(PATH_DELIMITER)) {
      dirKey = dirKey.substring(0, dirKey.length() - 1);
    }
    return dirKey;
  }

  /**
   * Creates a new OperationContext for the Azure Storage operation that has
   * listeners hooked to it that will update the metrics for this file system.
   * @return The OperationContext object to use.
   */
  private OperationContext getInstrumentedContext() {

    OperationContext operationContext = new OperationContext();
    ResponseReceivedMetricUpdater.hook(
       operationContext,
       instrumentation,
       bandwidthGaugeUpdater,
       getBandwidthThrottleFeedback());

    // If bandwidth throttling is enabled, bind the operation context to listen
    // to SendingRequestEvents. These events call back into the Azure store to
    // determine whether the thread should be throttled with a delay.
    //
    if (isBandwidthThrottled() || isConcurrentReadWriteAllowed()) {
      SendRequestThrottle.bind(
        operationContext,
        getThrottleSendRequestCallback(),
        isConcurrentReadWriteAllowed());
    }

    if (testHookOperationContext != null) {
      operationContext =
          testHookOperationContext.modifyOperationContext(operationContext);
    }
    ErrorMetricUpdater.hook(operationContext, instrumentation);

    // Return the operation context.
    //
    return operationContext;
  }

  @Override
  public FileMetadata retrieveMetadata(String key) throws IOException {

    // Attempts to check status may occur before opening any streams so first,
    // check if a session exists, if not create a session with the Azure storage server.
    //
    if (null == storageInteractionLayer) {
      final String errMsg =
          String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
      throw new AssertionError(errMsg);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Retrieving metadata for " + key);
    }

    try {
      if (checkContainer(ContainerAccessType.PureRead) ==
          ContainerState.DoesntExist) {
        // The container doesn't exist, so spare some service calls and just
        // return null now.
        return null;
      }

      // Handle the degenerate cases where the key does not exist or the
      // key is a container.
      //
      if (key.equals("/")) {
        // The key refers to root directory of container.
        // Set the modification time for root to zero.
        //
        return new FileMetadata(key, 0,
            defaultPermissionNoBlobMetadata(), BlobMaterialization.Implicit);
      }

      CloudBlockBlobWrapper blob = getBlobReference(key);

      // Download attributes and return file metadata only if the blob
      // exists.
      //
      if (null != blob && blob.exists(getInstrumentedContext())) {

        if (LOG.isDebugEnabled()) {
          LOG.debug("Found " + key +
              " as an explicit blob. Checking if it's a file or folder.");
        }

        // The blob exists, so capture the metadata from the blob
        // properties.
        //
        blob.downloadAttributes(getInstrumentedContext());
        BlobProperties properties = blob.getProperties();

        if (retrieveFolderAttribute(blob)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(key + " is a folder blob.");
          }
          return new FileMetadata(key, properties.getLastModified().getTime(),
              getPermissionStatus(blob), BlobMaterialization.Explicit);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(key + " is a normal blob.");
          }

          return new FileMetadata(
              key, // Always return denormalized key with metadata.
              properties.getLength(),
              properties.getLastModified().getTime(),
              getPermissionStatus(blob));
        }
      }

      // There is no file with that key name, but maybe it is a folder.
      // Query the underlying folder/container to list the blobs stored
      // there under that key.
      //
      Iterable<ListBlobItem> objects =
          listRootBlobs(
              key,
              true,
              EnumSet.of(BlobListingDetails.METADATA),
              null,
              getInstrumentedContext());

      // Check if the directory/container has the blob items.
      //
      for (ListBlobItem blobItem : objects) {
        if (blobItem instanceof CloudBlockBlobWrapper) {
          LOG.debug(
              "Found blob as a directory-using this file under it to infer its properties " +
                  blobItem.getUri());

          blob = (CloudBlockBlobWrapper)blobItem;
          // The key specifies a directory. Create a FileMetadata object which specifies
          // as such.
          //
          BlobProperties properties = blob.getProperties();

          return new FileMetadata(key, properties.getLastModified().getTime(),
              getPermissionStatus(blob),
              BlobMaterialization.Implicit);
        }
      }

      // Return to caller with a null metadata object.
      //
      return null;

    } catch (Exception e) {
      // Re-throw the exception as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public DataInputStream retrieve(String key) throws AzureException, IOException {
    try {
      // Check if a session exists, if not create a session with the
      // Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg =
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AssertionError(errMsg);
      }
      checkContainer(ContainerAccessType.PureRead);

      // Set up request options for retry policy if it is bandwidth throttled.
      //
      BlobRequestOptions options = getDownloadOptions();
      if (isBandwidthThrottled()){
        if (null == options) {
          options = new BlobRequestOptions();
        }
        options.setRetryPolicyFactory(new BandwidthThrottleRetry());
      }

      // Get blob reference and open the input buffer stream.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);
      BufferedInputStream inBufStream = new BufferedInputStream(
          blob.openInputStream(options, getInstrumentedContext()));

      // Return a data input stream.
      //
      DataInputStream inDataStream = new DataInputStream(inBufStream);
      return inDataStream;
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public DataInputStream retrieve(String key, long startByteOffset)
      throws AzureException, IOException {
    try {
      // Check if a session exists, if not create a session with the
      // Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg =
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AssertionError(errMsg);
      }
      checkContainer(ContainerAccessType.PureRead);

      // Get blob reference and open the input buffer stream.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);


      // Set up request options for retry policy if it is bandwidth throttled.
      //
      BlobRequestOptions options = getDownloadOptions();
      if (isBandwidthThrottled()){
        if (null != options) {
          options = new BlobRequestOptions();
        }

        options.setRetryPolicyFactory(new BandwidthThrottleRetry());
      }

      // Open input stream and seek to the start offset.
      //
      InputStream in = blob.openInputStream(options, getInstrumentedContext());

      // Create a data input stream.
      //
      DataInputStream inDataStream = new DataInputStream(in);
      inDataStream.skip(startByteOffset);
      return inDataStream;
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public PartialListing list(String prefix, final int maxListingCount, final int maxListingDepth)
      throws IOException {
    return list(prefix, maxListingCount, maxListingDepth, null);
  }

  @Override
  public PartialListing list(String prefix, final int maxListingCount, final int maxListingDepth,
      String priorLastKey) throws IOException {
    return list(prefix, PATH_DELIMITER, maxListingCount, maxListingDepth, priorLastKey);
  }

  @Override
  public PartialListing listAll(String prefix, final int maxListingCount,
      final int maxListingDepth, String priorLastKey) throws IOException {
    return list(prefix, null, maxListingCount, maxListingDepth, priorLastKey);
  }

  /**
   * Searches the given list of {@link FileMetadata} objects for a directory with
   * the given key.
   * @param list The list to search.
   * @param key The key to search for.
   * @return The wanted directory, or null if not found.
   */
  private static FileMetadata getDirectoryInList(final Iterable<FileMetadata> list,
      String key) {
    for (FileMetadata current : list) {
      if (current.isDir() && current.getKey().equals(key)) {
        return current;
      }
    }
    return null;
  }

  private PartialListing list(String prefix, String delimiter,
      final int maxListingCount, final int maxListingDepth, String priorLastKey)
          throws IOException {
    try {
      checkContainer(ContainerAccessType.PureRead);

      if (0 < prefix.length() && !prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }

      Iterable<ListBlobItem> objects;
      if (prefix.equals("/")) {
        objects = listRootBlobs(true);
      } else {
        objects = listRootBlobs(prefix, true);
      }

      ArrayList<FileMetadata> fileMetadata = new ArrayList<FileMetadata>();
      for (ListBlobItem blobItem : objects) {
        // Check that the maximum listing count is not exhausted.
        //
        if (0 < maxListingCount
            && fileMetadata.size() >= maxListingCount) {
          break;
        }

        if (blobItem instanceof CloudBlockBlobWrapper) {
          String blobKey = null;
          CloudBlockBlobWrapper blob = (CloudBlockBlobWrapper) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine format of the blob name depending on whether an absolute
          // path is being used or not.
          //
          //
          blobKey = normalizeKey(blob);

          FileMetadata metadata;
          if (retrieveFolderAttribute(blob)) {
            metadata = new FileMetadata(blobKey,
                properties.getLastModified().getTime(),
                getPermissionStatus(blob),
                BlobMaterialization.Explicit);
          } else {
            metadata = new FileMetadata(
                blobKey,
                properties.getLength(),
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }

          // Add the metadata to the list, but remove any existing duplicate
          // entries first that we may have added by finding nested files.
          //
          FileMetadata existing = getDirectoryInList(fileMetadata, blobKey);
          if (existing != null) {
            fileMetadata.remove(existing);
          }
          fileMetadata.add(metadata);
        } else if (blobItem instanceof CloudBlobDirectoryWrapper) {
          CloudBlobDirectoryWrapper directory = (CloudBlobDirectoryWrapper) blobItem;
          // Determine format of directory name depending on whether an absolute
          // path is being used or not.
          //
          String dirKey = normalizeKey(directory);
          // Strip the last /
          if (dirKey.endsWith(PATH_DELIMITER)) {
            dirKey = dirKey.substring(0, dirKey.length() - 1);
          }

          // Reached the targeted listing depth. Return metadata for the
          // directory using default permissions.
          //
          // Note: Something smarter should be done about permissions. Maybe
          //       inherit the permissions of the first non-directory blob.
          //       Also, getting a proper value for last-modified is tricky.
          //
          FileMetadata directoryMetadata = new FileMetadata(dirKey, 0,
              defaultPermissionNoBlobMetadata(),
              BlobMaterialization.Implicit);

          // Add the directory metadata to the list only if it's not already there.
          //
          if (getDirectoryInList(fileMetadata, dirKey) == null) {
            fileMetadata.add(directoryMetadata);
          }

          // Currently at a depth of one, decrement the listing depth for
          // sub-directories.
          //
          buildUpList(directory, fileMetadata,
              maxListingCount, maxListingDepth - 1);
        }
      }
      // Note: Original code indicated that this may be a hack.
      //
      priorLastKey = null;
      return new PartialListing(priorLastKey,
          fileMetadata.toArray(new FileMetadata[] {}),
          0 == fileMetadata.size() ? new String[] {}
      : new String[] { prefix });
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Build up a metadata list of blobs in an Azure blob directory. This method
   * uses a in-order first traversal of blob directory structures to maintain
   * the sorted order of the blob names.
   *
   * @param dir -- Azure blob directory
   *
   * @param list -- a list of file metadata objects for each non-directory blob.
   *
   * @param maxListingLength -- maximum length of the built up list.
   */
  private void buildUpList(CloudBlobDirectoryWrapper aCloudBlobDirectory,
      ArrayList<FileMetadata> aFileMetadataList, final int maxListingCount,
      final int maxListingDepth) throws Exception {

    // Push the blob directory onto the stack.
    //
    AzureLinkedStack<Iterator<ListBlobItem>> dirIteratorStack =
        new AzureLinkedStack<Iterator<ListBlobItem>>();

    Iterable<ListBlobItem> blobItems = aCloudBlobDirectory.listBlobs(null,
        false, EnumSet.of(BlobListingDetails.METADATA), null,
        getInstrumentedContext());
    Iterator<ListBlobItem> blobItemIterator = blobItems.iterator();

    if (0 == maxListingDepth || 0 == maxListingCount)
    {
      // Recurrence depth and listing count are already exhausted. Return
      // immediately.
      //
      return;
    }

    // The directory listing depth is unbounded if the maximum listing depth
    // is negative.
    //
    final boolean isUnboundedDepth = (maxListingDepth < 0);

    // Reset the current directory listing depth.
    //
    int listingDepth = 1;

    // Loop until all directories have been traversed in-order. Loop only
    // the following conditions are satisfied:
    // (1) The stack is not empty, and
    // (2) maxListingCount > 0 implies that the number of items in the
    // metadata list is less than the max listing count.
    //
    while (null != blobItemIterator
        && (maxListingCount <= 0 || aFileMetadataList.size() < maxListingCount)) {
      while (blobItemIterator.hasNext()) {
        // Check if the count of items on the list exhausts the maximum
        // listing count.
        //
        if (0 < maxListingCount
            && aFileMetadataList.size() >= maxListingCount) {
          break;
        }

        ListBlobItem blobItem = blobItemIterator.next();

        // Add the file metadata to the list if this is not a blob
        // directory
        // item.
        //
        if (blobItem instanceof CloudBlockBlobWrapper) {
          String blobKey = null;
          CloudBlockBlobWrapper blob = (CloudBlockBlobWrapper) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine format of the blob name depending on whether an absolute
          // path is being used or not.
          //
          //
          blobKey = normalizeKey(blob);

          FileMetadata metadata;
          if (retrieveFolderAttribute(blob)) {
            metadata = new FileMetadata(blobKey,
                properties.getLastModified().getTime(),
                getPermissionStatus(blob),
                BlobMaterialization.Explicit);
          } else {
            metadata = new FileMetadata(
                blobKey,
                properties.getLength(),
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }

          // Add the directory metadata to the list only if it's not already there.
          //
          FileMetadata existing = getDirectoryInList(aFileMetadataList, blobKey);
          if (existing != null) {
            aFileMetadataList.remove(existing);
          }
          aFileMetadataList.add(metadata);
        } else if (blobItem instanceof CloudBlobDirectoryWrapper) {
          CloudBlobDirectoryWrapper directory = (CloudBlobDirectoryWrapper) blobItem;

          // This is a directory blob, push the current iterator onto
          // the stack of iterators and start iterating through the current
          // directory.
          //
          if (isUnboundedDepth || maxListingDepth > listingDepth)
          {
            // Push the current directory on the stack and increment the listing
            // depth.
            //
            dirIteratorStack.push(blobItemIterator);
            ++listingDepth;

            // The current blob item represents the new directory. Get
            // an iterator for this directory and continue by iterating through
            // this directory.
            //
            blobItems = directory.listBlobs(null,
                false, EnumSet.noneOf(BlobListingDetails.class), null,
                getInstrumentedContext());
            blobItemIterator = blobItems.iterator();
          } else {
            // Determine format of directory name depending on whether an absolute
            // path is being used or not.
            //
            String dirKey = normalizeKey(directory);

            if (getDirectoryInList(aFileMetadataList, dirKey) == null) {
              // Reached the targeted listing depth. Return metadata for the
              // directory using default permissions.
              //
              // Note: Something smarter should be done about permissions. Maybe
              //       inherit the permissions of the first non-directory blob.
              //       Also, getting a proper value for last-modified is tricky.
              //
              FileMetadata directoryMetadata = new FileMetadata(dirKey,
                  0,
                  defaultPermissionNoBlobMetadata(),
                  BlobMaterialization.Implicit);

              // Add the directory metadata to the list.
              //
              aFileMetadataList.add(directoryMetadata);
            }
          }
        }
      }

      // Traversal of directory tree

      // Check if the iterator stack is empty. If it is set the next blob
      // iterator to null. This will act as a terminator for the for-loop.
      // Otherwise pop the next iterator from the stack and continue looping.
      //
      if (dirIteratorStack.isEmpty()) {
        blobItemIterator = null;
      } else {
        // Pop the next directory item from the stack and decrement the
        // depth.
        //
        blobItemIterator = dirIteratorStack.pop();
        --listingDepth;

        // Assertion: Listing depth should not be less than zero.
        //
        if (listingDepth < 0) {
          throw new AssertionError("Non-negative listing depth expected");
        }
      }
    }
  }

  /**
   * Deletes the given blob, taking special care that if we get a
   * blob-not-found exception upon retrying the operation, we just
   * swallow the error since what most probably happened is that
   * the first operation succeeded on the server.
   * @param blob The blob to delete.
   * @throws StorageException
   */
  private void safeDelete(CloudBlockBlobWrapper blob) throws StorageException {
    OperationContext operationContext = getInstrumentedContext();
    try {
      blob.delete(operationContext);
    } catch (StorageException e) {
      // On exception, check that if:
      // 1. It's a BlobNotFound exception AND
      // 2. It got there after one-or-more retries THEN
      // we swallow the exception.
      if (e.getErrorCode() != null &&
          e.getErrorCode().equals("BlobNotFound") &&
          operationContext.getRequestResults().size() > 1 &&
          operationContext.getRequestResults().get(0).getException() != null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Swallowing delete exception on retry: " + e.getMessage());
        }
        return;
      } else {
        throw e;
      }
    }
  }

  @Override
  public void delete(String key) throws IOException {
    try {
      if (checkContainer(ContainerAccessType.ReadThenWrite) ==
          ContainerState.DoesntExist) {
        // Container doesn't exist, no need to do anything
        return;
      }

      // Get the blob reference an delete it.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);
      if (blob.exists(getInstrumentedContext())) {
        safeDelete(blob);
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public void rename(String srcKey, String dstKey)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving " + srcKey + " to " + dstKey);
    }

    try {
      // Attempts rename may occur before opening any streams so first,
      // check if a session exists, if not create a session with the Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg =
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AssertionError(errMsg);
      }

      checkContainer(ContainerAccessType.ReadThenWrite);
      // Get the source blob and assert its existence. If the source key
      // needs to be normalized then normalize it.
      //
      CloudBlockBlobWrapper srcBlob = getBlobReference(srcKey);

      if (!srcBlob.exists(getInstrumentedContext())) {
        throw new AzureException ("Source blob " + srcKey+ " does not exist.");
      }

      // Get the destination blob. The destination key always needs to be
      // normalized.
      //
      CloudBlockBlobWrapper dstBlob = getBlobReference(dstKey);

      // Rename the source blob to the destination blob by copying it to
      // the destination blob then deleting it.
      //
      dstBlob.copyFromBlob(srcBlob, getInstrumentedContext());
      safeDelete(srcBlob);
    } catch (Exception e) {
      // Re-throw exception as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Changes the permission status on the given key.
   */
  @Override
  public void changePermissionStatus(String key, PermissionStatus newPermission)
      throws AzureException {
    try {
      checkContainer(ContainerAccessType.ReadThenWrite);
      CloudBlockBlobWrapper blob = getBlobReference(key);
      blob.downloadAttributes(getInstrumentedContext());
      storePermissionStatus(blob, newPermission);
      blob.uploadMetadata(getInstrumentedContext());
    } catch (Exception e) {
      throw new AzureException(e);
    }
  }

  @Override
  public void purge(String prefix) throws IOException {
    try {

      // Attempts to purge may occur before opening any streams so first,
      // check if a session exists, if not create a session with the Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg =
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AssertionError(errMsg);
      }

      if (checkContainer(ContainerAccessType.ReadThenWrite) ==
          ContainerState.DoesntExist) {
        // Container doesn't exist, no need to do anything.
        return;
      }
      // Get all blob items with the given prefix from the container and delete
      // them.
      //
      Iterable<ListBlobItem> objects = listRootBlobs(prefix, false);
      for (ListBlobItem blobItem : objects) {
        ((CloudBlob) blobItem).delete(DeleteSnapshotsOption.NONE, null, null, getInstrumentedContext());
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public void dump() throws IOException {
  }

  @Override
  public void close() {
    bandwidthGaugeUpdater.close();
  }
}