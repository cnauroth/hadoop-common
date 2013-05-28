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

import org.apache.commons.logging.*;

import com.microsoft.windowsazure.services.core.storage.OperationContext;
import com.microsoft.windowsazure.services.core.storage.RetryPolicy;
import com.microsoft.windowsazure.services.core.storage.RetryPolicyFactory;
import com.microsoft.windowsazure.services.core.storage.RetryResult;

/**
 * Factory for custom bandwidth throttling retry policies. When throttling is enabled
 * this factory will be registered with the CloudBlobClient to generate RetryPolicy
 * objects which intercepts and ignores HTTP 500 and HTTP 503 exceptions to allow our
 * bandwidth throttling mechanism to modulate bandwidth utilization.
 *
 */
public class BandwidthThrottleRetry extends RetryPolicy implements RetryPolicyFactory {

  public static final Log LOG = LogFactory.getLog(BandwidthThrottleRetry.class);

  // Default absolute maximum number of I/O retries when throttling before giving up.
  //
  private static final int THROTTLE_MAX_IO_RETRIES = 10;

  /**
   * Default constructor for BandwithThrottleRetry objects.
   */
  public BandwidthThrottleRetry() {
    // Set the maximum number of retries.
    //
    this(THROTTLE_MAX_IO_RETRIES);
  }

  /**
   * Constructor for BandwithThrottleRetry objects.
   */
  public BandwidthThrottleRetry(int maxRetries) {
    // Set the maximum number of retries.
    //
    super(0, maxRetries);
  }

  /**
   * Generate a new retry policy for the current request attempt represented by the
   * operation context.
   *
   * @param opContext
   *          The context of the current operation is represented by an operation
   *          context object which tracks requests to the storage service and
   *          provides additional runtime information about the current operation.
   *
   * @return A Retry policy object.
   */
  @Override
  public RetryPolicy createInstance(OperationContext opContext) {
    return new BandwidthThrottleRetry();
  }

  /**
   * Determines if the operation should be retried and how long to wait
   * until the next retry.
   *
   * @param currentRetryCount
   *        The number of retries for the given operation. A value of zero.
   *        signifies this is the first error encountered.
   * @param statusCode
   *        The status code of the last execution of this operation.
   * @param lastException
   *        An <code>Exception</code> object reference to the last exception
   *        encountered.
   * @param opContext
   *        An <code>OperationContext</code> object representing the context
   *        of the current operation. This object tracks requests to the
   *        storage service, and provides additional runtime information
   *        about the operation.
   *
   * @return A <code>RetryResult</code> object represents the retry result
   *        indicating whether or not the operation should be retried after
   *        a given back off period.
   */
  @Override
  public RetryResult shouldRetry(int currentRetryCount, int statusCode,
          Exception lastException, OperationContext opContext) {

    // Retry only if the current retry count is less than the absolute
    // maximum number of retries.
    //
    boolean canRetry = false;
    if (currentRetryCount < maximumAttempts) {
      canRetry = true;
      final String infoMsg =
          String.format("BandwithThrottleRetry.shouldRetry attempt %d",
                        currentRetryCount);
      LOG.info(infoMsg);
    } else {
      final String infoMsg =
          String.format("BandwidthThrottleRetry.shouldRetry is giving up after " +
                        "%d attempts.", currentRetryCount);
      LOG.info(infoMsg);
    }

    // If not past the maximum number of retries, retry instantaneously
    // with no back off. The throttling logic will determine the backoff
    // delay.
    //
    return new RetryResult(0, canRetry);
  }
}