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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Task to calculate success rates on every timer success rate timer tick.
 *
 */
public class ThrottlerTxSuccessRateTask implements Runnable {
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