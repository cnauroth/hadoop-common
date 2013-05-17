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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.azure.AzureException;

/**
 * The AzureFileSystemTimer class schedules periodic tasks using the Java
 * Executor Framework. Users of the class may schedule tasks for execution
 * in the future. These tasks can run periodically, or just once.
 *
 * Note: The timers have a 1 second granularity. If necessary, the granularity
 *       can be finer but we don't have a need for it today.
 */
public final class AzureFileSystemTimer {

  /**
   * Class CONSTANTS
   */
  public static final long INFINITE_DELAY = -1;
  public static final boolean DONT_INTERRUPT_RUNNING_TASKS = false;
  public static final String DEFAULT_TIMER_NAME = "default timer";

  /**
   * Class member variables.
   */
  private long initialDelayTicks;             // Initial delay as a timer tick count.
  private long timerPeriod;                   // Length of a tick count.
  private long shutdownDelayTicks;            // Stop timer sched. after tick count.
  private String timerName;                   // Timer name.
  private ScheduledFuture<?> timerFuture;     // Future for scheduled timer task.
  private ScheduledFuture<?> shutdownFuture;  // Future for scheduled shutdown task.
  private ScheduledExecutorService scheduler; // Timer scheduler.

  /**
   * Default timer task which does nothing on a timer tick.
   *
   */
  public static class DefaultTimerTask implements Runnable {
    /**
     * Run method implementation for the DefaultTimer task.
     */
    public void run() {
      // Do nothing on each timer tick. This is a default timer task the on/off
      // status of the timer is used to determine the timer state.
    }
  }

  /**
   * Constructor for the executor Azure file system class capturing the timer name,
   * the executor scheduler, the delay before starting, the period between alarms.
   * There is no automatic shutdown delay.  Shutdowns have to come explicitly from
   * the user.
   *
   * @param alarmName - moniker for the timer
   * @param scheduler - executor scheduler initialized by the caller
   * @param startAfter - delay this number of seconds before starting the timer
   * @param timerPeriod - period of the timer
   */
  public AzureFileSystemTimer (String alarmName, ScheduledExecutorService scheduler,
      long startAfter, long timerPeriod){

    // Initialize member variables with an infinite shutdown delay.
    //
    initializeAzureFileSystemTimer (
        alarmName, scheduler, startAfter, timerPeriod, INFINITE_DELAY);
  }

  /**
   * Constructor for the executor Azure file system class capturing the timer name,
   * the executor scheduler, the delay before starting, the period between alarms,
   * and the a time after which the timer is shutdown.
   *
   * @param alarmName - moniker for the timer
   * @param scheduler - executor scheduler initialized by the caller
   * @param startAfter - delay this number timer ticks before starting the timer
   * @param timerPeriod - period of the timer in seconds
   * @param stopAfter - shutdown timer and scheduler after this number of ticks.
   */
  public AzureFileSystemTimer (String alarmName, ScheduledExecutorService scheduler,
      long startAfter, long timerPeriod, long stopAfter){

    // Initialize member variables with a finite shutdown delay.
    //
    initializeAzureFileSystemTimer (
        alarmName, scheduler, startAfter, timerPeriod, stopAfter);
  }

  /**
   * Initialize member variables on AzureFileSystemTimer object.
   */
  private void initializeAzureFileSystemTimer (String alarmName,
      ScheduledExecutorService scheduler, long startAfter, long timerPeriod,
      long stopAfter) {

    // Capture the state of the time object by assigning to the appropriate member
    // variables.
    //
    this.timerName = (null != alarmName) ? alarmName : DEFAULT_TIMER_NAME;
    this.initialDelayTicks = startAfter;
    this.timerPeriod = timerPeriod;
    this.shutdownDelayTicks = stopAfter;
    this.scheduler = scheduler;
  }

  private final class TurnOffTimerTask implements Runnable {

    public void run () {
      turnOffTimer();
    }
  }

  private final class ShutdownTimerTask implements Runnable {
    public void run () {
      shutdownScheduler();
    }
  }

  /**
   * Execute a timer task after the initial delay with the period interval delay.
   *
   * @param timerTask - timer task to be executed when alarm fires.
   * @throws AzureException thrown on illegal shutdown actions.
   */
  public void turnOnTimer (Runnable timerTask) throws AzureException {
    // Schedule the new timer task if one is not scheduled already. If a timer is
    // scheduled ignore the request. This implies that turnOnTimer is idempotent
    // it can be issued multiple times without affecting the scheduled timer. To
    // cancel and recreate a new timer over the existing one, the caller will have
    // to call reset timer.
    //
    timerFuture = scheduler.scheduleWithFixedDelay(
        timerTask,
        initialDelayTicks * timerPeriod, timerPeriod,
        TimeUnit.SECONDS);

    // If the shutdown delay is not infinite schedule a task to automatically
    // stop the timer.  The shutdown delay is relative to the the inital delay.
    //
    if (null == shutdownFuture && INFINITE_DELAY != shutdownDelayTicks){
      // Schedule a task to shutdown the timer.
      //
      turnOffTimerAfterDelay (initialDelayTicks + shutdownDelayTicks);
    }
  }

  /**
   * Report whether or not a timer is turned on.
   *
   * @return boolean - true if the timer has been scheduled and false otherwise.
   */
  public boolean isOn () {
    // The timer is on if it has a non-null timer future.
    //
    return null != timerFuture;
  }

  /**
   * Report whether or not a timer is turned off.
   *
   * @return boolean - true if the timer has been scheduled and false otherwise.
   */
  public boolean isOff () {
    // The timer is off if it has a null timer future.
    //
    return null == timerFuture;
  }

  /**
   * Reset timer. Cancel timer and restart again with the same delay interval,
   * the same period, and the same shutdown delay.
   *
   * @param timerTask - timer task to be executed when alarm fires.
   * @throws AzureException thrown on illegal shutdown actions.
   */
  public void resetTimer (Runnable timerTask) throws AzureException {
    // Cancel the timer and reschedule.  If the task associated with the current
    // future is running, do not interrupt it. It should run to completion. This
    // implies that tasks should be thread-safe.
    //
    turnOffTimer();

    // Cancel scheduler shutdown if one is scheduled.
    //
    if (null != shutdownFuture) {
      shutdownFuture.cancel(DONT_INTERRUPT_RUNNING_TASKS);
      shutdownFuture = null;
    }

    // Schedule the new timer task.
    //
    turnOnTimer(timerTask);
  }

  /**
   * Cancel a timer if it is scheduled or simply return if it is not scheduled.
   * This method is idempotent since canceling an already cancelled time has no
   * effect on the timer.
   */
  public void turnOffTimer () {
    // Check if the timer was scheduled to determine whether or not it should be
    // explicitly cancelled. If the timer is not scheduled simply return. This
    // implies timer cancellation is idempotent.
    //
    if (null != timerFuture) {
      // Task has been scheduled, explicitly cancel it.
      //
      timerFuture.cancel(DONT_INTERRUPT_RUNNING_TASKS);
      timerFuture = null;
    }
  }

  /**
   * Turn of a timer after a given delay.
   * @param delayTicks
   * @throws AzureException thrown on illegal shutdown actions.
   */
  public void turnOffTimerAfterDelay (final long delayTicks) throws AzureException {
    // Check incoming parameter to make sure we are not attempting to automatically
    // turn of timer after an infinite delay.
    //
    if (INFINITE_DELAY == delayTicks) {
      final String errMsg =
          String.format(
              "Illegal attempt to automatically stop timer %s after infinite delay.",
              timerName);
      throw new IllegalArgumentException (errMsg);
    }

    // Check if the shutdown time for the timer is set.  If so throw an exception
    // indicating that a shutdown was already set for his timer.
    //
    if (null != shutdownFuture) {
      final String errMsg =
          String.format(
              "Illegal attempt to schedule an automatic cancellation on timer %s " +
                  "because a cancellation is already scheduled.", timerName);
      throw new AzureException (errMsg);
    }

    // Schedule timer shutdown.
    //
    TurnOffTimerTask turnOffTask = new TurnOffTimerTask();
    shutdownFuture = scheduler.schedule(
        turnOffTask, delayTicks * timerPeriod, TimeUnit.SECONDS);
  }

  /**
   * This method overloads turning off the timer and triggering the scheduler
   * shutdown.
   */
  public void shutdownScheduler () {
    // Timer is scheduled, cancel it.  This is an optimization if this is the only
    // timer scheduled by the scheduler leading to an immediate shutdown.
    //
    turnOffTimer();

    // Shutdown the scheduler if it is not already in shutdown mode.
    //
    if (!scheduler.isShutdown()) {// This guard maybe overkill since shutdown is
      // idempotent.

      // Shutdown the scheduler.
      //
      scheduler.shutdown();
    }
  }

  /**
   * Automatically shutdown the scheduler after a given delay period.
   *
   * @param delayTicks - delay before scheduling the shutdown.
   * @throws AzureException on timer errors.
   */
  public void shutdownSchedulerAfterDelay(final long delayTicks) {
    // Check incoming parameter to make sure we are not attempting to automatically
    // turn of timer after an infinite delay.
    //
    if (INFINITE_DELAY == delayTicks) {
      final String errMsg =
          String.format(
              "Illegal attempt to automatically stop timer %s after infinite delay.",
              timerName);
      throw new IllegalArgumentException (errMsg);
    }

    // Only schedule timer and scheduler shutdown if the scheduler is not being
    // already shutdown.
    //
    if  (!scheduler.isShutdown()){
      ShutdownTimerTask shutdownTask = new ShutdownTimerTask();
      scheduler.schedule(shutdownTask, delayTicks * timerPeriod, TimeUnit.SECONDS);
    }
  }
}
