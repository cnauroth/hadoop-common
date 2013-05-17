package org.apache.hadoop.fs.azurenative;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.azure.AzureException;
import org.junit.Test;

import junit.framework.TestCase;

/**
 * Unit tests for AzureFileSystemTimer class.
 *
 */

public class TestAzureFileSystemTimer extends TestCase {

  /**
   * CONSTANTS
   */
  private final int NUM_THREADS   = 5;
  private final int START_AFTER   = 2;
  private final int STOP_AFTER    = 10;
  private final int TIMER_PERIOD  = 3;
  private final int ONE_SECOND    = 1000;

  /**
   * PRIVATE MEMBER VARIABLES
   */
  private ScheduledExecutorService scheduler;

  // Set up the unit test.
  //
  @Override
  protected void setUp () throws Exception {
    // Setup the executor framework scheduler service.
    //
    scheduler = Executors.newScheduledThreadPool(NUM_THREADS);
  }

  // Tears down the unit test.
  //
  @Override
  protected void tearDown() throws Exception {
    // Shutdown the scheduler immediately if a scheduler exists.
    //
    if (null != scheduler) {
      scheduler.shutdownNow ();
    }
  }

  private final class TurnOnAlarmTask implements Runnable {

    private AtomicInteger timerTicks;

    public TurnOnAlarmTask() {
      // Reset timer ticks.
      //
      timerTicks = new AtomicInteger(0);
    }

    public int ResetTicks() {
      int ticks = getTicks ();
      timerTicks.set(0);
      return ticks;
    }

    // Get tick count.
    //
    public int getTicks() {
      return timerTicks.get();
    }

    // Run method implementation.
    //
    public void run () {
      // Increment the timer ticks at the expiration of each
      // period.
      //
      timerTicks.incrementAndGet();
    }
  }


  /**
   * Test AzureFileSystem timer where there is no delay before starting
   * the timer.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTimerNoDelay () throws AzureException, InterruptedException {
    // Create timer object with an initial delay of 0, a period of TIMER_PERIOD
    // second, and with an automatic stop after STOP_AFTER.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, 0, TIMER_PERIOD, STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
    testTimer.turnOnTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for duration of the timer.
    //
    Thread.sleep((STOP_AFTER + 1) * TIMER_PERIOD * ONE_SECOND);

    // Validate a tick count of STOP_AFTER.
    //
    assertEquals(STOP_AFTER, alarmTask.getTicks());

    // Validate that the timer is off.
    //
    assertTrue(testTimer.isOff());
  }

  /**
   * Test AzureFileSystem turning on timer where there is a delay before starting
   * the timer.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTurnOnTimerWithDelay ()
      throws AzureException, InterruptedException {
    // Create timer object with an initial delay of START_AFTER, a period of
    // TIMER_PERIOD second, and with an automatic stop after STOP_AFTER.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, START_AFTER, TIMER_PERIOD, STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
    testTimer.turnOnTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for duration of the timer.
    //
    Thread.sleep(STOP_AFTER * TIMER_PERIOD * ONE_SECOND);

    // Tick count should be less than STOP_AFTER delay.
    //
    assertTrue(alarmTask.getTicks() < STOP_AFTER);

    // Pause for an additional 5 seconds to account for the delay.
    //
    Thread.sleep((STOP_AFTER - START_AFTER) * TIMER_PERIOD * ONE_SECOND);

    // Validate a tick count.
    //
    assertEquals(STOP_AFTER, alarmTask.getTicks());

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());
  }

  /**
   * Test AzureFileSystem resetting an AzureFileSystem timer where there is a delay
   * before starting the timer.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testResetTimerWithDelay ()
      throws AzureException, InterruptedException {
    // Create timer object with an initial delay of START_AFTER, a period of
    // TIMER_PERIOD second, and with an automatic stop after STOP_AFTER.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, START_AFTER, TIMER_PERIOD, STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
    testTimer.turnOnTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for duration of the timer.
    //
    Thread.sleep(START_AFTER * TIMER_PERIOD * ONE_SECOND);

    // Reset the timer.
    //
    assertTrue(alarmTask.getTicks() < STOP_AFTER);
    testTimer.resetTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for the length of the timer including the initial delay.
    //
    Thread.sleep((STOP_AFTER + START_AFTER + 2) * TIMER_PERIOD * ONE_SECOND);

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());

    // Validate a tick count greater than the STOP_AFTER period since the
    // tick count on the alarm task was not reset before rescheduling the
    // alarm task.
    //
    assertTrue(STOP_AFTER < alarmTask.getTicks());
  }

  /**
   * Test shutting off AzureFileSystem timer where there is no delay before
   * starting the timer.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTurnOffTimerAfterNoDelay ()
      throws AzureException, InterruptedException {
    // Create timer object with an initial delay of 0 seconds, a period of
    // TIMER_PERIOD second, and with an automatic stop after STOP_AFTER.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, 0, TIMER_PERIOD, STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
    testTimer.turnOnTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for duration of the timer.
    //
    Thread.sleep((STOP_AFTER - 5) * TIMER_PERIOD * ONE_SECOND);

    // Cancel the timer.
    //
    testTimer.turnOffTimer();

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());

    // Tick count should be less than STOP_AFTER.
    //
    assertTrue(alarmTask.getTicks() < STOP_AFTER);
  }

  /**
   * Test shutting off AzureFileSystem timer where there is a delay before
   * starting the timer.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTurnOffTimerAfterDelay ()
      throws AzureException, InterruptedException {
    // Create timer object with an initial delay of START_AFTER, a period of
    // TIMER_PERIOD second, and with an automatic stop after STOP_AFTER.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, START_AFTER, TIMER_PERIOD, STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
    testTimer.turnOnTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for duration of the timer.
    //
    Thread.sleep((START_AFTER - 1) * TIMER_PERIOD * ONE_SECOND);

    // Cancel the timer.
    //
    testTimer.turnOffTimer();

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());

    // Tick count should be less than STOP_AFTER.
    //
    assertEquals(0, alarmTask.getTicks());
  }

  /**
   * Test scheduling shutting off a timer with no automatic stop with no delay.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTurnOffInfiniteTimerWithDelay ()
      throws AzureException, InterruptedException {
    // Create timer object with an initial delay of  START_AFTER, a period of
    // TIMER_PERIOD with no automatic stop.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, START_AFTER, TIMER_PERIOD);

    // Schedule cancellation of the timer after a delay of STOP_AFTER ticks.
    //
    testTimer.turnOffTimerAfterDelay(START_AFTER + STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
    testTimer.turnOnTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for duration of the timer.
    //
    Thread.sleep((START_AFTER + STOP_AFTER + 1) * TIMER_PERIOD * ONE_SECOND);

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());

    // Tick count should be less than STOP_AFTER.
    //
    assertTrue(alarmTask.getTicks() <= STOP_AFTER);


  }

  /**
   * Negative test catching attempts to schedule shutdowns after infinite delays.
   *
   */
  @Test
  public void testScheduleTurnOffTimerWithInfiniteDelay () {
    try {
      // Create timer object with an initial delay of  START_AFTER, a period of
      // TIMER_PERIOD with no automatic stop.
      //
      AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
          "testTimerNoDelay", scheduler, 0, TIMER_PERIOD, STOP_AFTER);

      // Schedule cancellation of the timer after a delay of STOP_AFTER ticks.
      //
      testTimer.turnOffTimerAfterDelay(AzureFileSystemTimer.INFINITE_DELAY);

      // Fail test
      //
      fail("Fail to trigger illegal argument exception for scheduled timer " +
          "cancellations with inifinite delays.");

    } catch (IllegalArgumentException e) {
      // Correctly caught illegal argument exception while attempting cancellation
      // after infinite delay.
      //
      assertTrue (true);
    }
    catch (Exception e) {
      // Fail test. No other exceptions expected.
      //
      final String errMsg =
          String.format("Exception '%s' is unexpected.", e.getMessage());
      fail(errMsg);
    }
  }

  /**
   * Negative test catching attempts to schedule cancel timer when a cancellation
   * is already scheduled.
   */
  @Test
  public void testScheduleTurnOffTimerMultipeTimes()
      throws AzureException, InterruptedException {

    // Create timer object with an initial delay of  START_AFTER, a period of
    // TIMER_PERIOD with no automatic stop.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, 0, TIMER_PERIOD, STOP_AFTER);

    try {
      // Create alarm task and turn on timer.
      //
      TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
      testTimer.turnOnTimer(alarmTask);

      // Validate the timer is on.
      //
      assertTrue(testTimer.isOn());

      // Schedule cancellation of the timer after a delay of STOP_AFTER ticks.
      //
      testTimer.turnOffTimerAfterDelay(STOP_AFTER - 2);

      // Fail test
      //
      fail("Fail to trigger AzureException for scheduling a cancellation" +
          "on a timer which has already scheduled a cancellation.");

    } catch (AzureException e) {
      // Correctly caught AzureException exception while attempting cancellation
      // after infinite delay.
      //
      assertTrue (true);
    }
    catch (Exception e) {
      // Fail test. No other exceptions expected.
      //
      final String errMsg =
          String.format("Exception '%s' is unexpected.", e.getMessage());
      fail(errMsg);
    } finally {
      if (testTimer.isOn()){
        testTimer.turnOffTimer();
      }
    }

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOff());
  }

  /**
   * Test to shutdown Azure file system timer scheduler.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testShutdownScheduler () throws AzureException, InterruptedException {

    // Start up a local scheduler.
    //
    ScheduledExecutorService localScheduler =
        Executors.newScheduledThreadPool(NUM_THREADS);

    // Create timer object with an initial delay of 0, a period of TIMER_PERIOD
    // second, and with an automatic stop after STOP_AFTER using a local scheduler.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", localScheduler, 0, TIMER_PERIOD, STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
    testTimer.turnOnTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for duration of two timer ticks.
    //
    Thread.sleep(TIMER_PERIOD * 2 * ONE_SECOND);

    // Shutdown scheduler.
    //
    testTimer.shutdownScheduler();

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());

    // Scheduler should be shutdown since the timer is cancelled before
    // shutting down the scheduler.
    //
    assertTrue(localScheduler.isShutdown());
  }

  /**
   * Test to schedule shutdown Azure file system timer scheduler after a delay
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testShutdownSchedulerAfterDelay ()
      throws AzureException, InterruptedException {

    // Start up a local scheduler.
    //
    ScheduledExecutorService localScheduler =
        Executors.newScheduledThreadPool(NUM_THREADS);

    // Create timer object with an initial delay of 0, a period of TIMER_PERIOD
    // second, and with an automatic stop after STOP_AFTER using a local scheduler.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", localScheduler, 0, TIMER_PERIOD, STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
    testTimer.turnOnTimer(alarmTask);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Shutdown scheduler after a delay of two timer ticks.
    //
    testTimer.shutdownSchedulerAfterDelay(2);

    // Sleep for three timer ticks to ensure shutdown was executed.
    //
    Thread.sleep(TIMER_PERIOD * 3 * ONE_SECOND);

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());

    // Scheduler should be shutdown since the timer is cancelled before
    // shutting down the scheduler.
    //
    assertTrue(localScheduler.isShutdown());
  }

  /**
   * Negative test to schedule shutdown Azure file system timer scheduler
   * after infinite delay.
   */
  @Test
  public void testShutdownSchedulerAfterInfiniteDelay () {

    // Start up a local scheduler.
    //
    ScheduledExecutorService localScheduler =
        Executors.newScheduledThreadPool(NUM_THREADS);
    // Create timer object with an initial delay of 0, a period of TIMER_PERIOD
    // second, and with an automatic stop after STOP_AFTER using a local scheduler.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", localScheduler, 0, TIMER_PERIOD, STOP_AFTER);
    try {

      // Create alarm task and turn on timer.
      //
      TurnOnAlarmTask alarmTask = new TurnOnAlarmTask();
      testTimer.turnOnTimer(alarmTask);

      // Validate the timer is on.
      //
      assertTrue(testTimer.isOn());

      // Shutdown scheduler after an infinite delay.
      //
      testTimer.shutdownSchedulerAfterDelay(AzureFileSystemTimer.INFINITE_DELAY);

      // Test failed because it did not catch illegal argument exceptions.
      //
      fail("Expected illegal argument exception when scheduling" +
          "timer scheduler shutdowns with infinite delays.");
    } catch (IllegalArgumentException e) {
      // Caught illegal argument exception as expected.
      //
      assertTrue(true);
    } catch (Exception e) {
      // Fail test caught unexpected exception.
      //
      final String errMsg =
          String.format("Exception '%s' is unexpected.", e.getMessage());
      fail(errMsg);
    } finally {
      // Turn of timer if it is on.
      //
      if (testTimer.isOn()) {
        testTimer.turnOffTimer();
      }
      // Shutdown local scheduler if not already shutdown.
      //
      if (!localScheduler.isShutdown()){
        localScheduler.shutdownNow();
      }
    }

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());
  }
}
