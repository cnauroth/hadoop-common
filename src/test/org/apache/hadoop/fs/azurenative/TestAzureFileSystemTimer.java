package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azurenative.AzureFileSystemTimer.AzureFileSystemTimerCallbacks;
import org.junit.*;


/**
 * Unit tests for AzureFileSystemTimer class.
 *
 */

public class TestAzureFileSystemTimer  {

  /**
   * CONSTANTS
   */
  private final int NUM_THREADS   = 1;
  private final int START_AFTER   = 2;
  private final int STOP_AFTER    = 10;
  private final int ONE_SECOND    = 1000;
  private final int TIMER_PERIOD  = ONE_SECOND / 10; // 10 ms.

  private class TestAzureFileSystemTimerCallback implements 
    AzureFileSystemTimerCallbacks {
    
    private final UUID TIMER_CALLBACK_CONTEXT = UUID.randomUUID();
    private long tickCount    = 0;
    
    
    /**
     * Method implementing the Azure file system timer callback interface.
     */
    //
    public void tickEvent (Object timerCallbackContext) {
      
      // Validate that the context coming out corresponds to the one sent in.
      //
      assertTrue (TIMER_CALLBACK_CONTEXT.equals(timerCallbackContext));
      
      // Increment tick count.
      //
      tickCount++;
    }
    
    /**
     * Private getter for the tick count.
     */
    private long getTickCount() {
      return tickCount;
    }
    
    /**
     * Private getter for the timer callback context.
     */
    private Object getTimerContext() {
      return TIMER_CALLBACK_CONTEXT;
    }
  }

  
  /**
   * PRIVATE MEMBER VARIABLES
   */
  private ScheduledExecutorService scheduler;

  // Set up the unit test.
  //
  @Before
  public void setUp () throws Exception {
    // Setup the executor framework scheduler service.
    //
    scheduler = Executors.newScheduledThreadPool(NUM_THREADS);
  }

  // Tears down the unit test.
  //
  @After
  public void tearDown() throws Exception {
    // Shutdown the scheduler immediately if a scheduler exists.
    //
    if (null != scheduler) {
      scheduler.shutdownNow ();
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
    testTimer.turnOnTimer();

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Validate that timer is not expired.
    //
    assertFalse(testTimer.isExpired());

    // Validate that tick count is less than timer expiration ticks.
    //
    assertTrue(testTimer.getTicks() < STOP_AFTER);

    // Pause for duration of the timer.
    //
    Thread.sleep((STOP_AFTER + 1) * TIMER_PERIOD);

    // Validate that timer is expired after expiration ticks.
    //
    assertTrue(testTimer.isExpired());

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
    testTimer.turnOnTimer();

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Validate that timer is not expired.
    //
    assertFalse(testTimer.isExpired());

    // Validate that tick count is less than timer expiration ticks.
    //
    assertTrue(testTimer.getTicks() < STOP_AFTER + START_AFTER);

    // Pause for duration of the timer.
    //
    Thread.sleep(STOP_AFTER * TIMER_PERIOD);

    // Test that timer is not expired and accounts for delay
    //
    assertFalse(testTimer.isExpired());

    // Pause for an additional seconds to account for the delay.
    //
    Thread.sleep((STOP_AFTER - START_AFTER) * TIMER_PERIOD);

    // Test that timer is expired after accounting for delay
    //
    assertTrue(testTimer.isExpired());

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
    testTimer.turnOnTimer();

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Pause for delay duration of the timer.
    //
    Thread.sleep(START_AFTER * TIMER_PERIOD);

    // Test that timer is not expired.
    //
    assertFalse (testTimer.isExpired());

    // Reset the timer.
    //
    testTimer.resetTimer();

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Test that the timer is not expired.
    //
    assertFalse(testTimer.isExpired());

    // Pause for the length of the timer including the initial delay.
    //
    Thread.sleep((STOP_AFTER + START_AFTER + 1) * TIMER_PERIOD);

    // Validate the timer has expired.
    //
    assertTrue(testTimer.isExpired());

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());
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
    testTimer.turnOnTimer();

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Validate the timer is not expired.
    //
    assertFalse(testTimer.isExpired());

    // Pause for something short of the duration of the timer.
    //
    Thread.sleep((STOP_AFTER - 1) * TIMER_PERIOD);

    // Validate the timer is nt expired.
    //
    assertFalse(testTimer.isExpired());

    // Cancel the timer.
    //
    testTimer.turnOffTimer();

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());
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
    testTimer.turnOnTimer();

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Validate that timer is not expired.
    //
    assertFalse(testTimer.isExpired());

    // Pause for duration short of the expiration of the timer.
    //
    Thread.sleep((START_AFTER - 1) * TIMER_PERIOD);

    // Validate that the timer still is not expired.
    //
    assertFalse(testTimer.isExpired());

    // Cancel the timer.
    //
    testTimer.turnOffTimer();

    // Validate that timer is expired.
    //
    assertTrue(testTimer.isExpired());

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());
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
        "testTimerNoDelay", scheduler, START_AFTER, TIMER_PERIOD, AzureFileSystemTimer.INFINITE_DELAY);

    // Create alarm task and turn on timer.
    //
    testTimer.turnOnTimer();
    
    // Schedule cancellation of the timer after a delay of STOP_AFTER ticks.
    //
    testTimer.turnOffTimerAfterDelay(START_AFTER + STOP_AFTER);

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());

    // Validate that the timer is not expired.
    //
    assertFalse(testTimer.isExpired());

    // Pause for duration of the timer.
    //
    Thread.sleep((START_AFTER + STOP_AFTER + 1) * TIMER_PERIOD);

    // Validate that timer is expired.
    //
    assertTrue(testTimer.isExpired());

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());
  }

  /**
   * Negative test catching attempts to schedule shutdowns after infinite delays.
   *
   */
  @Test
  public void testScheduleTurnOffTimerWithInfiniteDelay () {

    // Create timer object with an initial delay of  START_AFTER, a period of
    // TIMER_PERIOD with no automatic stop.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, 0, TIMER_PERIOD, AzureFileSystemTimer.INFINITE_DELAY);
    try {
      testTimer.turnOnTimer();

      // Validate timer is turned on.
      //
      assertTrue(testTimer.isOn());

      // Validate taht timer is not expired.
      //
      assertFalse(testTimer.isExpired());

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

      // Turn off the timer.
      //
      testTimer.turnOffTimer();

      // Validate timer is expired.
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
      testTimer.turnOnTimer();

      // Validate the timer is on.
      //
      assertTrue(testTimer.isOn());

      // Validate that timer is not expired.
      //
      assertFalse(testTimer.isExpired());

      // Schedule cancellation of the timer after a delay of STOP_AFTER ticks.
      //
      testTimer.turnOffTimerAfterDelay(STOP_AFTER - 2);

      // Fail test
      //
      fail("Fail to trigger AzureException for scheduling a cancellation" +
          "on a timer which has already scheduled a cancellation.");

    } catch (Exception e) {
      // Fail test. No other exceptions expected.
      //
      final String errMsg =
          String.format("Exception '%s' is unexpected.", e.getMessage());
      fail(errMsg);
    } catch (AssertionError e) {
      // Expecting assertion error.
      //
      final String errMsg =
          String.format("Exception '%s' is expected.", e.getMessage());
      System.out.println(errMsg);
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
   * Test to that tick counts are being appropriately updated on the timer.
   */
  @Test
  public void testTickCounts ()
      throws AzureException, InterruptedException {
    // Create timer object with an initial delay 1 tick, a period of TIMER_PERIOD
    // second, and with an automatic stop after STOP_AFTER using a local scheduler.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, 0, TIMER_PERIOD, STOP_AFTER);

    // Create alarm task and turn on timer.
    //
    testTimer.turnOnTimer();

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());
    
    // Introduce short delay to make sure this thread lags the timer thread.
    //
    Thread.sleep(ONE_SECOND / 500);

    // Note: There is zero delay so the first tick is instantaneous.  So start
    //       counting at tick 1.
    //
    for (long ticks = 1; ticks < STOP_AFTER; ticks++) {
      // Validate timer is not expired.
      //
      assertFalse(testTimer.isExpired());

      // Check the tick count.
      //
      assertEquals (ticks, testTimer.getTicks());

      // Sleep of one tick period.
      //
      Thread.sleep(TIMER_PERIOD);
    }
    
    // Validate the timer is expired.
    //
    assertTrue(testTimer.isExpired());

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());
  }
  
  /**
   * Test to that tick counts are being appropriately updated on the timer.
   */
  @Test
  public void testTimerCallback ()
      throws AzureException, InterruptedException {
    // Create timer object with an initial delay 1 tick, a period of TIMER_PERIOD
    // second, and with an automatic stop after STOP_AFTER using a local scheduler.
    //
    AzureFileSystemTimer testTimer = new AzureFileSystemTimer(
        "testTimerNoDelay", scheduler, 0, TIMER_PERIOD, STOP_AFTER);
    
    // Create a test timer callback object.
    //
    TestAzureFileSystemTimerCallback timerCallback = 
                      new TestAzureFileSystemTimerCallback();

    // Create alarm task and turn on timer.
    //
    testTimer.turnOnTimer(timerCallback, timerCallback.getTimerContext());

    // Validate the timer is on.
    //
    assertTrue(testTimer.isOn());
    
    // Introduce short delay to make sure this thread lags the timer thread.
    //
    Thread.sleep(ONE_SECOND / 500);

    // Note: There is zero delay so the first tick is instantaneous.  So start
    //       counting at tick 1.
    //
    for (long ticks = 1; ticks < STOP_AFTER; ticks++) {
      // Validate timer is not expired.
      //
      assertFalse(testTimer.isExpired());

      // Check the tick count.
      //
      assertEquals (ticks, testTimer.getTicks());
      
      // Check the tick count on the object registered on the callback interface.
      //
      assertEquals (ticks, timerCallback.getTickCount());

      // Sleep of one tick period.
      //
      Thread.sleep(TIMER_PERIOD);
    }
    
    // Validate the timer is expired.
    //
    assertTrue(testTimer.isExpired());

    // Validate the timer is off.
    //
    assertTrue(testTimer.isOff());
  }
}