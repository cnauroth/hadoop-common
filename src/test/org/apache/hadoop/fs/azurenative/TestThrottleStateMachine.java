package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.azurenative.BandwidthThrottle.ThrottleType;
import org.apache.hadoop.fs.azurenative.ThrottleStateMachine.ThrottleState;
import org.junit.*;


/**
 * Unit tests for AzureFileSystemTimer class.
 *
 */

public class TestThrottleStateMachine  {

  /**
   * CONSTANTS
   */
  private final String TIMER_NAME = "TestThrottleStateMachineTimer";
  private final int LATENCY       = 100; // milliseconds
  private final int NUM_THREADS   = 3;
  private final int STOP_AFTER    = 2;
  private final int TIMER_PERIOD  = 1000; // milliseconds.
  
  /**
   * PRIVATE MEMBER VARIABLES
   */
  private ThrottleStateMachine throttleSM;
  private ScheduledExecutorService scheduler;

  // Set up the unit test.
  //
  @Before
  public void setUp () throws Exception {
    // Setup the executor framework scheduler service.
    //
    scheduler = Executors.newScheduledThreadPool(NUM_THREADS);
    
    // Create an instance of the throttle state machine.
    //
    throttleSM = new ThrottleStateMachine(
        TIMER_NAME, scheduler, TIMER_PERIOD, STOP_AFTER);
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
   * Get the most current latency from the throttling state machine.
   * 
   */
  private long getLatency(ThrottleType kindOfThrottle){
    long latency = throttleSM.getCurrentLatency(kindOfThrottle);
    if (latency <= 0) {
      // Caught latency on a roll over.
      //
      latency = throttleSM.getPreviousLatency(kindOfThrottle);
    }
    
    // Return with the most recent valid latency.
    //
    return latency;
  }
  
  /**
   * Test which verifies that the throttling state machine remains in
   * the stable state when there are no transmission failure events.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testEquilibriumState() throws InterruptedException {
    // Fire success events every 10 milliseconds over a period of one second.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(LATENCY);
      throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
    }
    
    // Average latency should be 10.
    //
    assertEquals(LATENCY, getLatency(ThrottleType.UPLOAD));
    
    // Validate the state machine is still in the equilibrium state.
    //
    assertEquals(ThrottleState.THROTTLE_NONE, throttleSM.getState(ThrottleType.UPLOAD));
    
    // Neither ramp up or ramp down is expected by the throttling engine.
    //
    assertFalse(throttleSM.rampDown(ThrottleType.UPLOAD));
    assertFalse(throttleSM.rampUp(ThrottleType.UPLOAD));
  }
  
  
  /**
   * Test which verifies that the throttling state machine enters and remains
   * in the RAMPDOWN state when there are continued failures.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTransitionToRampDownState() throws InterruptedException{
    // Fire an equal number of success and failure events over a one second period.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(10);
      if (0 == i % 2) {
        throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
      } else {
        throttleSM.updateTransmissionFailure(ThrottleType.UPLOAD, 1, LATENCY);
      }
    }
    
    // Average latency should be LATENCY.
    //
    assertEquals(LATENCY, getLatency(ThrottleType.UPLOAD));
    
    // Validate the state machine is still in the RAMPDOWN state.
    //
    assertEquals(ThrottleState.THROTTLE_RAMPDOWN, throttleSM.getState(ThrottleType.UPLOAD));
    
    // Validate that throttling engine cannot ramp up.
    //
    assertFalse(throttleSM.rampUp(ThrottleType.UPLOAD))
    ;
    // Validate that throttling engine can ramp down.
    //
    assertTrue(throttleSM.rampDown(ThrottleType.UPLOAD));
    
    // Validate that only one ramp down is allowed within two throttle intervals.
    //
    assertFalse(throttleSM.rampDown(ThrottleType.UPLOAD));
  }
  
  /**
   * Test which verifies that the throttling state machine enters and remains
   * in the RAMPUP state when there are continued successes.
   *
   * @throws AzureException if there is a timer error.
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTransitionToRampUpState() throws InterruptedException{
    // Fire an equal number of success and failure events over a one second period.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(LATENCY);
      if (0 == i % 2) {
        throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
      } else {
        throttleSM.updateTransmissionFailure(ThrottleType.UPLOAD, 1, LATENCY);
      }
    }
    
    // Validate the state machine is still in the RAMPDOWN state.
    //
    assertEquals(throttleSM.getState(ThrottleType.UPLOAD), ThrottleState.THROTTLE_RAMPDOWN);
    
    // State machine should return to RAMPUP state after sustained transmission successes.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(LATENCY);
      throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
    }
    
    // Validate that the state machine is in the RAMPUP state with sustained successes.
    //
    assertEquals(ThrottleState.THROTTLE_RAMPUP, throttleSM.getState(ThrottleType.UPLOAD));
    
    // Validate that throttling engine cannot ramp down.
    //
    assertFalse(throttleSM.rampDown(ThrottleType.UPLOAD));
    
    // Validate that throttling engine can ramp up.
    //
    assertTrue(throttleSM.rampUp(ThrottleType.UPLOAD));
    
    // Validate that only one ramp up is allowed within two throttle intervals.
    //
    assertFalse(throttleSM.rampUp(ThrottleType.UPLOAD));
  }
  
  /**
   * Test transitions through the STABLE, RAMPDOWN, RAMPUP, and back to the STABLE state.
   *
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTransitionBackToStableState() throws InterruptedException{
    // Fire an equal number of success and failure events over a one second period.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(LATENCY);
      if (0 == i % 2) {
        throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
      } else {
        throttleSM.updateTransmissionFailure(ThrottleType.UPLOAD, 1, LATENCY);
      }
    }
    
    // Validate the state machine is still in the RAMPDOWN state.
    //
    assertEquals(ThrottleState.THROTTLE_RAMPDOWN, throttleSM.getState(ThrottleType.UPLOAD));
    
    // State machine should return to RAMPUP state after sustained transmission successes.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(LATENCY);
      throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
    }
    
    // Validate that the state machine is in the RAMPUP state with sustained successes.
    //
    assertEquals(ThrottleState.THROTTLE_RAMPUP, throttleSM.getState(ThrottleType.UPLOAD));
    
    // Stop throttling and validate that state machine is back in the stable state.
    //
    throttleSM.stopThrottling(ThrottleType.UPLOAD);
    
    // Validate that state machine is in the stable state.
    //
    assertEquals( ThrottleState.THROTTLE_NONE, throttleSM.getState(ThrottleType.UPLOAD));
    
    // Validate that throttling engine cannot ramp down.
    //
    assertFalse(throttleSM.rampDown(ThrottleType.UPLOAD));
    
    // Validate that only one ramp up is allowed within two throttle intervals.
    //
    assertFalse(throttleSM.rampUp(ThrottleType.UPLOAD));
  }
  
  /**
   * Test transitions through the STABLE, RAMPDOWN, RAMPUP, and back to the STABLE state.
   *
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testTransitionFromRampupToRampdownState() throws InterruptedException{
    // Fire an equal number of success and failure events over a one second period.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(LATENCY);
      if (0 == i % 2) {
        throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
      } else {
        throttleSM.updateTransmissionFailure(ThrottleType.UPLOAD, 1, LATENCY);
      }
    }
    
    // Validate the state machine is still in the RAMPDOWN state.
    //
    assertEquals(ThrottleState.THROTTLE_RAMPDOWN, throttleSM.getState(ThrottleType.UPLOAD));
    
    // State machine should return to RAMPUP state after sustained transmission successes.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(LATENCY);
      throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
    }
    
    // Inject a transmission failure.
    //
    Thread.sleep(LATENCY);
    throttleSM.updateTransmissionFailure(ThrottleType.UPLOAD, 1, LATENCY);
    
    // Validate that the state machine is in the RAMPDOWN state.
    //
    assertEquals( ThrottleState.THROTTLE_RAMPDOWN, throttleSM.getState(ThrottleType.UPLOAD));
    
    // Validate that throttling engine cannot be ramped up.
    //
    assertFalse(throttleSM.rampUp(ThrottleType.UPLOAD));
    
    // Validate that throttling engine can ramp down.
    //
    assertTrue(throttleSM.rampDown(ThrottleType.UPLOAD));
    
    // Validate that only one ramp down is allowed within two throttle intervals.
    //
    assertFalse(throttleSM.rampDown(ThrottleType.UPLOAD));
  }
  
  /**
   * Negative tests verifying that stop throttling events are not expected in the
   * RAMPDOWN state.
   *
   * @throws InterruptedException if sleep is interrupted.
   */
  @Test
  public void testStopThrottlingInStableState() throws InterruptedException {
    // Fire success events every LATENCY milliseconds over a period of one second.
    //
    for (int i = 0; i < TIMER_PERIOD / LATENCY; i++) {
      Thread.sleep(LATENCY);
      throttleSM.updateTransmissionSuccess(ThrottleType.UPLOAD, 1, LATENCY);
    }
    
    // Validate the state machine is still in the equilibrium state.
    //
    assertEquals( ThrottleState.THROTTLE_NONE, throttleSM.getState(ThrottleType.UPLOAD));
    
    // Neither ramp up or ramp down is expected by the throttling engine.
    //
    assertFalse(throttleSM.rampDown(ThrottleType.UPLOAD));
    assertFalse(throttleSM.rampUp(ThrottleType.UPLOAD));
    
    Thread.sleep(LATENCY);
    throttleSM.updateTransmissionFailure(ThrottleType.UPLOAD, 1, LATENCY);
    
    try {
      // Try to stop throttling in the DOWNLOAD state.
      //
      throttleSM.stopThrottling(ThrottleType.UPLOAD);
      throw new Exception("Should not be possible to stop throttling in STABLE state.");
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
      
      // Validate download state.
      //
      assertEquals(ThrottleState.THROTTLE_RAMPDOWN, throttleSM.getState(ThrottleType.UPLOAD));
    }
  }
}