package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeNotNull;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.fs.azurenative.BandwidthThrottle.ThrottleType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.junit.*;

public class TestAzureFileSystemThrottlingFeedback
implements ThrottleSendRequestCallback, BandwidthThrottleFeedback {

  // Class constants.
  //
  static final int  DOWNLOAD_BLOCK_SIZE = 8 * 1024 * 1024;
  static final int  UPLOAD_BLOCK_SIZE   = 4 * 1024 * 1024;
  static final int  BLOB_SIZE           = 32 * 1024 * 1024;

  // Member variables.
  //
  AzureBlobStorageTestAccount testAccount;

  protected int[]sumTxFailure = new int[2];
  protected int[]sumTxSuccess = new int[2];
  protected long[]sumPayload  = new long[2];

  // Overridden TestCase methods.
  //
  @Before
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createThrottledStore(
        UPLOAD_BLOCK_SIZE, DOWNLOAD_BLOCK_SIZE,
        (ThrottleSendRequestCallback) this,
        (BandwidthThrottleFeedback) this);
    assumeNotNull(testAccount);
  }

  @After
  public void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
    }
  }

  /**
   * Implements the ThrottleSendRequest callback interface to determine whether
   * send requests should be delayed.
   *
   * @param kindOfThrottle
   *          - download or upload throttle.
   * @param payloadSize
   *          size of payload on throttle.
   */
  @Override
  public void throttleSendRequest(ThrottleType kindOfThrottle, long payloadSize) {
    sumPayload[kindOfThrottle.getValue()] += payloadSize;
  }

  /**
   * Azure throttling feedback method: Update transmission success counter by
   * delta.
   *
   * @param kindOfThrottle
   *          - download or upload throttle
   * @param delta
   *          - increment the transmission success by this amount
   * @return accumulated transmission successes this far.
   */
  @Override
  public int updateTransmissionSuccess(ThrottleType kindOfThrottle, int delta) {
    sumTxSuccess[kindOfThrottle.getValue()] += delta;
    return sumTxSuccess[kindOfThrottle.getValue()];
  }

  /**
   * Azure throttling feedback method: Update transmission failure by delta.
   *
   * @param kindOfThrottle
   *          - download or upload throttle
   * @param delta
   *          - increment the transmission failure by this amount
   * @return accumulated transmission failures this far.
   */
  @Override
  public int updateTransmissionFailure(ThrottleType kindOfThrottle, int delta) {
    sumTxFailure[kindOfThrottle.getValue()] += delta;
    return sumTxFailure[kindOfThrottle.getValue()];
  }

  @Test
  public void testThrottleFeedback() throws Exception {

    // Open a blob output stream and write BLOB_SIZE bytes to it.
    //
    OutputStream outputStream = testAccount.getStore().storefile(
        "ASV_throttle.txt",
        new PermissionStatus("", "", FsPermission.getDefault()));
    outputStream.write(new byte[BLOB_SIZE]);
    outputStream.flush();
    outputStream.close();

    // Read back the blob and close the stream.
    //
    InputStream inputStream = testAccount.getStore().retrieve("ASV_throttle.txt", 0);
    int count = 0;
    while (inputStream.read() >= 0) {
      count++;
    }
    inputStream.close();

    // Validate that the whole blob was read back.
    //
    assertEquals(count, BLOB_SIZE);

    // Validate throttling feed back interfaces.
    //
    assertTrue(sumPayload[ThrottleType.UPLOAD.getValue()] >= BLOB_SIZE);
    assertEquals(BLOB_SIZE, sumPayload[ThrottleType.DOWNLOAD.getValue()]);

    assertTrue(sumTxSuccess[ThrottleType.UPLOAD.getValue()] > 0);
    assertTrue(sumTxSuccess[ThrottleType.DOWNLOAD.getValue()] > 0);

    assertEquals(0, sumTxFailure[ThrottleType.UPLOAD.getValue()]);
    assertEquals(0, sumTxFailure[ThrottleType.DOWNLOAD.getValue()]);
  }

  @Test
  public void testPartialBlock() throws Exception {

    final int BUFFER_SIZE = 1024;

    // Reset counters.
    //
    for (ThrottleType kindOfThrottle : ThrottleType.values()) {
      sumPayload[kindOfThrottle.getValue()] = 0;
      sumTxSuccess[kindOfThrottle.getValue()] = 0;
      sumTxFailure[kindOfThrottle.getValue()] = 0;
    }

    // Open a blob output stream and write and 8 character string.
    //
    DataOutputStream outputStream = testAccount.getStore().storefile(
        "ASV_String.txt",
        new PermissionStatus("", "", FsPermission.getDefault()));

    outputStream.writeChars("12345678");
    outputStream.flush();
    outputStream.close();

    DataInputStream inputStream =
        testAccount.getStore().retrieve("ASV_String.txt", 0);
    int count = 0;
    int c = 0;
    byte buffer[] = new byte[BUFFER_SIZE];
    
    while (c >= 0) {
      c = inputStream.read(buffer, 0, BUFFER_SIZE);
      if (c >= BUFFER_SIZE) {
        throw new IOException("Exceeded buffer size");
      } else if (c >= 0) {
        // Counting the number of characters.
        //
        count += c;
      }
    }
    
    // Close the stream.
    //
    inputStream.close();

    // Validate that 8 bytes were read.
    //
    assertEquals(16, count);

    // Validate throttling feed back interfaces.
    //
    assertTrue(sumPayload[ThrottleType.UPLOAD.getValue()] >= UPLOAD_BLOCK_SIZE);
    assertEquals(count, sumPayload[ThrottleType.DOWNLOAD.getValue()]);

    assertTrue(sumTxSuccess[ThrottleType.UPLOAD.getValue()] > 0);
    assertTrue(sumTxSuccess[ThrottleType.DOWNLOAD.getValue()] > 0);

    assertEquals(0, sumTxFailure[ThrottleType.UPLOAD.getValue()]);
    assertEquals(0, sumTxFailure[ThrottleType.DOWNLOAD.getValue()]);
  }
}
