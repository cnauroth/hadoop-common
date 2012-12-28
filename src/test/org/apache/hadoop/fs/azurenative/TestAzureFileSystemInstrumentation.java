package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.metrics2.*;
import static org.apache.hadoop.test.MetricsAsserts.*;
import static org.apache.hadoop.fs.azurenative.AzureMetricsTestUtil.*;

import org.hamcrest.*;
import org.mockito.ArgumentCaptor;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import junit.framework.TestCase;

public class TestAzureFileSystemInstrumentation extends TestCase {
  private FileSystem fs;
  private AzureBlobStorageTestAccount testAccount;

  @Override
  protected void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }
  }

  @Override
  protected void tearDown() throws Exception {
    if (testAccount != null) {
      testAccount.cleanup();
      testAccount = null;
      fs = null;
    }
  }

  @Override
  protected void runTest() throws Throwable {
    if (testAccount != null) {
      super.runTest();
    }
  }

  public void testMetricTags() throws Exception {
    String accountName =
        testAccount.getRealAccount().getBlobEndpoint()
        .getAuthority().split("\\.")[0];
    String containerName =
        testAccount.getRealContainer().getName();
    MetricsRecordBuilder myMetrics = getMyMetrics();
    verify(myMetrics).add(argThat(
        new TagMatcher("accountName", accountName)
        ));
    verify(myMetrics).add(argThat(
        new TagMatcher("containerName", containerName)
        ));
    verify(myMetrics).add(argThat(
        new TagMatcher("context", "azureFileSystem")
        ));
    verify(myMetrics).add(argThat(
        new TagExistsMatcher("asvFileSystemId")
        ));
  }

  public void testMetricsOnMkdirList() throws Exception {
    long base = getBaseWebResponses();
    
    // Create a directory
    assertTrue(fs.mkdirs(new Path("a")));
    // At the time of writing, it takes 1 request to create the actual directory,
    // plus 2 requests per level to check that there's no blob with that name
    // So for the path above (/user/<name>/a), it takes 2 requests each to check
    // there's no blob called /user, no blob called /user/<name> and no blob
    // called /user/<name>/a, and then 1 reqest for the creation, totalling 7.
    base = assertWebResponsesInRange(base, 1, 7);
    assertEquals(1, getLongCounterValue(getInstrumentation(), ASV_DIRECTORIES_CREATED));

    // List the root contents
    assertEquals(1, fs.listStatus(new Path("/")).length);    
    base = assertWebResponsesEquals(base, 1);
  }

  private BandwidthGaugeUpdater getBandwidthGaugeUpdater() {
    NativeAzureFileSystem azureFs = (NativeAzureFileSystem)fs;
    AzureNativeFileSystemStore azureStore = azureFs.getStore();
    return azureStore.getBandwidthGaugeUpdater();
  }

  public void testMetricsOnFileCreateRead() throws Exception {
    long base = getBaseWebResponses();
    
    assertEquals(0, getCurrentBytesWritten(getInstrumentation()));

    Path filePath = new Path("/metricsTest_webResponses");
    final int FILE_SIZE = 1000;

    // Suppress auto-update of bandwidth metrics so we get
    // to update them exactly when we want to.
    getBandwidthGaugeUpdater().suppressAutoUpdate();

    // Create a file
    Date start = new Date();
    OutputStream outputStream = fs.create(filePath);
    outputStream.write(new byte[FILE_SIZE]);
    outputStream.close();
    long uploadDurationMs = new Date().getTime() - start.getTime();
    
    // The exact number of requests/responses that happen to create a file
    // can vary  - at the time of writing this code it takes 6
    // requests/responses for the 1000 byte file (33 for 100 MB), but that
    // can very easily change in the future. Just assert that we do roughly
    // more than 2 but less than 15.
    logOpResponseCount("Creating a 1K file", base);
    base = assertWebResponsesInRange(base, 2, 15);
    getBandwidthGaugeUpdater().triggerUpdate(true);
    long bytesWritten = getCurrentBytesWritten(getInstrumentation());
    assertTrue("The bytes written in the last second " + bytesWritten +
        " is pretty far from the expected range of around " + FILE_SIZE +
        " bytes plus a little overhead.",
        bytesWritten > (FILE_SIZE / 2) && bytesWritten < (FILE_SIZE * 2));
    long totalBytesWritten = getCurrentTotalBytesWritten(getInstrumentation());
    assertTrue("The total bytes written  " + totalBytesWritten +
        " is pretty far from the expected range of around " + FILE_SIZE +
        " bytes plus a little overhead.",
        totalBytesWritten >= FILE_SIZE && totalBytesWritten < (FILE_SIZE * 2));
    long uploadRate = getLongGaugeValue(getInstrumentation(), ASV_UPLOAD_RATE);
    System.out.println("Upload rate: " + uploadRate + " bytes/second.");
    long expectedRate = (FILE_SIZE * 1000L) / uploadDurationMs;
    assertTrue("The upload rate " + uploadRate +
        " is below the expected range of around " + expectedRate +
        " bytes/second that the unit test observed. This should never be" +
        " the case since the test underestimates the rate by looking at " +
        " end-to-end time instead of just block upload time.",
        uploadRate >= expectedRate);
    long uploadLatency = getLongGaugeValue(getInstrumentation(),
        ASV_UPLOAD_LATENCY);
    System.out.println("Upload latency: " + uploadLatency);
    long expectedLatency = uploadDurationMs; // We're uploading less than a block.
    assertTrue("The upload latency " + uploadLatency +
        " should be greater than zero now that I've just uploaded a file.",
        uploadLatency > 0);
    assertTrue("The upload latency " + uploadLatency +
        " is more than the expected range of around " + expectedLatency +
        " milliseconds that the unit test observed. This should never be" +
        " the case since the test overestimates the latency by looking at " +
        " end-to-end time instead of just block upload time.",
        uploadLatency <= expectedLatency);
    
    // Read the file
    start = new Date();
    InputStream inputStream = fs.open(filePath);
    int count = 0;
    while (inputStream.read() >= 0) {
      count++;
    }
    inputStream.close();
    long downloadDurationMs = new Date().getTime() - start.getTime();
    assertEquals(FILE_SIZE, count);

    // Again, exact number varies. At the time of writing this code
    // it takes 4 request/responses, so just assert a rough range between
    // 1 and 10.
    logOpResponseCount("Reading a 1K file", base);
    base = assertWebResponsesInRange(base, 1, 10);
    getBandwidthGaugeUpdater().triggerUpdate(false);
    long totalBytesRead = getCurrentTotalBytesRead(getInstrumentation());
    assertEquals(FILE_SIZE, totalBytesRead);
    long bytesRead = getCurrentBytesRead(getInstrumentation());
    assertTrue("The bytes read in the last second " + bytesRead +
        " is pretty far from the expected range of around " + FILE_SIZE +
        " bytes plus a little overhead.",
        bytesRead > (FILE_SIZE / 2) && bytesRead < (FILE_SIZE * 2));
    long downloadRate = getLongGaugeValue(getInstrumentation(), ASV_DOWNLOAD_RATE);
    System.out.println("Download rate: " + downloadRate + " bytes/second.");
    expectedRate = (FILE_SIZE * 1000L) / downloadDurationMs;
    assertTrue("The download rate " + downloadRate +
        " is below the expected range of around " + expectedRate +
        " bytes/second that the unit test observed. This should never be" +
        " the case since the test underestimates the rate by looking at " +
        " end-to-end time instead of just block download time.",
        downloadRate >= expectedRate);
    long downloadLatency = getLongGaugeValue(getInstrumentation(),
        ASV_DOWNLOAD_LATENCY);
    System.out.println("Download latency: " + downloadLatency);
    expectedLatency = downloadDurationMs; // We're downloading less than a block.
    assertTrue("The download latency " + downloadLatency +
        " should be greater than zero now that I've just downloaded a file.",
        uploadLatency > 0);
    assertTrue("The download latency " + downloadLatency +
        " is more than the expected range of around " + expectedLatency +
        " milliseconds that the unit test observed. This should never be" +
        " the case since the test overestimates the latency by looking at " +
        " end-to-end time instead of just block download time.",
        downloadLatency <= expectedLatency);
  }

  public void testMetricsOnFileRename() throws Exception {
    long base = getBaseWebResponses();

    Path originalPath = new Path("/metricsTest_RenameStart");
    Path destinationPath = new Path("/metricsTest_RenameFinal");

    // Create an empty file
    assertEquals(0, getLongCounterValue(getInstrumentation(), ASV_FILES_CREATED));
    assertTrue(fs.createNewFile(originalPath));
    logOpResponseCount("Creating an empty file", base);
    base = assertWebResponsesInRange(base, 2, 20);
    assertEquals(1, getLongCounterValue(getInstrumentation(), ASV_FILES_CREATED));

    // Rename the file
    assertTrue(fs.rename(originalPath, destinationPath));
    // Varies: at the time of writing this code it takes 7 requests/responses.
    logOpResponseCount("Renaming a file", base);
    base = assertWebResponsesInRange(base, 2, 15);
  }

  public void testMetricsOnFileExistsDelete() throws Exception {
    long base = getBaseWebResponses();

    Path filePath = new Path("/metricsTest_delete");

    // Check existence
    assertFalse(fs.exists(filePath));
    // At the time of writing this code it takes 2 requests/responses to
    // check existence, which seems excessive. Check for range 1-4 for now.
    logOpResponseCount("Checking file existence for non-existent file", base);
    base = assertWebResponsesInRange(base, 1, 2);

    // Create an empty file
    assertTrue(fs.createNewFile(filePath));
    base = getCurrentWebResponses();

    // Check existence again
    assertTrue(fs.exists(filePath));
    logOpResponseCount("Checking file existence for existent file", base);
    base = assertWebResponsesInRange(base, 1, 2);

    // Delete the file
    assertEquals(0, getLongCounterValue(getInstrumentation(), ASV_FILES_DELETED));
    assertTrue(fs.delete(filePath, false));
    // At the time of writing this code it takes 4 requests/responses to
    // delete, which seems excessive. Check for range 1-4 for now.
    logOpResponseCount("Deleting a file", base);
    base = assertWebResponsesInRange(base, 1, 4);
    assertEquals(1, getLongCounterValue(getInstrumentation(), ASV_FILES_DELETED));
  }

  public void testMetricsOnDirRename() throws Exception {
    long base = getBaseWebResponses();
    
    Path originalDirName = new Path("/metricsTestDirectory_RenameStart");
    Path innerFileName = new Path(originalDirName, "innerFile");
    Path destDirName = new Path("/metricsTestDirectory_RenameFinal");
    
    // Create an empty directory
    assertTrue(fs.mkdirs(originalDirName));
    base = getCurrentWebResponses();
    
    // Create an inner file
    assertTrue(fs.createNewFile(innerFileName));
    base = getCurrentWebResponses();
    
    // Rename the directory
    assertTrue(fs.rename(originalDirName, destDirName));
    // At the time of writing this code it takes 11 requests/responses
    // to rename the directory with one file. Check for range 1-20 for now.
    logOpResponseCount("Renaming a directory", base);
    base = assertWebResponsesInRange(base, 1, 20);
  }

  private void logOpResponseCount(String opName, long base) {
    System.out.println(opName + " took " + (getCurrentWebResponses() - base) +
        " web responses to complete.");
  }

  /**
   * Gets (and asserts) the value of the asv_web_responses counter just
   * after the creation of the file system object.
   */
  private long getBaseWebResponses() {
    // The number of requests should start at 1
    // from when we check the existence of the container
    return assertWebResponsesEquals(0, 1);    
  }

  /**
   * Gets the current value of the asv_web_responses counter.
   */
  private long getCurrentWebResponses() {
    ArgumentCaptor<Long> captor = ArgumentCaptor.forClass(Long.class);
    verify(getMyMetrics()).addCounter(eq(ASV_WEB_RESPONSES), anyString(),
        captor.capture());
    return captor.getValue();
  }

  /**
   * Checks that the asv_web_responses counter is at the given value.
   * @param base The base value (before the operation of interest).
   * @param expected The expected value for the operation of interest.
   * @return The new base value now.
   */
  private long assertWebResponsesEquals(long base, long expected) {
    assertCounter(ASV_WEB_RESPONSES, base + expected, getMyMetrics());
    return base + expected;
  }

  /**
   * Checks that the asv_web_responses counter is in the given range.
   * @param base The base value (before the operation of interest).
   * @param inclusiveLowerLimit The lower limit for what it should increase by.
   * @param inclusiveUpperLimit The upper limit for what it should increase by.
   * @return The new base value now.
   */
  private long assertWebResponsesInRange(long base,
      long inclusiveLowerLimit,
      long inclusiveUpperLimit) {
    InRange matcher = new InRange(base + inclusiveLowerLimit,
        base + inclusiveUpperLimit); 
    verify(getMyMetrics()).addCounter(eq(ASV_WEB_RESPONSES), anyString(),
        longThat(matcher));
    return matcher.obtained;
  }

  /**
   * Gets the metrics for the file system object.
   * @return The metrics record.
   */
  private MetricsRecordBuilder getMyMetrics() {
    return getMetrics(getInstrumentation());
  }

  private AzureFileSystemInstrumentation getInstrumentation() {
    return ((NativeAzureFileSystem)fs).getInstrumentation();
  }

  /**
   * A matcher class for asserting that we got a tag with a given
   * value.
   */
  private static class TagMatcher extends TagExistsMatcher {
    private final String tagValue;
    
    public TagMatcher(String tagName, String tagValue) {
      super(tagName);
      this.tagValue = tagValue;
    }

    @Override
    public boolean matches(MetricsTag toMatch) {
      return toMatch.value().equals(tagValue);
    }

    @Override
    public void describeTo(Description desc) {
      super.describeTo(desc);
      desc.appendText(" with value " + tagValue);
    }
  }

  /**
   * A matcher class for asserting that we got a tag with any value.
   */
  private static class TagExistsMatcher extends BaseMatcher<MetricsTag> {
    private final String tagName;
    
    public TagExistsMatcher(String tagName) {
      this.tagName = tagName;
    }

    @Override
    public boolean matches(Object toMatch) {
      MetricsTag asTag = (MetricsTag)toMatch;
      return asTag.name().equals(tagName) && matches(asTag);
    }
    
    protected boolean matches(MetricsTag toMatch) {
      return true;
    }

    @Override
    public void describeTo(Description desc) {
      desc.appendText("Has tag " + tagName);
    }
  }

  /**
   * A matcher class for asserting that a long value is in a
   * given range.
   */
  private static class InRange extends BaseMatcher<Long> {
    private final long inclusiveLowerLimit;
    private final long inclusiveUpperLimit;
    private long obtained;

    public InRange(long inclusiveLowerLimit, long inclusiveUpperLimit) {
      this.inclusiveLowerLimit = inclusiveLowerLimit;
      this.inclusiveUpperLimit = inclusiveUpperLimit;
    }

    @Override
    public boolean matches(Object number) {
      obtained = (Long)number;
      return obtained >= inclusiveLowerLimit &&
          obtained <= inclusiveUpperLimit;
    }

    @Override
    public void describeTo(Description description) {
      description.appendText("Between " + inclusiveLowerLimit +
          " and " + inclusiveUpperLimit + " inclusively");
    }
  }
}
