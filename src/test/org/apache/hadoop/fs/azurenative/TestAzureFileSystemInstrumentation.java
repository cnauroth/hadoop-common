package org.apache.hadoop.fs.azurenative;

import java.io.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.metrics2.*;
import static org.apache.hadoop.test.MetricsAsserts.*;

import org.hamcrest.*;
import org.mockito.ArgumentCaptor;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import junit.framework.TestCase;

public class TestAzureFileSystemInstrumentation extends TestCase {
  private static final String ASV_WEB_RESPONSES = "asv_web_responses";
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

  public void testWebResponsesOnMkdirList() throws Exception {
    long base = getBaseWebResponses();
    
    // Create a directory
    assertTrue(fs.mkdirs(new Path("a")));
    // At the time of writing, it takes 1 request to create the actual directory,
    // plus 2 requests per level to check that there's no blob with that name
    // So for the path above (/user/<name>/a), it takes 2 requests each to check
    // there's no blob called /user, no blob called /user/<name> and no blob
    // called /user/<name>/a, and then 1 reqest for the creation, totalling 7.
    base = assertWebResponsesInRange(base, 1, 7);

    // List the root contents
    assertEquals(1, fs.listStatus(new Path("/")).length);    
    base = assertWebResponsesEquals(base, 1);
  }

  public void testWebResponsesOnFileCreateRead() throws Exception {
    long base = getBaseWebResponses();

    Path filePath = new Path("/metricsTest_webResponses");
    final int FILE_SIZE = 1000;

    // Create a file
    OutputStream outputStream = fs.create(filePath);
    outputStream.write(new byte[FILE_SIZE]);
    outputStream.close();
    
    // The exact number of requests/responses that happen to create a file
    // can vary  - at the time of writing this code it takes 7
    // requests/responses for the 1000 byte file (33 for 100 MB), but that
    // can very easily change in the future. Just assert that we do roughly
    // more than 2 but less than 15.
    logOpResponseCount("Creating a 1K file", base);
    base = assertWebResponsesInRange(base, 2, 15);
    
    // Read the file
    InputStream inputStream = fs.open(filePath);
    int count = 0;
    while (inputStream.read() >= 0) {
      count++;
    }
    inputStream.close();
    assertEquals(FILE_SIZE, count);
    
    // Again, exact number varies. At the time of writing this code
    // it takes 4 request/responses, so just assert a rough range between
    // 1 and 10.
    logOpResponseCount("Reading a 1K file", base);
    base = assertWebResponsesInRange(base, 1, 10);
  }

  public void testWebResponsesOnFileRenameDelete() throws Exception {
    long base = getBaseWebResponses();
    
    Path originalPath = new Path("/metricsTest_RenameStart");
    Path destinationPath = new Path("/metricsTest_RenameFinal");
    
    // Create an empty file
    assertTrue(fs.createNewFile(originalPath));
    logOpResponseCount("Creating an empty file", base);
    base = assertWebResponsesInRange(base, 2, 20);
    
    // Rename the file
    assertTrue(fs.rename(originalPath, destinationPath));
    // Varies: at the time of writing this code it takes 7 requests/responses.
    logOpResponseCount("Renaming a file", base);
    base = assertWebResponsesInRange(base, 2, 15);
  }

  public void testWebResponsesOnFileExistsDelete() throws Exception {
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
    assertTrue(fs.delete(filePath, false));
    // At the time of writing this code it takes 4 requests/responses to
    // delete, which seems excessive. Check for range 1-4 for now.
    logOpResponseCount("Deleting a file", base);
    base = assertWebResponsesInRange(base, 1, 4);
  }

  public void testWebResponsesOnDirRename() throws Exception {
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
    return getMetrics(((NativeAzureFileSystem)fs).getInstrumentation());
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
