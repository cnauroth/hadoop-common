package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.metrics2.*;
import static org.apache.hadoop.test.MetricsAsserts.*;

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
  
  public void testWebRequestsOnMkdirList() throws Exception {
    // The number of requests should start at 1
    // from when we check the existence of the container
    assertCounter("asv_web_requests", 1L, getMyMetrics());
    
    // Create a directory
    assertTrue(fs.mkdirs(new Path("a")));
    assertCounter("asv_web_requests", 2L, getMyMetrics());

    // List the root contents
    assertEquals(1, fs.listStatus(new Path("/")).length);    
    assertCounter("asv_web_requests", 3L, getMyMetrics());
  }

  private MetricsRecordBuilder getMyMetrics() {
    return getMetrics(((NativeAzureFileSystem)fs).getInstrumentation());
  }
}
