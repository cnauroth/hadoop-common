package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.metrics2.*;
import static org.apache.hadoop.test.MetricsAsserts.*;

import junit.framework.TestCase;

public class TestAzureFileSystemInstrumentation extends TestCase {
  private static String prefixUri;
  private FileSystem fs;
  private AzureBlobStorageTestAccount testAccount;

  @Override
  protected void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.create();
    if (testAccount != null) {
      fs = testAccount.getFileSystem();
    }

    prefixUri = testAccount.getUriPrefix();
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
    assertTrue(fs.mkdirs(new Path(prefixUri + "a")));
    assertEquals(1, fs.listStatus(new Path(prefixUri + "/")).length);
    MetricsRecordBuilder metrics = getMyMetrics();
    assertCounter("asv_web_requests", 2L, metrics);
  }

  private MetricsRecordBuilder getMyMetrics() {
    return getMetrics(((NativeAzureFileSystem)fs).getInstrumentation());
  }
}
