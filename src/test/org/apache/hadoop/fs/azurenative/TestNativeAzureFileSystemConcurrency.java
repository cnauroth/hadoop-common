package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.StringUtils;

import junit.framework.*;

public class TestNativeAzureFileSystemConcurrency extends TestCase {
  private AzureBlobStorageTestAccount testAccount;
  private FileSystem fs;

  @Override
  protected void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createMock();
    fs = testAccount.getFileSystem();
  }

  @Override
  protected void tearDown() throws Exception {
    testAccount.cleanup();
    fs = null;
  }

  private static String toString(FileStatus[] list) {
    String[] asStrings = new String[list.length];
    for (int i = 0; i < list.length; i++) {
      asStrings[i] = list[i].getPath().toString();
    }
    return StringUtils.join(",", asStrings);
  }

  /**
   * Test to make sure that we don't expose the temporary upload
   * folder when listing at the root.
   */
  public void testNoTempBlobsVisible() throws Exception {
    Path filePath = new Path("/inProgress");
    FSDataOutputStream outputStream = fs.create(filePath);
    // Make sure I can't see the temporary blob if I ask for a listing
    FileStatus[] listOfRoot = fs.listStatus(new Path("/"));
    assertEquals(
        "Expected no files listed, instead got: " + toString(listOfRoot),
        0, listOfRoot.length);
    outputStream.close();
  }
}
