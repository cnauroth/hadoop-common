package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.StringUtils;

import junit.framework.*;

public class TestNativeAzureFileSystemConcurrency extends TestCase {
  private AzureBlobStorageTestAccount testAccount;
  private FileSystem fs;
  private InMemoryBlockBlobStore backingStore;

  @Override
  protected void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createMock();
    fs = testAccount.getFileSystem();
    backingStore = testAccount.getMockStorage().getBackingStore();
  }

  @Override
  protected void tearDown() throws Exception {
    testAccount.cleanup();
    fs = null;
    backingStore = null;
  }

  public void testLinkBlobs() throws Exception {
    Path filePath = new Path("/inProgress");
    FSDataOutputStream outputStream = fs.create(filePath);
    // Since the stream is still open, we should see an empty link
    // blob in the backing store linking to the temporary file.
    HashMap<String, String> metadata =
        backingStore.getMetadata(
            AzureBlobStorageTestAccount.toMockUri(filePath));
    assertNotNull(metadata);
    String linkValue = metadata.get(AzureNativeFileSystemStore.
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY);
    assertNotNull(linkValue);
    assertTrue(backingStore.exists(
        AzureBlobStorageTestAccount.toMockUri(linkValue)));
    // Also, ASV should say the file exists now even before we close the
    // stream.
    assertTrue(fs.exists(filePath));
    outputStream.close();
    // Now there should be no link metadata on the final file.
    metadata =
        backingStore.getMetadata(
            AzureBlobStorageTestAccount.toMockUri(filePath));
    assertNull(metadata.get(AzureNativeFileSystemStore.
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY));
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
        "Expected one file listed, instead got: " + toString(listOfRoot),
        1, listOfRoot.length);
    assertEquals(fs.makeQualified(filePath), listOfRoot[0].getPath());
    outputStream.close();
  }
}
