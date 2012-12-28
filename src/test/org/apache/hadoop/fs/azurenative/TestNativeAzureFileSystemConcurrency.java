package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.fs.*;

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
}
