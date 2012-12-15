package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.fs.*;

import junit.framework.*;

/**
 * Tests that ASV handles things gracefully when users add blobs to
 * the Azure Storage container from outside ASV's control.
 */
public class TestOutOfBandAzureBlobOperations extends TestCase {
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

  private static String toMockUri(String path) {
    return String.format("http://%s.blob.core.windows.net/%s/%s",
        AzureBlobStorageTestAccount.MOCK_ACCOUNT_NAME,
        AzureBlobStorageTestAccount.MOCK_CONTAINER_NAME,
        path);
  }

  private void createEmptyBlobOutOfBand(String path) {
    backingStore.setContent(
        toMockUri(path),
        new byte[] { 1, 2 },
        new HashMap<String, String>());
  }

  public void testImplicitFolderListed() throws Exception {
    createEmptyBlobOutOfBand("ab/b");

    // List the blob itself.
    FileStatus[] obtained = fs.listStatus(new Path("/ab/b"));
    assertNotNull(obtained);
    assertEquals(1, obtained.length);
    assertFalse(obtained[0].isDir());
    assertEquals("/ab/b", obtained[0].getPath().toUri().getPath());

    // List the directory
    obtained = fs.listStatus(new Path("/ab"));
    assertNotNull(obtained);
    assertEquals(1, obtained.length);
    assertFalse(obtained[0].isDir());
    assertEquals("/ab/b", obtained[0].getPath().toUri().getPath());

    // Get the directory's file status
    FileStatus dirStatus = fs.getFileStatus(new Path("/ab"));
    assertNotNull(dirStatus);
    assertTrue(dirStatus.isDir());
    assertEquals("/ab", dirStatus.getPath().toUri().getPath());
  }

  public void testImplicitFolderDeleted() throws Exception {
    createEmptyBlobOutOfBand("ab/b");
    assertTrue(fs.exists(new Path("/ab")));
    assertTrue(fs.delete(new Path("/ab"), true));
    assertFalse(fs.exists(new Path("/ab")));
  }
}
