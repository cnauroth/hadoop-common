package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.fs.*;

import junit.framework.*;

public class TestOutOfBandAzureBlobOperations extends TestCase {
  private static String toMockUri(String path) {
    return String.format("http://%s.blob.core.windows.net/%s/%s",
        AzureBlobStorageTestAccount.MOCK_ACCOUNT_NAME,
        AzureBlobStorageTestAccount.MOCK_CONTAINER_NAME,
        path);
  }

  public void testImplicitFolderListed() throws Exception {
    AzureBlobStorageTestAccount testAccount =
        AzureBlobStorageTestAccount.createMock();
    testAccount.getMockStorage().getBackingStore().setContent(
        toMockUri("a/b"),
        new byte[] { 1, 2 },
        new HashMap<String, String>());

    // List the blob itself.
    FileStatus[] obtained =
        testAccount.getFileSystem().listStatus(new Path("/a/b"));
    assertNotNull(obtained);
    assertEquals(1, obtained.length);
    assertFalse(obtained[0].isDir());
    assertEquals("/a/b", obtained[0].getPath().toUri().getPath());

    // List the directory
    obtained = testAccount.getFileSystem().listStatus(new Path("/a"));
    assertNotNull(obtained);
    assertEquals(1, obtained.length);
    assertFalse(obtained[0].isDir());
    assertEquals("/a/b", obtained[0].getPath().toUri().getPath());

    // Get the directory's file status
    FileStatus dirStatus =
        testAccount.getFileSystem().getFileStatus(new Path("/a"));
    assertNotNull(dirStatus);
    assertTrue(dirStatus.isDir());
    assertEquals("/a", dirStatus.getPath().toUri().getPath());

    testAccount.cleanup();
  }
}
