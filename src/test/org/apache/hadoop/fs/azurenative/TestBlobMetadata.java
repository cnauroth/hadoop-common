package org.apache.hadoop.fs.azurenative;

import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.security.UserGroupInformation;

import junit.framework.*;

/**
 * Tests that we put the correct metadata on blobs created through ASV. 
 */
public class TestBlobMetadata extends TestCase {
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

  public void testPermissionMetadata() throws Exception {
    FsPermission justMe = new FsPermission(
        FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    Path selfishFile = new Path("/noOneElse");
    fs.create(selfishFile, justMe,
        true, 4096, fs.getDefaultReplication(), fs.getDefaultBlockSize(), null).close();
    HashMap<String, String> metadata =
        backingStore.getMetadata(AzureBlobStorageTestAccount.toMockUri(selfishFile));
    assertNotNull(metadata);
    String storedPermission = metadata.get("asv_permission");
    String expectedOwner = UserGroupInformation.getCurrentUser().getShortUserName();
    String expectedPermission = String.format(
        "{\"owner\":\"%s\",\"group\":\"\",\"permissions\":\"rw-------\"}",
        expectedOwner);
    assertEquals(expectedPermission, storedPermission);
    FileStatus retrievedStatus = fs.getFileStatus(selfishFile);
    assertNotNull(retrievedStatus);
    assertEquals(justMe, retrievedStatus.getPermission());
    assertEquals(expectedOwner, retrievedStatus.getOwner());
    assertEquals("", retrievedStatus.getGroup());
  }
}
