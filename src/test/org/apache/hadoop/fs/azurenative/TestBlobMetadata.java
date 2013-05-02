package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;

import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.*;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.*;

/**
 * Tests that we put the correct metadata on blobs created through ASV. 
 */
public class TestBlobMetadata {
  private AzureBlobStorageTestAccount testAccount;
  private FileSystem fs;
  private InMemoryBlockBlobStore backingStore;

  @Before
  public void setUp() throws Exception {
    testAccount = AzureBlobStorageTestAccount.createMock();
    fs = testAccount.getFileSystem();
    backingStore = testAccount.getMockStorage().getBackingStore();
  }

  @After
  public void tearDown() throws Exception {
    testAccount.cleanup();
    fs = null;
    backingStore = null;
  }

  private static String getExpectedOwner() throws Exception {
    return UserGroupInformation.getCurrentUser().getShortUserName();
  }

  private static String getExpectedPermissionString(String permissionString)
      throws Exception {
    return String.format(
        "{\"owner\":\"%s\",\"group\":\"%s\",\"permissions\":\"%s\"}",
        getExpectedOwner(),
        NativeAzureFileSystem.AZURE_DEFAULT_GROUP_DEFAULT,
        permissionString);
  }

  /**
   * Tests that ASV stamped the version in the container metadata.
   */
  @Test
  public void testContainerVersionMetadata() throws Exception {
    // Do a write operation to trigger version stamp
    fs.createNewFile(new Path("/foo"));
    HashMap<String, String> containerMetadata =
        backingStore.getContainerMetadata();
    assertNotNull(containerMetadata);
    assertEquals(AzureNativeFileSystemStore.CURRENT_ASV_VERSION,
        containerMetadata.get(AzureNativeFileSystemStore.VERSION_METADATA_KEY));
  }

  /**
   * Tests that ASV stamped the version in the container metadata if
   * it does a write operation to a pre-existing container.
   */
  @Test
  public void testPreExistingContainerVersionMetadata() throws Exception {
    // Create a mock storage with a pre-existing container that has no
    // ASV version metadata on it.
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    MockStorageInterface mockStorage = new MockStorageInterface();
    store.setAzureStorageInteractionLayer(mockStorage);
    FileSystem fs = new NativeAzureFileSystem(store);
    Configuration conf = new Configuration();
    AzureBlobStorageTestAccount.setMockAccountKey(conf);
    mockStorage.addPreExistingContainer(
        AzureBlobStorageTestAccount.getMockContainerUri(),
        null);
    fs.initialize(new URI(AzureBlobStorageTestAccount.MOCK_ASV_URI), conf);

    // Now, do some read operations (should touch the metadata)
    assertFalse(fs.exists(new Path("/IDontExist")));
    assertEquals(0, fs.listStatus(new Path("/")).length);

    // Check that no container metadata exists yet
    assertNull(mockStorage.getBackingStore().getContainerMetadata());

    // Now do a write operation - should stamp the version
    fs.mkdirs(new Path("/dir"));

    // Check that now we have the version stamp
    HashMap<String, String> containerMetadata =
        mockStorage.getBackingStore().getContainerMetadata();
    assertNotNull(containerMetadata);
    assertEquals(AzureNativeFileSystemStore.CURRENT_ASV_VERSION,
        containerMetadata.get(AzureNativeFileSystemStore.VERSION_METADATA_KEY));
  }

  @Test
  public void testPermissionMetadata() throws Exception {
    FsPermission justMe = new FsPermission(
        FsAction.READ_WRITE, FsAction.NONE, FsAction.NONE);
    Path selfishFile = new Path("/noOneElse");
    fs.create(selfishFile, justMe,
        true, 4096, fs.getDefaultReplication(), fs.getDefaultBlockSize(), null).close();
    HashMap<String, String> metadata =
        backingStore.getMetadata(
            AzureBlobStorageTestAccount.toMockUri(selfishFile));
    assertNotNull(metadata);
    String storedPermission = metadata.get("asv_permission");
    assertEquals(getExpectedPermissionString("rw-------"),
        storedPermission);
    FileStatus retrievedStatus = fs.getFileStatus(selfishFile);
    assertNotNull(retrievedStatus);
    assertEquals(justMe, retrievedStatus.getPermission());
    assertEquals(getExpectedOwner(), retrievedStatus.getOwner());
    assertEquals(NativeAzureFileSystem.AZURE_DEFAULT_GROUP_DEFAULT,
        retrievedStatus.getGroup());
  }

  @Test
  public void testFolderMetadata() throws Exception {
    Path folder = new Path("/folder");
    FsPermission justRead = new FsPermission(
        FsAction.READ, FsAction.READ, FsAction.READ);
    fs.mkdirs(folder, justRead);
    HashMap<String, String> metadata =
        backingStore.getMetadata(
            AzureBlobStorageTestAccount.toMockUri(folder));
    assertNotNull(metadata);
    assertEquals("true", metadata.get("asv_isfolder"));
    assertEquals(getExpectedPermissionString("r--r--r--"),
        metadata.get("asv_permission"));
  }
}
