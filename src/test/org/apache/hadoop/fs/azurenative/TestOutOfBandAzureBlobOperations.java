package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;

import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.permission.*;
import org.junit.*;

/**
 * Tests that ASV handles things gracefully when users add blobs to
 * the Azure Storage container from outside ASV's control.
 */
public class TestOutOfBandAzureBlobOperations {
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

  private void createEmptyBlobOutOfBand(String path) {
    backingStore.setContent(
        AzureBlobStorageTestAccount.toMockUri(path),
        new byte[] { 1, 2 },
        new HashMap<String, String>());
  }

  @Test
  public void testImplicitFolderListed() throws Exception {
    createEmptyBlobOutOfBand("root/b");

    // List the blob itself.
    FileStatus[] obtained = fs.listStatus(new Path("/root/b"));
    assertNotNull(obtained);
    assertEquals(1, obtained.length);
    assertFalse(obtained[0].isDir());
    assertEquals("/root/b", obtained[0].getPath().toUri().getPath());

    // List the directory
    obtained = fs.listStatus(new Path("/root"));
    assertNotNull(obtained);
    assertEquals(1, obtained.length);
    assertFalse(obtained[0].isDir());
    assertEquals("/root/b", obtained[0].getPath().toUri().getPath());

    // Get the directory's file status
    FileStatus dirStatus = fs.getFileStatus(new Path("/root"));
    assertNotNull(dirStatus);
    assertTrue(dirStatus.isDir());
    assertEquals("/root", dirStatus.getPath().toUri().getPath());
  }

  @Test
  public void testImplicitFolderDeleted() throws Exception {
    createEmptyBlobOutOfBand("root/b");
    assertTrue(fs.exists(new Path("/root")));
    assertTrue(fs.delete(new Path("/root"), true));
    assertFalse(fs.exists(new Path("/root")));
  }

  @Test
  public void testFileInImplicitFolderDeleted() throws Exception {
    createEmptyBlobOutOfBand("root/b");
    assertTrue(fs.exists(new Path("/root")));
    assertTrue(fs.delete(new Path("/root/b"), true));
    assertTrue(fs.exists(new Path("/root")));
  }

  @Test
  public void testFileAndImplicitFolderSameName() throws Exception {
    createEmptyBlobOutOfBand("root/b");
    createEmptyBlobOutOfBand("root/b/c");
    FileStatus[] listResult = fs.listStatus(new Path("/root/b"));
    // File should win.
    assertEquals(1, listResult.length);
    assertFalse(listResult[0].isDir());
    try {
      // Trying to delete root/b/c would cause a dilemma for ASV, so
      // it should throw.
      fs.delete(new Path("/root/b/c"), true);
      assertTrue(
          "Should've thrown.",
          false);
    } catch (AzureException e) {
      assertEquals(
          "File /root/b/c has a parent directory /root/b" +
          " which is also a file. Can't resolve.",
          e.getMessage());
    }
  }

  private static enum DeepCreateTestVariation {
    File,
    Folder
  };

  /**
   * Tests that when we create the file (or folder) x/y/z, we also
   * create explicit folder blobs for x and x/y
   */
  @Test
  public void testCreatingDeepFileCreatesExplicitFolder() throws Exception {
    for (DeepCreateTestVariation variation: DeepCreateTestVariation.values()) {
      switch (variation) {
      case File:
        assertTrue(fs.createNewFile(new Path("/x/y/z")));
        break;
      case Folder:
        assertTrue(fs.mkdirs(new Path("/x/y/z")));
        break;
      }
      assertTrue(backingStore.exists(
          AzureBlobStorageTestAccount.toMockUri("x")));
      assertTrue(backingStore.exists(
          AzureBlobStorageTestAccount.toMockUri("x/y")));
      fs.delete(new Path("/x"), true);
    }
  }

  @Test
  public void testSetPermissionOnImplicitFolder() throws Exception {
    createEmptyBlobOutOfBand("root/b");
    FsPermission newPermission = new FsPermission((short)0600);
    fs.setPermission(new Path("/root"), newPermission);
    FileStatus newStatus = fs.getFileStatus(new Path("/root"));
    assertNotNull(newStatus);
    assertEquals(newPermission, newStatus.getPermission());
  }

  @Test
  public void testSetOwnerOnImplicitFolder() throws Exception {
    createEmptyBlobOutOfBand("root/b");
    fs.setOwner(new Path("/root"), "newOwner", null);
    FileStatus newStatus = fs.getFileStatus(new Path("/root"));
    assertNotNull(newStatus);
    assertEquals("newOwner", newStatus.getOwner());
  }
}
