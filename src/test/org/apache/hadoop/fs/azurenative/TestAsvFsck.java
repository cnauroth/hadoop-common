package org.apache.hadoop.fs.azurenative;

import static org.junit.Assert.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.junit.*;

public class TestAsvFsck {
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

  /**
   * Counts the number of temporary blobs in the backing store.
   */
  private int getNumTempBlobs() {
    int count = 0;
    for (String key : backingStore.getKeys()) {
      if (key.contains(NativeAzureFileSystem.AZURE_TEMP_FOLDER)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Tests that we recover files properly
   */
  @Test
  public void testRecover() throws Exception {
    Path danglingFile = new Path("/crashedInTheMiddle");

    // Create a file and leave it dangling and try to recover it.
    FSDataOutputStream stream = fs.create(danglingFile);
    stream.write(new byte[] { 1, 2, 3 });
    stream.flush();

    // Now we should still only see a zero-byte file in this place
    FileStatus fileStatus = fs.getFileStatus(danglingFile);
    assertNotNull(fileStatus);
    assertEquals(0, fileStatus.getLen());
    assertEquals(1, getNumTempBlobs());

    // Run AsvFsck -move to recover the file.
    runFsck("-move");

    // Now we should the see the file in lost+found with the data there.
    fileStatus = fs.getFileStatus(new Path("/lost+found",
        danglingFile.getName()));
    assertNotNull(fileStatus);
    assertEquals(3, fileStatus.getLen());
    assertEquals(0, getNumTempBlobs());
    // But not in its original location
    assertFalse(fs.exists(danglingFile));
  }

  private void runFsck(String command) throws Exception {
    Configuration conf = fs.getConf();
    // Set the dangling cutoff to zero, so every temp blob is considered
    // dangling.
    conf.setInt(NativeAzureFileSystem.AZURE_TEMP_EXPIRY_PROPERTY_NAME, 0);
    AsvFsck fsck = new AsvFsck(conf);
    fsck.setMockFileSystemForTesting(fs);
    fsck.run(new String[]
        {
          AzureBlobStorageTestAccount.MOCK_ASV_URI,
          command
        });
  }

  /**
   * Tests that we delete dangling files properly
   */
  @Test
  public void testDelete() throws Exception {
    Path danglingFile = new Path("/crashedInTheMiddle");

    // Create a file and leave it dangling and try to delete it.
    FSDataOutputStream stream = fs.create(danglingFile);
    stream.write(new byte[] { 1, 2, 3 });
    stream.flush();

    // Now we should still only see a zero-byte file in this place
    FileStatus fileStatus = fs.getFileStatus(danglingFile);
    assertNotNull(fileStatus);
    assertEquals(0, fileStatus.getLen());
    assertEquals(1, getNumTempBlobs());

    // Run AsvFsck -delete to delete the file.
    runFsck("-delete");

    // Now we should see no trace of the file.
    assertEquals(0, getNumTempBlobs());
    assertFalse(fs.exists(danglingFile));
  }
}
