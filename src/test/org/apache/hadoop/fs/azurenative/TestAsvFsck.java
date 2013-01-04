package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import junit.framework.*;

public class TestAsvFsck extends TestCase {
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

    // Run AsvFsck -recover to recover the file.
    Configuration conf = fs.getConf();
    // Set the dangling cutoff to zero, so every temp blob is considered
    // dangling.
    conf.setInt(NativeAzureFileSystem.AZURE_TEMP_EXPIRY_PROPERTY_NAME, 0);
    AsvFsck fsck = new AsvFsck(conf);
    fsck.setMockFileSystemForTesting(fs);
    fsck.run(new String[]
        {
          AzureBlobStorageTestAccount.MOCK_ASV_URI,
          "-move"
        });

    // Now we should the see the file in lost+found with the data there.
    fileStatus = fs.getFileStatus(new Path("/lost+found",
        danglingFile.getName()));
    assertNotNull(fileStatus);
    assertEquals(3, fileStatus.getLen());
    assertEquals(0, getNumTempBlobs());
    // But not in its original location
    assertFalse(fs.exists(danglingFile));
  }
}
