package org.apache.hadoop.fs.azurenative;

import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.azure.AzureException;

import junit.framework.*;

public class TestAzureFileSystemErrorConditions extends TestCase {
  public void testNoInitialize() throws Exception {
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    boolean passed = false;
    try {
      store.retrieveMetadata("foo");
      passed = true;
    } catch (AssertionError e) {
    }
    assertFalse(
        "Doing an operation on the store should throw if not initalized.",
        passed);
  }

  /**
   * Try accessing an unauthorized or non-existent (treated the same)
   * container from ASV.
   */
  public void testAccessUnauthorizedPublicContainer() throws Exception {
    Path noAccessPath = new Path(
        "asv://nonExistentContainer@hopefullyNonExistentAccount/someFile");
    NativeAzureFileSystem.suppressRetryPolicy();
    try {
      FileSystem.get(noAccessPath.toUri(), new Configuration())
        .open(noAccessPath);
      assertTrue("Should've thrown.", false);
    } catch (AzureException ex) {
      assertTrue("Unexpected message in exception " + ex,
          ex.getMessage().contains(
          "Unable to access container nonExistentContainer in account" +
          " hopefullyNonExistentAccount"));
    } finally {
      NativeAzureFileSystem.resumeRetryPolicy();
    }
  }

  public void testAccessContainerWithWrongVersion() throws Exception {
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    MockStorageInterface mockStorage = new MockStorageInterface();
    store.setAzureStorageInteractionLayer(mockStorage);
    FileSystem fs = new NativeAzureFileSystem(store);
    Configuration conf = new Configuration();
    AzureBlobStorageTestAccount.setMockAccountKey(conf);
    HashMap<String, String> metadata = new HashMap<String, String>();
    metadata.put(AzureNativeFileSystemStore.VERSION_METADATA_KEY,
        "2090-04-05"); // It's from the future!
    mockStorage.addPreExistingContainer(
        AzureBlobStorageTestAccount.getMockContainerUri(),
        metadata);

    boolean passed = false;
    try {
      fs.initialize(new URI(AzureBlobStorageTestAccount.MOCK_ASV_URI), conf);
      passed = true;
    } catch (AzureException ex) {
      assertTrue("Unexpected exception message: " + ex,
          ex.getMessage().contains("unsupported version: 2090-04-05."));
    }
    assertFalse("Should've thrown an exception because of the wrong version.",
        passed);
  }
}
