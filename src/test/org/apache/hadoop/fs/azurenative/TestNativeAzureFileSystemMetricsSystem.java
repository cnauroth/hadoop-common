package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.fs.*;

import junit.framework.*;

/**
 * Tests that the ASV-specific metrics system is working correctly.
 */
public class TestNativeAzureFileSystemMetricsSystem extends TestCase {
  private static final String ASV_FILES_CREATED = "asv_files_created";
  
  private static int getFilesCreated(AzureBlobStorageTestAccount testAccount) {
    return testAccount.getLatestMetricValue(ASV_FILES_CREATED, 0).intValue();    
  }

  /**
   * Tests that when we have multiple file systems created/destroyed 
   * metrics from each are published correctly.
   * @throws Exception 
   */
  public void testMetricsAcrossFileSystems()
      throws Exception {
    AzureBlobStorageTestAccount a1, a2, a3;
    
    a1 = AzureBlobStorageTestAccount.createMock();
    assertEquals(0, getFilesCreated(a1));
    a2 = AzureBlobStorageTestAccount.createMock();
    assertEquals(0, getFilesCreated(a2));
    a1.getFileSystem().create(new Path("/foo")).close();
    assertEquals(0, getFilesCreated(a1));
    assertEquals(0, getFilesCreated(a2));
    a1.cleanup(); // Causes the file system to close, which publishes metrics
    a2.cleanup();
    assertEquals(1, getFilesCreated(a1));
    assertEquals(0, getFilesCreated(a2));
    a3 = AzureBlobStorageTestAccount.createMock();
    assertEquals(0, getFilesCreated(a3));
    a3.cleanup();
    assertEquals(0, getFilesCreated(a3));
  }
}
