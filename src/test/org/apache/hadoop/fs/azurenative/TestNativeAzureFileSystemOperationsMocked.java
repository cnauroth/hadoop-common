package org.apache.hadoop.fs.azurenative;

import org.apache.hadoop.fs.*;

public class TestNativeAzureFileSystemOperationsMocked extends
  FSMainOperationsBaseTest {

  @Override
  public void setUp() throws Exception {
    fSys = AzureBlobStorageTestAccount.createMock().getFileSystem();
  }
}
