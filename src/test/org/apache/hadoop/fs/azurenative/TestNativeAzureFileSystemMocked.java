package org.apache.hadoop.fs.azurenative;

public class TestNativeAzureFileSystemMocked extends
    TestNativeAzureFileSystemBase {

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.createMock();
  }
}
