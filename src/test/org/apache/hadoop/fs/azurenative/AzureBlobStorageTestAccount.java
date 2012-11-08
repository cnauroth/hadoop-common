package org.apache.hadoop.fs.azurenative;

import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;

public final class AzureBlobStorageTestAccount {
  private static final String CONNECTION_STRING_PROPERTY_NAME = "fs.azure.storageConnectionString";
  private CloudBlobContainer container;
  private FileSystem fs;

  private AzureBlobStorageTestAccount(FileSystem fs, CloudBlobContainer container) {
    this.container = container;
    this.fs = fs;
  }

  private static boolean hasConnectionString(Configuration conf) {
    if (conf.get(CONNECTION_STRING_PROPERTY_NAME) != null) {
      return true;
    }
    if (System.getenv(CONNECTION_STRING_PROPERTY_NAME) != null) {
      conf.set(CONNECTION_STRING_PROPERTY_NAME,
          System.getenv(CONNECTION_STRING_PROPERTY_NAME));
      return true;
    }

    return false;
  }

  public static AzureBlobStorageTestAccount create() throws Exception {
    FileSystem fs = null;
    CloudBlobContainer container = null;
    Configuration conf = new Configuration();
    if (!hasConnectionString(conf)) {
      System.out
          .println("Skipping live Azure test because of missing connection string.");
      return null;
    }
    fs = new NativeAzureFileSystem();
    String containerName = String.format("asvtests-%s-%tQ",
        System.getProperty("user.name"), new Date());
    String connectionString = conf.get(CONNECTION_STRING_PROPERTY_NAME);
    CloudStorageAccount account = CloudStorageAccount
        .parse(connectionString);
    container = account
        .createCloudBlobClient().getContainerReference(containerName);
    container.create();
    String accountUrl = account.getBlobEndpoint().getAuthority();
    String accountName = accountUrl.substring(0, accountUrl.indexOf('.'));
    conf.set(CONNECTION_STRING_PROPERTY_NAME + "." + accountName, connectionString);
    fs.initialize(new URI("asv://" +
         accountUrl + "/" + containerName + "/"),
         conf);
    return new AzureBlobStorageTestAccount(fs, container);
  }

  public void cleanup() throws Exception {
    if (fs != null) {
      fs.close();
      fs = null;
    }
    if (container != null) {
      container.delete();
      container = null;
    }
  }

  public FileSystem getFileSystem() {
    return fs;
  }

}
