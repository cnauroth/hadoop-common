package org.apache.hadoop.fs.azurenative;

import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;

public final class AzureBlobStorageTestAccount {
  public enum UriType {
    FULL, IMPLICIT, RELATIVE
  }

  private static final String URI_PATH_ENVIRONMENT = "URI_TYPE"; 

  private static final String CONNECTION_STRING_PROPERTY_NAME = "fs.azure.storageConnectionString";
  private String prefixUri;
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

    String env = System.getenv(URI_PATH_ENVIRONMENT);
    UriType typeOfUri = UriType.RELATIVE;
    if (null != env) {
      typeOfUri = UriType.valueOf(env.toUpperCase());
    }
    if (UriType.IMPLICIT == typeOfUri) {
      fs.initialize (new URI ("asv://" + containerName + "/"), conf);
    } else {
      fs.initialize(new URI("asv://" + accountUrl + "/" + containerName + "/"), conf);
    }

    AzureBlobStorageTestAccount testAcct = new AzureBlobStorageTestAccount(fs, container);
    testAcct.setUriPrefix(typeOfUri, "asv", accountName, containerName);

    return testAcct;
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

  private void setUriPrefix (UriType typeOfUri, String scheme, String account, String container) {
    switch (typeOfUri) {
      case FULL: 
        prefixUri = scheme + "://" + account + ".blob.core.windows.net" + "/" + container + "/";
        break;
      case IMPLICIT:
        prefixUri = scheme + "://" + container + "/";
        break;

      default:
        prefixUri = "";
        break;
    }
  }

  public String getUriPrefix () {
    return prefixUri;
  }

}
