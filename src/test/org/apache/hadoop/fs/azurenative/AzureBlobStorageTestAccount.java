package org.apache.hadoop.fs.azurenative;

import java.net.URI;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.metrics2.*;

import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.utils.Base64;

public final class AzureBlobStorageTestAccount {

  private static final String CONNECTION_STRING_PROPERTY_NAME = "fs.azure.storageConnectionString";
  private static final String ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key.";
  private static final String SINK_IDENTIFIER = "identifier";
  private CloudBlobContainer container;
  private FileSystem fs;
  private final int sinkIdentifier;
  private static AtomicInteger sinkIdentifierCounter = new AtomicInteger();
  private static final ConcurrentHashMap<Integer, ArrayList<MetricsRecord>> allMetrics =
      new ConcurrentHashMap<Integer, ArrayList<MetricsRecord>>();

  private AzureBlobStorageTestAccount(FileSystem fs, CloudBlobContainer container,
      int sinkIdentifier) {
    this.container = container;
    this.fs = fs;
    this.sinkIdentifier = sinkIdentifier;
  }

  private static void addRecord(int sinkIdentifier, MetricsRecord record) {
    ArrayList<MetricsRecord> list = new ArrayList<MetricsRecord>();
    ArrayList<MetricsRecord> previous = allMetrics.putIfAbsent(sinkIdentifier, list);
    if (previous != null) {
      list = previous;
    }
    list.add(record);
  }

  private static ArrayList<MetricsRecord> getRecords(int sinkIdentifier) {
    return allMetrics.get(sinkIdentifier);
  }

  public Number getLatestMetricValue(String metricName, Number defaultValue) 
      throws IndexOutOfBoundsException{
    ArrayList<MetricsRecord> myMetrics = getRecords(sinkIdentifier);
    if (myMetrics == null) {
      if (defaultValue != null) {
        return defaultValue;
      }
      throw new IndexOutOfBoundsException(metricName);
    }
    boolean found = false;
    Number ret = null;
    for (MetricsRecord currentRecord : myMetrics) {
      for (Metric currentMetric : currentRecord.metrics()) {
        if (currentMetric.name().equalsIgnoreCase(metricName)) {
          found = true;
          ret = currentMetric.value();
          break;
        }
      }
    }
    if (!found) {
      if (defaultValue != null) {
        return defaultValue;
      }
      throw new IndexOutOfBoundsException(metricName);
    }
    return ret;
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
  
  private static void saveMetricsConfigFile(int sinkIdentifier) {
    new org.apache.hadoop.metrics2.impl.ConfigBuilder()
    .add("azure-file-system.sink.azuretestcollector.class", StandardCollector.class.getName())
    .add("azure-file-system.sink.azuretestcollector." + SINK_IDENTIFIER, sinkIdentifier)
    .save("hadoop-metrics2-azure-file-system.properties");
  }

  public static AzureBlobStorageTestAccount createMock() throws Exception {
    int sinkIdentifier = sinkIdentifierCounter.incrementAndGet();
    saveMetricsConfigFile(sinkIdentifier);
    Configuration conf = new Configuration();
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();
    store.setAzureStorageInteractionLayer(new MockStorageInterface());
    FileSystem fs = new NativeAzureFileSystem(store);
    conf.set(ACCOUNT_KEY_PROPERTY_NAME + "mockAccount",
        Base64.encode(new byte[] {1, 2, 3}));
    fs.initialize(new URI("asv://mockAccount+mockContainer/"), conf);
    AzureBlobStorageTestAccount testAcct =
        new AzureBlobStorageTestAccount(fs, null, sinkIdentifier);
    return testAcct;
  }

  public static AzureBlobStorageTestAccount create() throws Exception {
    int sinkIdentifier = sinkIdentifierCounter.incrementAndGet();
    saveMetricsConfigFile(sinkIdentifier);
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

    // Set the account key base on whether the account is authenticated or is an anonymous
    // public account.
    //
    conf.set(CONNECTION_STRING_PROPERTY_NAME + "." + accountName, connectionString);
    String accountKey = null;

    // Split name value pairs by splitting on the ';' character
    //
    final String[] valuePairs =  connectionString.split(";");
    for (String str : valuePairs) {
      // Split on the equals sign to get the key/value pair.
      //
      String [] pair = str.split("\\=", 2);
      if (pair[0].toLowerCase().equals("AccountKey".toLowerCase())) {
        accountKey = pair[1];
        break;
      }
    }
    if (null == accountKey){
      // Connection string not configured with an account key.
      //
      final String errMsg = 
          String.format("Account key not configured in connection string: '%s'.", connectionString);
      throw new Exception (errMsg);
    }

    conf.set(ACCOUNT_KEY_PROPERTY_NAME + accountName, accountKey);

    fs.initialize(new URI("asv://" + accountName + "+" + containerName + "/"), conf);

    AzureBlobStorageTestAccount testAcct =
        new AzureBlobStorageTestAccount(fs, container, sinkIdentifier);

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

  public static class StandardCollector implements MetricsSink {
    private int sinkIdentifier;
    
    @Override
    public void init(SubsetConfiguration conf) {
      sinkIdentifier = conf.getInt(SINK_IDENTIFIER);
    }

    @Override
    public void putMetrics(MetricsRecord record) {
      addRecord(sinkIdentifier, record);
    }

    @Override
    public void flush() {
    }
  }
}
