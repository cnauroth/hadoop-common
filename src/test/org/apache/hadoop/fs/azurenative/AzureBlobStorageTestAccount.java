package org.apache.hadoop.fs.azurenative;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.configuration.SubsetConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.metrics2.*;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.utils.Base64;

public final class AzureBlobStorageTestAccount {

  private static final String CONNECTION_STRING_PROPERTY_NAME = "fs.azure.storageConnectionString";
  private static final String ACCOUNT_KEY_PROPERTY_NAME = "fs.azure.account.key.";
  private static final String SINK_IDENTIFIER = "identifier";
  public static final String MOCK_ACCOUNT_NAME = "mockAccount";
  public static final String MOCK_CONTAINER_NAME = "mockContainer";
  public static final String  ASV_AUTHORITY_DELIMITER = "@";
  public static final String ASV_SCHEME = "asv";
  public static final String PATH_DELIMITER = "/";
  public static final String AZURE_ROOT_CONTAINER = "$root";
  public static final String MOCK_ASV_URI = "asv://" + MOCK_CONTAINER_NAME + 
      ASV_AUTHORITY_DELIMITER + MOCK_ACCOUNT_NAME + "/";

  private CloudStorageAccount account;
  private CloudBlobContainer container;
  private CloudBlockBlob blob;
  private FileSystem fs;
  private final int sinkIdentifier;
  private MockStorageInterface mockStorage;
  private static AtomicInteger sinkIdentifierCounter = new AtomicInteger();
  private static final ConcurrentHashMap<Integer, ArrayList<MetricsRecord>> allMetrics =
      new ConcurrentHashMap<Integer, ArrayList<MetricsRecord>>();


  private AzureBlobStorageTestAccount(FileSystem fs,
      CloudStorageAccount account,
      CloudBlobContainer container,
      int sinkIdentifier) {
    this.account = account;
    this.container = container;
    this.fs = fs;
    this.sinkIdentifier = sinkIdentifier;
  }

  /**
   * Create a test account sessions with the default root container.
   * 
   * @param fs - file system, namely ASV file system
   * @param account - Windows Azure account object
   * @param blob - block blob reference
   * @param sinkIdentifier - instrumentation sink identifier
   */
  private AzureBlobStorageTestAccount(FileSystem fs, CloudStorageAccount account, 
      CloudBlockBlob blob, int sinkIdentifier) {

    this.account = account;
    this.blob = blob;
    this.fs = fs;
    this.sinkIdentifier = sinkIdentifier;
  }  

  private AzureBlobStorageTestAccount(FileSystem fs,
      MockStorageInterface mockStorage,
      int sinkIdentifier) {
    this.fs = fs;
    this.mockStorage = mockStorage;
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

  public static String getMockContainerUri() {
    return String.format("http://%s.blob.core.windows.net/%s",
        AzureBlobStorageTestAccount.MOCK_ACCOUNT_NAME,
        AzureBlobStorageTestAccount.MOCK_CONTAINER_NAME);
  }

  public static String toMockUri(String path) {
    return String.format("http://%s.blob.core.windows.net/%s/%s",
        AzureBlobStorageTestAccount.MOCK_ACCOUNT_NAME,
        AzureBlobStorageTestAccount.MOCK_CONTAINER_NAME,
        path);
  }

  public static String toMockUri(Path path) {
    return toMockUri(path.toUri().getRawPath().substring(1)); // Remove the first /
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
    MockStorageInterface mockStorage = new MockStorageInterface();
    store.setAzureStorageInteractionLayer(mockStorage);
    FileSystem fs = new NativeAzureFileSystem(store);
    setMockAccountKey(conf);
    fs.initialize(new URI(MOCK_ASV_URI), conf);
    AzureBlobStorageTestAccount testAcct =
        new AzureBlobStorageTestAccount(fs, mockStorage, sinkIdentifier);
    return testAcct;
  }

  /**
   * Sets the mock account key in the given configuration.
   * @param conf The configuration.
   */
  public static void setMockAccountKey(Configuration conf) {
    conf.set(ACCOUNT_KEY_PROPERTY_NAME + MOCK_ACCOUNT_NAME,
        Base64.encode(new byte[] {1, 2, 3}));
  }

  private static URI createAccountUri(String accountName)
      throws URISyntaxException {
    return new URI(ASV_SCHEME + ":" + PATH_DELIMITER + PATH_DELIMITER + accountName);
  }

  private static URI createAccountUri(String accountName, String containerName)
      throws URISyntaxException {
    return new URI(ASV_SCHEME + ":" + PATH_DELIMITER + PATH_DELIMITER +
        containerName + ASV_AUTHORITY_DELIMITER + accountName);
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
    CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
    container = account.createCloudBlobClient().getContainerReference(containerName);
    container.create();
    String accountUrl = account.getBlobEndpoint().getAuthority();
    String accountName = accountUrl.substring(0, accountUrl.indexOf('.'));

    // Set the account key base on whether the account is authenticated or is an anonymous
    // public account.
    //
    conf.set(CONNECTION_STRING_PROPERTY_NAME + "." + accountName, connectionString);
    String accountKey = null;

    // Capture the account key from the connection string.
    //
    accountKey = getAccountKey (connectionString);

    // Check if connection string is configured with or without an account key.
    //
    if (null == accountKey){
      // Connection string not configured with an account key.
      //
      final String errMsg = 
          String.format("Account key not configured in connection string: '%s'.", connectionString);
      throw new Exception (errMsg);
    }

    // Set the account key property.
    //
    conf.set(ACCOUNT_KEY_PROPERTY_NAME + accountName, accountKey);

    // Set account URI and initialize Azure file system.
    //
    URI accountUri = createAccountUri(accountName, containerName);
    fs.initialize(accountUri, conf);

    // Create test account initializing the appropriate member variables.
    //
    AzureBlobStorageTestAccount testAcct =
        new AzureBlobStorageTestAccount(fs, account, container, sinkIdentifier);

    return testAcct;
  }

  private static String generateContainerName() throws Exception {
    String containerName =
        String.format ("asvtests-%s-%tQ",
            System.getProperty("user.name"),
            new Date());
    return containerName;
  }

  private static String getAccountKey(String connectionString)
      throws Exception {
    String acctKey = null;

    // Split name value pairs by splitting on the ';' character
    //
    final String[] valuePairs =  connectionString.split(";");
    for (String str : valuePairs) {
      // Split on the equals sign to get the key/value pair.
      //
      String [] pair = str.split("\\=", 2);
      if (pair[0].toLowerCase().equals("AccountKey".toLowerCase())) {
        acctKey = pair[1];
        break;
      }
    }

    // Return to caller with the account name.
    //
    return acctKey;
  }

  public static void primePublicContainer(CloudBlobClient blobClient, String accountName,
      String containerName, String blobName, int fileSize)
          throws Exception {

    // Create a container if it does not exist. The container name
    // must be lower case.
    //
    CloudBlobContainer container = 
        blobClient.getContainerReference(
            "https://" + accountName +
            ".blob.core.windows.net/" + containerName);
    container.createIfNotExist();

    // Create a new shared access policy.
    //
    SharedAccessBlobPolicy sasPolicy = new SharedAccessBlobPolicy();

    // Set READ and WRITE permissions.
    //
    sasPolicy.setPermissions(EnumSet.of(
        SharedAccessBlobPermissions.READ,
        SharedAccessBlobPermissions.WRITE,
        SharedAccessBlobPermissions.LIST,
        SharedAccessBlobPermissions.DELETE));

    // Create the container permissions.
    //
    BlobContainerPermissions containerPermissions = new BlobContainerPermissions();

    // Turn public access to the container off.
    //
    containerPermissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);

    // Set the policy using the values set above.
    //
    containerPermissions.getSharedAccessPolicies().put("testasvpolicy", sasPolicy);
    container.uploadPermissions(containerPermissions);

    // Create a blob output stream.
    //
    String blobAddressUri = 
        String.format("https://%s.blob.core.windows.net/%s/%s",
            accountName, containerName, blobName);
    CloudBlockBlob blob = blobClient.getBlockBlobReference(blobAddressUri);    
    BlobOutputStream outputStream = blob.openOutputStream();

    outputStream.write(new byte[fileSize]);
    outputStream.close();
  }

  public static AzureBlobStorageTestAccount createAnonymous(
      final String blobName, final int fileSize) throws Exception {

    int sinkIdentifier = sinkIdentifierCounter.incrementAndGet();
    saveMetricsConfigFile(sinkIdentifier);

    FileSystem fs = null;
    CloudBlobContainer container = null;
    Configuration conf = new Configuration();

    // Check for the existence of a connection string.
    //
    if (!hasConnectionString(conf))
    {
      // Skip test because of missing connection string.
      //
      System.out.println (
          "Skipping live Azure test because of missing connection string.");
    }

    // Set up a session with the cloud blob client to generate SAS and check the
    // existence of a container and capture the container object.
    //
    String connectionString = conf.get(CONNECTION_STRING_PROPERTY_NAME);
    CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
    CloudBlobClient blobClient = account.createCloudBlobClient();

    // Capture the account URL and the account name.
    //
    String accountUrl = account.getBlobEndpoint().getAuthority();
    String accountName = accountUrl.substring(0, accountUrl.indexOf('.'));

    // Generate a container name and create a shared access signature string for it.
    //
    String containerName = generateContainerName();

    // Set up public container with the specified blob name.
    //
    primePublicContainer (blobClient, accountName, containerName, blobName, fileSize);

    // Capture the blob container object. It should exist after generating the 
    // shared access signature.
    //
    container = blobClient.getContainerReference(containerName);
    if (null == container || !container.exists()) {
      final String errMsg =
          String.format ("Container '%s' expected but not found while creating SAS account.");
      throw new Exception (errMsg);
    }

    // Set the account URI.
    //
    URI accountUri = createAccountUri(accountName, containerName);

    // Initialize the Native Azure file system with anonymous credentials.
    //
    fs = new NativeAzureFileSystem();
    fs.initialize(accountUri, conf);

    // Create test account initializing the appropriate member variables.
    //
    AzureBlobStorageTestAccount testAcct = new AzureBlobStorageTestAccount(
        fs, account, container, sinkIdentifier);

    // Return to caller with test account.
    //
    return testAcct;
  }

  private static CloudBlockBlob primeRootContainer(CloudBlobClient blobClient, String accountName, 
      String blobName, int fileSize) throws Exception {

    // Create a container if it does not exist. The container name
    // must be lower case.
    //
    CloudBlobContainer container = 
        blobClient.getContainerReference(
            "https://" + accountName +
            ".blob.core.windows.net/" + "$root");
    container.createIfNotExist();

    // Create a blob output stream.
    //
    String blobAddressUri = 
        String.format("https://%s.blob.core.windows.net/$root/%s",
            accountName, blobName);
    CloudBlockBlob blob = blobClient.getBlockBlobReference(blobAddressUri);    
    BlobOutputStream outputStream = blob.openOutputStream();

    outputStream.write(new byte[fileSize]);
    outputStream.close();

    // Return a reference to the block blob object.
    //
    return blob;
  }

  public static AzureBlobStorageTestAccount createRoot(
      final String blobName, final int fileSize) throws Exception {

    int sinkIdentifier = sinkIdentifierCounter.incrementAndGet();
    saveMetricsConfigFile(sinkIdentifier);

    FileSystem fs = null;
    CloudBlobContainer container = null;
    Configuration conf = new Configuration();

    // Check for the existence of a connection string.
    //
    if (!hasConnectionString(conf))
    {
      // Skip test because of missing connection string.
      //
      System.out.println (
          "Skipping live Azure test because of missing connection string.");
    }

    // Set up a session with the cloud blob client to generate SAS and check the
    // existence of a container and capture the container object.
    //
    String connectionString = conf.get(CONNECTION_STRING_PROPERTY_NAME);
    CloudStorageAccount account = CloudStorageAccount.parse(connectionString);
    CloudBlobClient blobClient = account.createCloudBlobClient();

    // Capture the account URL and the account name.
    //
    String accountUrl = account.getBlobEndpoint().getAuthority();
    String accountName = accountUrl.substring(0, accountUrl.indexOf('.'));


    // Set up public container with the specified blob name.
    //
    CloudBlockBlob blobRoot = primeRootContainer (blobClient, accountName, blobName, fileSize);

    // Capture the blob container object. It should exist after generating the 
    // shared access signature.
    //
    container = blobClient.getContainerReference(AZURE_ROOT_CONTAINER);
    if (null == container || !container.exists()) {
      final String errMsg =
          String.format ("Container '%s' expected but not found while creating SAS account.");
      throw new Exception (errMsg);
    }

    // Set the account key base on whether the account is authenticated or is an anonymous
    // public account.
    //
    conf.set(CONNECTION_STRING_PROPERTY_NAME + "." + accountName, connectionString);
    String accountKey = null;

    // Capture the account key.
    //
    accountKey = getAccountKey(connectionString);
    if (null == accountKey){
      // Connection string not configured with an account key.
      //
      final String errMsg = 
          String.format("Account key not configured in connection string: '%s'.", connectionString);
      throw new Exception (errMsg);
    }

    // Set the account key property name to test the precedence of SAS over account keys.
    //
    conf.set(ACCOUNT_KEY_PROPERTY_NAME + accountName, accountKey);

    // Set the account URI without a container name.
    //
    URI accountUri = createAccountUri(accountName);

    // Initialize the Native Azure file system with anonymous credentials.
    //
    fs = new NativeAzureFileSystem();
    fs.initialize(accountUri, conf);

    // Create test account initializing the appropriate member variables.
    // Set the container value to null for the default root container.
    //
    AzureBlobStorageTestAccount testAcct = new AzureBlobStorageTestAccount(
        fs, account, blobRoot, sinkIdentifier);

    // Return to caller with test account.
    //
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
    if (blob != null) {
      // The blob member variable is set for blobs under root containers.
      // Delete blob objects created for root container tests when cleaning
      // up the test account.
      //
      blob.delete ();
      blob = null;
    }
  }

  public FileSystem getFileSystem() {
    return fs;
  }

  /**
   * Gets the real blob container backing this account if it's not
   * a mock. 
   * @return A container, or null if it's a mock.
   */
  public CloudBlobContainer getRealContainer() {
    return container;
  }

  /**
   * Gets the real blob account backing this account if it's not
   * a mock. 
   * @return An account, or null if it's a mock.
   */
  public CloudStorageAccount getRealAccount() {
    return account;
  }

  /**
   * Gets the mock storage interface if this account is backed
   * by a mock.
   * @return The mock storage, or null if it's backed by a real account.
   */
  public MockStorageInterface getMockStorage() {
    return mockStorage;
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
