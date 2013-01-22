/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azurenative;

import static org.apache.hadoop.fs.azurenative.NativeAzureFileSystem.PATH_DELIMITER;

import java.io.*;
import java.net.*;
import java.security.InvalidKeyException;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.permission.*;
import org.mortbay.util.ajax.JSON;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.*;

import static org.apache.hadoop.fs.azurenative.StorageInterface.*;

class AzureNativeFileSystemStore implements NativeFileSystemStore {

  public static final Log LOG = LogFactory.getLog(AzureNativeFileSystemStore.class);

  private CloudStorageAccount account;
  private StorageInterface storageInteractionLayer;
  private CloudBlobDirectoryWrapper rootDirectory;
  private CloudBlobContainerWrapper container;

  
  // Constants local to this class.
  //
  private static final String KEY_ACCOUNT_KEY_PREFIX = "fs.azure.account.key.";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_IN = "fs.azure.concurrentConnection.in";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentConnection.out";
  private static final String KEY_STREAM_MIN_READ_SIZE = "fs.azure.stream.min.read.size";
  private static final String KEY_STORAGE_CONNECTION_TIMEOUT = "fs.azure.storage.timeout";
  private static final String KEY_WRITE_BLOCK_SIZE = "fs.azure.write.block.size";

  private static final String PERMISSION_METADATA_KEY = "asv_permission";
  private static final String IS_FOLDER_METADATA_KEY = "asv_isfolder";
  static final String VERSION_METADATA_KEY = "asv_version";
  static final String CURRENT_ASV_VERSION = "2013-01-01";
  static final String LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY =
      "asv_tmpupload";

  private static final String HTTP_SCHEME = "http";
  private static final String HTTPS_SCHEME = "https";
  private static final String ASV_SCHEME = "asv";
  private static final String ASV_SECURE_SCHEME = "asvs";
  private static final String ASV_URL_AUTHORITY = ".blob.core.windows.net";
  private static final String ASV_AUTHORITY_DELIMITER = "@";
  private static final String AZURE_ROOT_CONTAINER = "$root";

  // Default minimum read size for streams is 4MB.
  //
  private static final int DEFAULT_STREAM_MIN_READ_SIZE = 4 * 1024 * 1024;

  // Default write block size is 4MB.
  //
  private static final int DEFAULT_WRITE_BLOCK_SIZE = 4194304;

  // DEFAULT concurrency for writes and reads.
  //
  private static final int DEFAULT_CONCURRENT_READS = 4;
  private static final int DEFAULT_CONCURRENT_WRITES = 8;

  private URI sessionUri;
  private Configuration sessionConfiguration;
  private int concurrentReads = DEFAULT_CONCURRENT_READS;
  private int concurrentWrites = DEFAULT_CONCURRENT_WRITES;
  private boolean isAnonymousCredentials = false;
  private AzureFileSystemInstrumentation instrumentation;
  private BandwidthGaugeUpdater bandwidthGaugeUpdater;
  private final static JSON permissionJsonSerializer =
      createPermissionJsonSerializer();
  private boolean suppressRetryPolicy = false;
  /**
   * If set, then we need to put a version property on the container the first
   * time we do any write operation in there.
   */
  private boolean needToStampVersionOnWrite = false;
  private final Object versionStampLock = new Object();

  /**
   * Suppress the default retry policy for the Storage, useful in unit
   * tests to test negative cases without waiting forever.
   */
  void suppressRetryPolicy() {
    suppressRetryPolicy = true;
  }

  /**
   * If we're asked by unit tests to not retry, set the retry policy factory
   * in the client accordingly.
   */
  private void suppressRetryPolicyInClientIfNeeded() {
    if (suppressRetryPolicy) {
      storageInteractionLayer.setRetryPolicyFactory(new RetryNoRetry());
    }
  }

  /**
   * Creates a JSON serializer that can serialize a PermissionStatus object
   * into the JSON string we want in the blob metadata.
   * @return The JSON serializer.
   */
  private static JSON createPermissionJsonSerializer() {
    JSON serializer = new JSON();
    serializer.addConvertor(PermissionStatus.class,
        new PermissionStatusJsonSerializer());
    return serializer;
  }

  /**
   * A convertor for PermissionStatus to/from JSON as we want it in the blob
   * metadata.
   */
  private static class PermissionStatusJsonSerializer implements JSON.Convertor {
    private static final String OWNER_TAG = "owner";
    private static final String GROUP_TAG = "group";
    private static final String PERMISSIONS_TAG = "permissions";

    @Override
    public void toJSON(Object obj, JSON.Output out) {
      PermissionStatus permissionStatus = (PermissionStatus)obj;
      // Don't store group as null, just store it as empty string
      // (which is FileStatus behavior).
      String group = permissionStatus.getGroupName() == null ?
          "" : permissionStatus.getGroupName();
      out.add(OWNER_TAG, permissionStatus.getUserName());
      out.add(GROUP_TAG, group);
      out.add(PERMISSIONS_TAG, permissionStatus.getPermission().toString());
    }

    @Override
    public Object fromJSON(@SuppressWarnings("rawtypes") Map object) {
      return PermissionStatusJsonSerializer.fromJSONMap(object);
    }

    @SuppressWarnings("rawtypes")
    public static PermissionStatus fromJSONString(String jsonString) {
      // The JSON class can only find out about an object's class (and call me)
      // if we store the class name in the JSON string. Since I don't want to
      // do that (it's an implementation detail), I just deserialize as a
      // the default Map (JSON's default behavior) and parse that.
      return fromJSONMap(
          (Map)permissionJsonSerializer.fromJSON(jsonString));
    }

    private static PermissionStatus fromJSONMap(
        @SuppressWarnings("rawtypes") Map object) {
      return new PermissionStatus(
          (String)object.get(OWNER_TAG),
          (String)object.get(GROUP_TAG),
          // The initial - below is the Unix file type,
          // which FsPermission needs there but ignores.
          FsPermission.valueOf("-" + (String)object.get(PERMISSIONS_TAG))
          );
    }
  }

  void setAzureStorageInteractionLayer(
      StorageInterface storageInteractionLayer) {
    this.storageInteractionLayer = storageInteractionLayer;
  }

  BandwidthGaugeUpdater getBandwidthGaugeUpdater() {
    return bandwidthGaugeUpdater;
  }

  /**
   * Method for the URI and configuration object necessary to create a storage
   * session with an Azure session. It parses the scheme to ensure it matches
   * the storage protocol supported by this file system.
   * 
   * @param uri - URI for target storage blob.
   * @param conf- reference to configuration object.
   * @param instrumentation - the metrics source that will keep track of operations here.
   * 
   * @throws IllegalArgumentException if URI or job object is null, or invalid scheme.
   */
  @Override
  public void initialize(URI uri, Configuration conf, AzureFileSystemInstrumentation instrumentation)
      throws IllegalArgumentException, AzureException, IOException  {

    if (null == instrumentation) {
      throw new IllegalArgumentException("Null instrumentation");
    }
    this.instrumentation = instrumentation;
    this.bandwidthGaugeUpdater = new BandwidthGaugeUpdater(instrumentation);
    if (null == this.storageInteractionLayer) {
      this.storageInteractionLayer = new StorageInterfaceImpl();
    }

    // Check that URI exists.
    //
    if (null == uri) {
      throw new IllegalArgumentException("Cannot initialize ASV file system, URI is null");
    }

    // Check scheme associated with the URI to ensure it supports one of the
    // protocols for this file system.
    //
    if (null == uri.getScheme() || (!ASV_SCHEME.equals(uri.getScheme().toLowerCase()) &&
        !ASV_SECURE_SCHEME.equals(uri.getScheme().toLowerCase()))) {
      final String errMsg = 
          String.format(
              "Cannot initialize ASV file system. Scheme is not supported, " +
                  "expected '%s' scheme.", ASV_SCHEME);
      throw new  IllegalArgumentException(errMsg);
    }

    // Check authority for the URI to guarantee that it is non-null.
    //
    if (null == uri.getAuthority()) {
      final String errMsg =
          String.format(
              "Cannot initialize ASV file system, URI authority not recognized.");
      throw new IllegalArgumentException(errMsg);
    }

    // Check that configuration object is non-null.
    //
    if (null == conf) {
      throw new IllegalArgumentException("Cannot initialize ASV file system, URI is null");
    }

    // Incoming parameters validated.  Capture the URI and the job configuration object.
    //
    sessionUri = uri;
    sessionConfiguration = conf;

    // Start an Azure storage session.
    //
    createAzureStorageSession ();
  }

  /**
   * Method to extract the account name from an Azure URI.
   * 
   * @param uri -- ASV blob URI
   * @returns accountName -- the account name for the URI.
   * @throws URISyntaxException if the URI does not have an authority it is badly formed.
   */
  private String getAccountFromAuthority(URI uri) throws URISyntaxException {

    // Check to make sure that the authority is valid for the URI.
    //
    String authority = uri.getRawAuthority();
    if (null == authority){
      // Badly formed or illegal URI.
      //
      throw new URISyntaxException(uri.toString(), "Expected URI with a valid authority");
    }

    // Check if authority container the delimiter separating the account name from the 
    // the container.
    //
    if (!authority.contains (ASV_AUTHORITY_DELIMITER)) {
      return authority;
    } 

    // Split off the container name and the authority.
    //
    String [] authorityParts = authority.split(ASV_AUTHORITY_DELIMITER, 2);

    // Because the string contains an '@' delimiter, a container must be specified.
    //
    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      // Badly formed ASV authority since there is no container.
      //
      final String errMsg = 
          String.format("URI '%' has a malformed ASV authority, expected container name." +
              "Authority takes the form" + " asv://[<container name>@]<account name>",
              uri.toString());
      throw new IllegalArgumentException(errMsg);
    }

    // Return with the container name. It is possible that this name is NULL.
    //
    return authorityParts[1];
  }

  /**
   * Method to extract the container name from an Azure URI.
   * 
   * @param uri -- ASV blob URI
   * @returns containerName -- the container name for the URI. May be null.
   * @throws URISyntaxException if the uri does not have an authority it is badly formed.
   */
  private String getContainerFromAuthority(URI uri) throws URISyntaxException {

    // Check to make sure that the authority is valid for the URI.
    //
    String authority = uri.getRawAuthority();
    if (null == authority){
      // Badly formed or illegal URI.
      //
      throw new URISyntaxException(uri.toString(), "Expected URI with a valid authority");
    }

    // The URI has a valid authority. Extract the container name. It is the second
    // component of the ASV URI authority.
    //
    if (!authority.contains(ASV_AUTHORITY_DELIMITER)) {
      // The authority does not have a container name. Use the default container by
      // setting the container name to the default Azure root container.
      //
      return AZURE_ROOT_CONTAINER;
    } 

    // Split off the container name and the authority.
    //
    String [] authorityParts = authority.split(ASV_AUTHORITY_DELIMITER, 2);

    // Because the string contains an '@' delimiter, a container must be specified.
    //
    if (authorityParts.length < 2 || "".equals(authorityParts[0])) {
      // Badly formed ASV authority since there is no container.
      //
      final String errMsg =
          String.format("URI '%' has a malformed ASV authority, expected container name." +
              "Authority takes the form" + " asv://[<container name>@]<account name>",
              uri.toString());
      throw new IllegalArgumentException(errMsg);
    }

    // Set the container name from the first entry for the split parts of the authority.
    //
    return authorityParts[0];
  }

  /**
   * Build an Azure storage connection string given an account name and the key.
   * 
   * @param accountName - account name associated with the Azure account.
   * @param accountKey  -  Azure storage account key.
   * @return connectionString - connection string build from account name and key
   * @throws URISyntaxException 
   * @throws AzureException 
   */
  private String buildAzureStorageConnectionString (final String accountName, 
      final String accountKey) throws AzureException, URISyntaxException {
    String connectionString = "DefaultEndpointsProtocol=" + getHTTPScheme() + ";" +
        "AccountName=" + accountName + ";" +
        "AccountKey=" + accountKey;

    // Return to the caller with the connection string.
    //
    return connectionString;
  }

  /**
   * Get the appropriate return the appropriate scheme for communicating with
   * Azure depending on whether asv or asvs is specified in the target URI.
   * 
   * return scheme - HTTPS if asvs is specified or HTTP if asv is specified.
   * throws URISyntaxException if session URI does not have the appropriate
   * scheme.
   * @throws AzureException 
   */
  private String getHTTPScheme () throws URISyntaxException, AzureException {
    // Determine the appropriate scheme for communicating with Azure storage.
    //
    String sessionScheme = sessionUri.getScheme();
    if (null == sessionScheme) {
      // The session URI has no scheme and is malformed.
      //
      final String errMsg =
          String.format("Session URI does not have a URI scheme as is malformed.", sessionUri);
      throw new URISyntaxException(sessionUri.toString(), errMsg);
    }

    if (ASV_SCHEME.equals(sessionScheme.toLowerCase())){
      return HTTP_SCHEME;
    }

    if (ASV_SECURE_SCHEME.equals(sessionScheme.toLowerCase())){
      return HTTPS_SCHEME;
    }

    final String errMsg =
        String.format ("Scheme '%s://' not recognized by Hadoop ASV file system",sessionScheme);
    throw new AzureException (errMsg);
  }

  /**
   * Set the configuration parameters for this client storage session with Azure.
   * 
   */
  private void configureAzureStorageSession() {

    // Assertion: Target session URI already should have been captured.
    //
    assert null != sessionUri :
      "Expected a non-null session URI when configuring storage session";

    // Assertion: A client session already should have been established with Azure.
    //
    assert null != storageInteractionLayer :
      String.format("Cannot configure storage session for URI '%s' " + 
          "if storage session has not been established.", sessionUri.toString());

    // Set up the minimum stream read block size and the write block
    // size.
    //
    storageInteractionLayer.setStreamMinimumReadSizeInBytes(
        sessionConfiguration.getInt(
            KEY_STREAM_MIN_READ_SIZE, DEFAULT_STREAM_MIN_READ_SIZE));

    storageInteractionLayer.setWriteBlockSizeInBytes(
        sessionConfiguration.getInt(
            KEY_WRITE_BLOCK_SIZE, DEFAULT_WRITE_BLOCK_SIZE));

    // The job may want to specify a timeout to use when engaging the
    // storage service. The default is currently 90 seconds. It may
    // be necessary to increase this value for long latencies in larger
    // jobs. If the timeout specified is greater than zero seconds use
    // it,
    // otherwise use the default service client timeout.
    //
    int storageConnectionTimeout = 
        sessionConfiguration.getInt(
            KEY_STORAGE_CONNECTION_TIMEOUT, 0);

    if (0 < storageConnectionTimeout) {
      storageInteractionLayer.setTimeoutInMs(storageConnectionTimeout * 1000);
    }

    // Set the concurrency values equal to the that specified in the
    // configuration file. If it does not exist, set it to the default
    // value calculated as double the number of CPU cores on the client
    // machine. The concurrency value is minimum of double the cores and
    // the read/write property.
    //
    int cpuCores = 2 * Runtime.getRuntime().availableProcessors();

    concurrentReads = sessionConfiguration.getInt(
        KEY_CONCURRENT_CONNECTION_VALUE_IN,
        Math.min(cpuCores, DEFAULT_CONCURRENT_READS));

    concurrentWrites = sessionConfiguration.getInt(
        KEY_CONCURRENT_CONNECTION_VALUE_OUT,
        Math.min(cpuCores, DEFAULT_CONCURRENT_WRITES));
  }

  /**
   * Connect to Azure storage using anonymous credentials.
   * 
   * @param uri - URI to target blob (R/O access to public blob)
   * 
   * @throws StorageException raised on errors communicating with Azure storage.
   * @throws IOException raised on errors performing I/O or setting up the session.
   * @throws URISyntaxExceptions raised on creating mal-formed URI's.
   */
  private void connectUsingAnonymousCredentials(final URI uri)
      throws StorageException, IOException, URISyntaxException {
    // Use an HTTP scheme since the URI specifies a publicly accessible
    // container. Explicitly create a storage URI corresponding to the URI 
    // parameter for use in creating the service client.
    //
    String accountName = getAccountFromAuthority(uri);
    URI storageUri = new URI(getHTTPScheme() + ":" + PATH_DELIMITER + PATH_DELIMITER + 
        accountName +
        ASV_URL_AUTHORITY);

    // Create the service client with anonymous credentials.
    //
    storageInteractionLayer.createBlobClient(storageUri);
    suppressRetryPolicyInClientIfNeeded();

    // Extract the container name from the URI.
    //
    String containerName = getContainerFromAuthority (uri);
    String containerUri = storageUri.toString() + PATH_DELIMITER + containerName;
    rootDirectory = storageInteractionLayer.getDirectoryReference(containerUri);

    // Capture the container reference.
    //
    container = storageInteractionLayer.getContainerReference(containerName);

    // Check for container existence, and our ability to access it.
    //
    try {
      if (!container.exists(getInstrumentedContext())) {
        throw new AzureException("Container " + containerName +
            " in account " + accountName + " not found, and we can't create " +
            " it using anoynomous credentials.");
      }
    } catch (StorageException ex) {
      throw new AzureException("Unable to access container " + containerName +
          " in account " + accountName +
          " using anonymous credentials, and no credentials found for them " +
          " in the configuration.", ex);
    }

    // Accessing the storage server unauthenticated using
    // anonymous credentials.
    //
    isAnonymousCredentials = true;

    // Configure Azure storage session.
    //
    configureAzureStorageSession();
  }

  /**
   * Connect to Azure storage using anonymous credentials.
   * 
   * @param uri - URI to target blob
   * @param connectionString - connection string with Azure storage credentials.
   * 
   * @throws InvalidKeyException raised on errors parsing the connection string credentials.
   * @throws StorageException raised on errors communicating with Azure storage.
   * @throws IOException raised on errors performing I/O or setting up the session.
   * @throws URISyntaxExceptions raised on on creating malformed URI's.
   */
  private void connectUsingConnectionStringCredentials(
      final String accountName, final String containerName, final String accountKey) 
          throws InvalidKeyException, StorageException, IOException, URISyntaxException {

    // Build the connection string from the account name and the account key.
    //
    String connectionString = buildAzureStorageConnectionString(accountName, accountKey);

    // Capture storage account from the connection string in order to create
    // the blob client. The blob client will be used to retrieve the container
    // if it exists, otherwise a new container is created.
    //
    account = CloudStorageAccount.parse(connectionString);
    storageInteractionLayer.createBlobClient(account);
    suppressRetryPolicyInClientIfNeeded();

    // Set the root directory.
    //
    String containerUri = getHTTPScheme() + ":" + PATH_DELIMITER + PATH_DELIMITER +
        accountName + 
        ASV_URL_AUTHORITY +
        PATH_DELIMITER +
        containerName;
    rootDirectory = storageInteractionLayer.getDirectoryReference(containerUri);

    // Capture the container reference for debugging purposes.
    //
    container = storageInteractionLayer.getContainerReference(containerUri.toString());

    // Check for the existence of the Azure container. If it does not exist,
    // create one.
    //
    if (!container.exists(getInstrumentedContext())) {
      // Stamp the version in there.
      storeVersionAttribute(container);
      container.create(getInstrumentedContext());
    } else {
      // Container already exists, check to see if it's not my current version
      container.downloadAttributes(getInstrumentedContext());
      String containerVersion = retrieveVersionAttribute(container);
      if (containerVersion != null // No version is OK,
                                   // means it was probably created by the user
          && !containerVersion.equals(CURRENT_ASV_VERSION)) {
        throw new AzureException("The container " + containerName +
            " is at an unsupported version: " + containerVersion +
            ". Current supported version: " + CURRENT_ASV_VERSION);
      }
      if (containerVersion == null) {
        // We're OK with reading from containers with no version metadata,
        // but we need to stamp the version the first time we do any write
        // operation so that we later know what version of ASV messed with it.
        needToStampVersionOnWrite = true;
      }
    }

    // Assertion: The container should exist at this point.
    //
    assert container.exists(getInstrumentedContext()) :
      String.format ("Container %s expected but does not exist", containerName);

    // Configure Azure storage session.
    //
    configureAzureStorageSession();
  }

  /**
   * Establish a session with Azure blob storage based on the target URI. The
   * method determines whether or not the URI target contains an explicit
   * account or an implicit default cluster-wide account.
   * 
   * @throws AzureException
   * @throws IOException
   */
  private void createAzureStorageSession () 
      throws AzureException, IOException {

    // Make sure this object was properly initialized with references to
    // the sessionUri and sessionConfiguration.
    //
    if (null == sessionUri || null == sessionConfiguration) {
      throw new AzureException(
          "Filesystem object not initialized properly." +
          "Unable to start session with Azure Storage server.");
    }

    // File system object initialized, attempt to establish a session
    // with the Azure storage service for the target URI string.
    //
    try {
      // Inspect the URI authority to determine the account and use the account to
      // start an Azure blob client session using an account key for the
      // the account or anonymously.
      // For all URI's do the following checks in order:
      // 1. Validate that <account> can be used with the current Hadoop 
      //    cluster by checking it exists in the list of configured accounts
      //    for the cluster.
      // 2. Look up the AccountKey in the list of configured accounts for the cluster.
      // 3. If there is no AccountKey, assume anonymous public blob access
      //    when accessing the blob.
      //
      // If the URI does not specify a container use the default root container under
      // the account name.
      //

      // Assertion: Container name on the session Uri should be non-null.
      //
      assert null != getContainerFromAuthority(sessionUri) : 
        String.format("Non-null container expected from session URI: ", 
            sessionUri.toString());

      // Get the account name.
      //
      String accountName = getAccountFromAuthority(sessionUri);
      if(null == accountName) {
        // Account name is not specified as part of the URI. Throw indicating
        // an invalid account name.
        //
        final String errMsg = 
            String.format("Cannot load ASV file system account name not specified in URI:",
                sessionUri.toString());
        throw new AzureException(errMsg);
      }
      instrumentation.setAccountName(accountName);

      String containerName = getContainerFromAuthority(sessionUri);
      instrumentation.setContainerName(containerName);

      // Check whether the account is configured with an account key.
      //
      String propertyValue = sessionConfiguration.get(KEY_ACCOUNT_KEY_PREFIX + accountName);
      if (null != propertyValue) {

        // Account key was found. Create the Azure storage session using the account
        // key and container.
        //
        connectUsingConnectionStringCredentials(getAccountFromAuthority(sessionUri),
            getContainerFromAuthority(sessionUri), propertyValue);

        // Return to caller
        //
        return;
      }

      // The account access is not configured for this cluster. Try anonymous access.
      //
      connectUsingAnonymousCredentials(sessionUri);

    } catch (Exception e) {
      // Caught exception while attempting to initialize the Azure File
      // System store, re-throw the exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * This should be called from any method that does any modifications
   * to the underlying container: it makes sure to put the ASV current
   * version in the container's metadata if it's not already there.
   */
  private void versionContainer() throws StorageException {
    if (!needToStampVersionOnWrite) {
      return;
    }
    synchronized (versionStampLock) {
      if (!needToStampVersionOnWrite) {
        return;
      }

      // Make sure that the container still doesn't have any version
      // stamped on it.
      container.downloadAttributes(getInstrumentedContext());
      if (retrieveVersionAttribute(container) == null) {
        // No ASV version found, just stamp the current version.
        storeVersionAttribute(container);
        container.uploadMetadata(getInstrumentedContext());
      }
      needToStampVersionOnWrite = false; // Done, no need to do this anymore.
    }
  }

  @Override
  public DataOutputStream storefile(String key, PermissionStatus permissionStatus)
      throws AzureException {
    try {

      // Check if a session exists, if not create a session with the
      // Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg = 
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AzureException(errMsg);
      }

      // Check if there is an authenticated account associated with the
      // file this instance of the ASV file system. If not the file system
      // has not been authenticated and all access is anonymous.
      //
      if (!isAuthenticatedAccess()) {
        // Preemptively raise an exception indicating no uploads are
        // allowed to anonymous accounts.
        //
        throw new AzureException(new IOException(
            "Uploads to public accounts using anonymous "
                + "access is prohibited."));
      }

      versionContainer();

      /**
       * Note: Windows Azure Blob Storage does not allow the creation of arbitrary directory
       *      paths under the default $root directory.  This is by design to eliminate
       *      ambiguity in specifying a implicit blob address. A blob in the $root conatiner
       *      cannot include a / in its name and must be careful not to include a trailing
       *      '/' when referencing  blobs in the $root container.
       *      A '/; in the $root container permits ambiguous blob names as in the following
       *      example involving two containers $root and mycontainer:
       *                http://myaccount.blob.core.windows.net/$root
       *                http://myaccount.blob.core.windows.net/mycontainer
       *      If the URL "mycontainer/somefile.txt were allowed in $root then the URL:
       *                http://myaccount.blob.core.windows.net/mycontainer/myblob.txt
       *      could mean either:
       *        (1) container=mycontainer; blob=myblob.txt
       *        (2) container=$root; blob=mycontainer/myblob.txt
       *
       *      To avoid this type of ambiguity the Azure blob storage prevents arbitrary path
       *      under $root.  For a simple and more consistent user experience it was decided
       *      to eliminate the opportunity for creating such paths by making the $root container
       *      read-only under ASV.  (cf. JIRA HADOOP-254).
       */

      //Check that no attempt is made to write to blobs on default
      //$root containers.
      //
      if (AZURE_ROOT_CONTAINER.equals(getContainerFromAuthority(sessionUri))){
        // Azure containers are restricted to non-root containers.
        //
        final String errMsg =
            String.format(
                "Writes to '%s' container for URI '%s' are prohibited, " +
                    "only updates on non-root containers permitted.",
                    AZURE_ROOT_CONTAINER, sessionUri.toString());
        throw new AzureException(errMsg);
      }

      // Get the block blob reference from the store's container and
      // return it.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);

      // Set up request options.
      //
      BlobRequestOptions options = new BlobRequestOptions();
      options.setStoreBlobContentMD5(true);
      options.setConcurrentRequestCount(concurrentWrites);

      // Create the output stream for the Azure blob.
      //
      OutputStream outputStream = blob.openOutputStream(
          options, getInstrumentedContext());

      // Return to caller with DataOutput stream.
      //
      DataOutputStream dataOutStream = new DataOutputStream(outputStream);
      return dataOutStream;
    } catch (Exception e) {
      // Caught exception while attempting to open the blob output stream.
      // Re-throw
      // as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Default permission to use when no permission metadata is found.
   * @return The default permission to use.
   */
  private static PermissionStatus defaultPermissionNoBlobMetadata() {
    return new PermissionStatus("", "", FsPermission.getDefault());
  }

  private static void storeMetadataAttribute(CloudBlockBlobWrapper blob,
      String key, String value) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null == metadata) {
      metadata = new HashMap<String, String> ();
    }
    metadata.put(key, value);
    blob.setMetadata(metadata);
  }

  private static String getMetadataAttribute(CloudBlockBlobWrapper blob,
      String key) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null == metadata || !metadata.containsKey(key)) {
      return null;
    }
    return metadata.get(key);
  }

  private void storePermissionStatus(CloudBlockBlobWrapper blob,
      PermissionStatus permissionStatus) {
    storeMetadataAttribute(blob,
        PERMISSION_METADATA_KEY, permissionJsonSerializer.toJSON(permissionStatus));
  }

  private PermissionStatus getPermissionStatus(CloudBlockBlobWrapper blob) {
    String permissionMetadataValue = getMetadataAttribute(blob,
        PERMISSION_METADATA_KEY);
    if (permissionMetadataValue != null) {
      return PermissionStatusJsonSerializer.fromJSONString(
          permissionMetadataValue);
    } else {
      return defaultPermissionNoBlobMetadata();
    }
  }

  private static void storeFolderAttribute(CloudBlockBlobWrapper blob) {
    storeMetadataAttribute(blob, IS_FOLDER_METADATA_KEY, "true");
  }

  private static void storeLinkAttribute(CloudBlockBlobWrapper blob,
      String linkTarget) {
    storeMetadataAttribute(blob,
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY, linkTarget);
  }
  
  private static String getLinkAttributeValue(CloudBlockBlobWrapper blob) {
    return getMetadataAttribute(blob,
        LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY);
  }

  private static boolean retrieveFolderAttribute(CloudBlockBlobWrapper blob) {
    HashMap<String, String> metadata = blob.getMetadata();
    return null != metadata && metadata.containsKey(IS_FOLDER_METADATA_KEY);
  }

  private static void storeVersionAttribute(CloudBlobContainerWrapper container) {
    HashMap<String, String> metadata = container.getMetadata();
    if (null == metadata) {
      metadata = new HashMap<String, String> ();
    }
    metadata.put(VERSION_METADATA_KEY, CURRENT_ASV_VERSION);
    container.setMetadata(metadata);
  }

  private static String retrieveVersionAttribute(CloudBlobContainerWrapper container) {
    HashMap<String, String> metadata = container.getMetadata();
    return metadata == null || !metadata.containsKey(VERSION_METADATA_KEY) ?
        null : metadata.get(VERSION_METADATA_KEY);
  }

  @Override
  public void storeEmptyFolder(String key, PermissionStatus permissionStatus) throws AzureException {

    if (null == storageInteractionLayer) {
      final String errMsg =
          String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
      throw new AssertionError(errMsg);
    }

    // Check if there is an authenticated account associated with the file
    // this instance of the ASV file system. If not the file system has not
    // been authenticated and all access is anonymous.
    //
    if (!isAuthenticatedAccess()) {
      // Preemptively raise an exception indicating no uploads are
      // allowed to
      // anonymous accounts.
      //
      throw new AzureException(
          "Uploads to to public accounts using anonymous access is prohibited.");
    }

    try {
      versionContainer();
      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);
      storeFolderAttribute(blob);
      blob.upload(new ByteArrayInputStream(new byte[0]), getInstrumentedContext());
    } catch (Exception e) {
      // Caught exception while attempting upload. Re-throw as an Azure
      // storage
      // exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Stores an empty blob that's linking to the temporary file where're we're
   * uploading the initial data.
   */
  @Override
  public void storeEmptyLinkFile(String key, String tempBlobKey,
      PermissionStatus permissionStatus) throws AzureException {
    if (null == storageInteractionLayer) {
      final String errMsg =
          String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
      throw new AssertionError(errMsg);
    }
    // Check if there is an authenticated account associated with the file
    // this instance of the ASV file system. If not the file system has not
    // been authenticated and all access is anonymous.
    //
    if (!isAuthenticatedAccess()) {
      // Preemptively raise an exception indicating no uploads are
      // allowed to
      // anonymous accounts.
      //
      throw new AzureException(
          "Uploads to to public accounts using anonymous access is prohibited.");
    }

    try {
      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermissionStatus(blob, permissionStatus);
      storeLinkAttribute(blob, tempBlobKey);
      blob.upload(new ByteArrayInputStream(new byte[0]), getInstrumentedContext());
    } catch (Exception e) {
      // Caught exception while attempting upload. Re-throw as an Azure
      // storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * If the blob with the given key exists and has a link in its metadata
   * to a temporary file (see storeEmptyLinkFile), this method returns
   * the key to that temporary file.
   * Otherwise, returns null.
   */
  @Override
  public String getLinkInFileMetadata(String key) throws AzureException {
    if (null == storageInteractionLayer) {
      final String errMsg =
          String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
      throw new AssertionError(errMsg);
    }

    try {
      CloudBlockBlobWrapper blob = getBlobReference(key);
      blob.downloadAttributes(getInstrumentedContext());
      return getLinkAttributeValue(blob);
    } catch (Exception e) {
      // Caught exception while attempting download. Re-throw as an Azure
      // storage exception.
      //
      throw new AzureException(e);
    }
  }

  /**
   * Private method to check for authenticated access.
   * 
   * @ returns boolean -- true if access is credentialed and authenticated and
   * false otherwise.
   */
  private boolean isAuthenticatedAccess() throws AzureException {

    if (isAnonymousCredentials) {
      // Assertion: No account should be associated with this connection.
      //
      assert null == account : "Non-null account not expected for anonymous credentials";

      // Access to this storage account is unauthenticated.
      //
      return false;
    }
    // Access is authenticated.
    //
    return true;
  }

  /**
   * This private method determines whether or not to use absolute paths or
   * the container reference depending on whether the original FileSystem
   * object was constructed using the short- or long-form URI.
   * 
   * @returns boolean : true if the suffix of the authority is ASV_URL_AUTHORITY
   */
  public boolean useAbsolutePath() {
    // Check that the suffix of the authority is ASV_URL_AUTHORITY
    //
    return sessionUri.getAuthority().toLowerCase().endsWith(ASV_URL_AUTHORITY);
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container depending on whether the
   * original file system object was constructed with a short- or long-form
   * URI. If the root directory is non-null the URI in the file constructor
   * was in the long form.
   * 
   * @param includeMetadata if set, the listed items will have their metadata
   *                        populated already.
   * 
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs(boolean includeMetadata)
      throws StorageException, URISyntaxException {
    return rootDirectory.listBlobs(
        null, false,
        includeMetadata ?
            EnumSet.of(BlobListingDetails.METADATA) :
              EnumSet.noneOf(BlobListingDetails.class),
              null,
              getInstrumentedContext());
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container given a specified prefix for
   * the directory depending on whether the original file system object was
   * constructed with a short- or long-form URI. If the root directory is
   * non-null the URI in the file constructor was in the long form.
   * 
   * @param aPrefix
   *            : string name representing the prefix of containing blobs.
   * @param includeMetadata if set, the listed items will have their metadata
   *                        populated already.
   * 
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix,
      boolean includeMetadata)
          throws StorageException, URISyntaxException {

    return rootDirectory.listBlobs(aPrefix,
        false,
        includeMetadata ?
            EnumSet.of(BlobListingDetails.METADATA) :
              EnumSet.noneOf(BlobListingDetails.class),
              null,
              getInstrumentedContext());
  }

  /**
   * Appends the given string to the root directory's URI, and returns
   * the new URI.
   * @param pathSuffix The suffix to append.
   * @return The URI with the suffix appended.
   */
  private URI appendToRootDirectoryPath(String pathSuffix)
      throws URISyntaxException {
    return new URI(
        rootDirectory.getUri().getScheme(),
        rootDirectory.getUri().getHost(),
        rootDirectory.getUri().getPath() + pathSuffix,
        rootDirectory.getUri().getQuery(),
        rootDirectory.getUri().getFragment());
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container given a specified prefix for
   * the directory depending on whether the original file system object was
   * constructed with a short- or long-form URI.  It also uses the specified
   * flat or hierarchical option, listing details options, request options,
   * and operation context.
   * 
   * @param aPrefix string name representing the prefix of containing blobs.
   * @param useFlatBlobListing - the list is flat if true, or hierarchical otherwise.
   * @param listingDetails - determine whether snapshots, metadata, commmitted/uncommitted data
   * @param options - object specifying additional options for the request. null = default options
   * @param opContext - context of the current operation
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix, boolean useFlatBlobListing, 
      EnumSet<BlobListingDetails> listingDetails, BlobRequestOptions options,
      OperationContext opContext) throws StorageException, URISyntaxException {

    CloudBlobDirectoryWrapper directory = storageInteractionLayer.getDirectoryReference(
        appendToRootDirectoryPath(aPrefix).toString());

    return directory.listBlobs(
        null,
        useFlatBlobListing,
        listingDetails,
        options,
        opContext);
  }

  /**
   * This private method uses the root directory or the original container to
   * get the block blob reference depending on whether the original file
   * system object was constructed with a short- or long-form URI. If the root
   * directory is non-null the URI in the file constructor was in the long
   * form.
   * 
   * @param aKey
   *            : a key used to query Azure for the block blob.
   * @returns blob : a reference to the Azure block blob corresponding to the
   *          key.
   * @throws URISyntaxException
   * 
   */
  private CloudBlockBlobWrapper getBlobReference(String aKey)
      throws StorageException, URISyntaxException {

    CloudBlockBlobWrapper blob = storageInteractionLayer.getBlockBlobReference(
        appendToRootDirectoryPath(aKey).toString());
    // Return with block blob.
    return blob;
  }

  /**
   * This private method normalizes the key by stripping the container
   * name from the path and returns a path relative to the root directory
   * of the container.
   * 
   * @param keyUri - adjust this key to a path relative to the root directory
   * 
   * @returns normKey
   */
  private String normalizeKey(URI keyUri) {

    String normKey;

    // Strip the container name from the path and return the path
    // relative to the root directory of the container.
    //
    normKey = keyUri.getPath().split(PATH_DELIMITER, 3)[2];

    // Return the fixed key.
    //
    return normKey;
  }

  /**
   * This private method normalizes the key by stripping the container
   * name from the path and returns a path relative to the root directory
   * of the container.
   *
   * @param blob - adjust the key to this blob to a path relative to the root
   *               directory
   *
   * @returns normKey
   */
  private String normalizeKey(CloudBlockBlobWrapper blob) {
    return normalizeKey(blob.getUri());
  }

  /**
   * This private method normalizes the key by stripping the container
   * name from the path and returns a path relative to the root directory
   * of the container.
   *
   * @param blob - adjust the key to this directory to a path relative to the
   *               root directory
   *
   * @returns normKey
   */
  private String normalizeKey(CloudBlobDirectoryWrapper directory) {
    String dirKey = normalizeKey(directory.getUri());
    // Strip the last /
    if (dirKey.endsWith(PATH_DELIMITER)) {
      dirKey = dirKey.substring(0, dirKey.length() - 1);
    }
    return dirKey;
  }

  /**
   * Creates a new OperationContext for the Azure Storage operation that has
   * listeners hooked to it that will update the metrics for this file system.
   * @return The OperationContext object to use.
   */
  private OperationContext getInstrumentedContext() {
    final OperationContext operationContext = new OperationContext();
    ResponseReceivedMetricUpdater.hook(operationContext, instrumentation, bandwidthGaugeUpdater);
    ErrorMetricUpdater.hook(operationContext, instrumentation);
    return operationContext;
  }

  @Override
  public FileMetadata retrieveMetadata(String key) throws IOException {

    // Attempts to check status may occur before opening any streams so first,
    // check if a session exists, if not create a session with the Azure storage server.
    //
    if (null == storageInteractionLayer) {
      final String errMsg = 
          String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
      throw new AssertionError(errMsg);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Retrieving metadata for " + key);
    }

    try {
      // Handle the degenerate cases where the key does not exist or the
      // key is a container.
      //
      if (key.equals("/")) {
        // The key refers to root directory of container.
        //
        return new FileMetadata(key, defaultPermissionNoBlobMetadata(), BlobMaterialization.Implicit);
      }

      CloudBlockBlobWrapper blob = getBlobReference(key);

      // Download attributes and return file metadata only if the blob
      // exists.
      //
      if (null != blob && blob.exists(getInstrumentedContext())) {

        if (LOG.isDebugEnabled()) {
          LOG.debug("Found " + key +
              " as an explicit blob. Checking if it's a file or folder.");
        }

        // The blob exists, so capture the metadata from the blob
        // properties.
        //
        blob.downloadAttributes(getInstrumentedContext());

        if (retrieveFolderAttribute(blob)) {
          if (LOG.isDebugEnabled()) {
            LOG.debug(key + " is a folder blob.");
          }
          return new FileMetadata(key, getPermissionStatus(blob), BlobMaterialization.Explicit);
        } else {
          if (LOG.isDebugEnabled()) {
            LOG.debug(key + " is a normal blob.");
          }
          BlobProperties properties = blob.getProperties();

          return new FileMetadata(
              key, // Always return denormalized key with metadata.
              properties.getLength(),
              properties.getLastModified().getTime(),
              getPermissionStatus(blob));
        }
      }

      // There is no file with that key name, but maybe it is a folder.
      // Query the underlying folder/container to list the blobs stored
      // there under that key.
      // TODO: Replace container with generic directory name.
      // 
      Iterable<ListBlobItem> objects =
          listRootBlobs(
              key,
              true,
              EnumSet.of(BlobListingDetails.METADATA),
              null,
              getInstrumentedContext());

      // Check if the directory/container has the blob items.
      //
      for (ListBlobItem blobItem : objects) {
        if (blobItem instanceof CloudBlockBlobWrapper) {
          LOG.debug(
              "Found blob as a directory-using this file under it to infer its properties" +
                  blobItem.getUri());

          // The key specifies a directory. Create a FileMetadata object which specifies
          // as such.
          //
          // TODO: Maybe there a directory metadata class which extends the file metadata
          // TODO: class or an explicit parameter indicating it is a directory rather than
          // TODO: using polymorphism to distinguish the two.
          //
          return new FileMetadata(key, getPermissionStatus((CloudBlockBlobWrapper)blobItem),
              BlobMaterialization.Implicit);
        }
      }

      // Return to caller with a null metadata object.
      //
      return null;

    } catch (Exception e) {
      // Re-throw the exception as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public DataInputStream retrieve(String key) throws AzureException, IOException {
    try {
      // Check if a session exists, if not create a session with the
      // Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg = 
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AssertionError(errMsg);
      }

      // Get blob reference and open the input buffer stream.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);
      BlobRequestOptions options = new BlobRequestOptions();
      options.setConcurrentRequestCount(concurrentReads);
      BufferedInputStream inBufStream = new BufferedInputStream(
          blob.openInputStream(options, getInstrumentedContext()));

      // Return a data input stream.
      //
      DataInputStream inDataStream = new DataInputStream(inBufStream);
      return inDataStream;
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public DataInputStream retrieve(String key, long startByteOffset)
      throws AzureException, IOException {
    try {  
      // Check if a session exists, if not create a session with the
      // Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg = 
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AssertionError(errMsg);
      }

      // Get blob reference and open the input buffer stream.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);
      BlobRequestOptions options = new BlobRequestOptions();
      options.setConcurrentRequestCount(concurrentReads);

      // Open input stream and seek to the start offset.
      //
      InputStream in = blob.openInputStream(options, getInstrumentedContext());

      // Create a data input stream.
      //
      DataInputStream inDataStream = new DataInputStream(in);
      inDataStream.skip(startByteOffset);
      return inDataStream;
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public PartialListing list(String prefix, final int maxListingCount, final int maxListingDepth)
      throws IOException {
    return list(prefix, maxListingCount, maxListingDepth, null);
  }

  @Override
  public PartialListing list(String prefix, final int maxListingCount, final int maxListingDepth,
      String priorLastKey) throws IOException {
    return list(prefix, PATH_DELIMITER, maxListingCount, maxListingDepth, priorLastKey);
  }

  @Override
  public PartialListing listAll(String prefix, final int maxListingCount, 
      final int maxListingDepth, String priorLastKey) throws IOException {
    return list(prefix, null, maxListingCount, maxListingDepth, priorLastKey);
  }

  /**
   * Searches the given list of {@link FileMetadata} objects for a directory with
   * the given key.
   * @param list The list to search. 
   * @param key The key to search for.
   * @return The wanted directory, or null if not found.
   */
  private static FileMetadata getDirectoryInList(final Iterable<FileMetadata> list,
      String key) {
    for (FileMetadata current : list) {
      if (current.isDir() && current.getKey().equals(key)) {
        return current;
      }
    }
    return null;
  }

  private PartialListing list(String prefix, String delimiter,
      final int maxListingCount, final int maxListingDepth, String priorLastKey) 
          throws IOException {
    try {
      if (0 < prefix.length() && !prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }

      Iterable<ListBlobItem> objects;
      if (prefix.equals("/")) {
        objects = listRootBlobs(true);
      } else {
        objects = listRootBlobs(prefix, true);
      }

      ArrayList<FileMetadata> fileMetadata = new ArrayList<FileMetadata>();
      for (ListBlobItem blobItem : objects) {
        // Check that the maximum listing count is not exhausted.
        //
        if (0 < maxListingCount
            && fileMetadata.size() >= maxListingCount) {
          break;
        }

        if (blobItem instanceof CloudBlockBlobWrapper) {
          // TODO: Validate that the following code block actually
          // makes
          // TODO: sense. Min Wei tagged it as a hack
          // Fix the scheme on the key.
          String blobKey = null;
          CloudBlockBlobWrapper blob = (CloudBlockBlobWrapper) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine format of the blob name depending on whether an absolute
          // path is being used or not.
          //
          //
          blobKey = normalizeKey(blob);

          FileMetadata metadata;
          if (retrieveFolderAttribute(blob)) {
            metadata = new FileMetadata(blobKey, getPermissionStatus(blob),
                BlobMaterialization.Explicit);
          } else {
            metadata = new FileMetadata(
                blobKey,
                properties.getLength(),
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }

          // Add the metadata to the list, but remove any existing duplicate
          // entries first that we may have added by finding nested files.  
          //
          FileMetadata existing = getDirectoryInList(fileMetadata, blobKey);
          if (existing != null) {
            fileMetadata.remove(existing);
          }
          fileMetadata.add(metadata);
        } else if (blobItem instanceof CloudBlobDirectoryWrapper) {
          CloudBlobDirectoryWrapper directory = (CloudBlobDirectoryWrapper) blobItem;
          // Determine format of directory name depending on whether an absolute
          // path is being used or not.
          //
          String dirKey = normalizeKey(directory);
          // Strip the last /
          if (dirKey.endsWith(PATH_DELIMITER)) {
            dirKey = dirKey.substring(0, dirKey.length() - 1);
          }

          // Reached the targeted listing depth. Return metadata for the
          // directory using default permissions.
          //
          // TODO: Something smarter should be done about permissions. Maybe
          // TODO: inherit the permissions of the first non-directory blob.
          //
          FileMetadata directoryMetadata = new FileMetadata(dirKey, defaultPermissionNoBlobMetadata(),
              BlobMaterialization.Implicit);

          // Add the directory metadata to the list only if it's not already there.
          //
          if (getDirectoryInList(fileMetadata, dirKey) == null) {
            fileMetadata.add(directoryMetadata);
          }

          // Currently at a depth of one, decrement the listing depth for
          // sub-directories.
          //
          buildUpList(directory, fileMetadata,
              maxListingCount, maxListingDepth - 1);
        }
      }
      // TODO: Original code indicated that this may be a hack.
      //
      priorLastKey = null;
      return new PartialListing(priorLastKey,
          fileMetadata.toArray(new FileMetadata[] {}),
          0 == fileMetadata.size() ? new String[] {}
      : new String[] { prefix });
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  /*
   * Build up a metadata list of blobs in an Azure blob directory. This method
   * uses a in-order first traversal of blob directory structures to maintain
   * the sorted order of the blob names.
   * 
   * @param dir -- Azure blob directory
   * 
   * @param list -- a list of file metadata objects for each non-directory
   * blob.
   * 
   * @param maxListingLength -- maximum length of the built up list.
   */

  private void buildUpList(CloudBlobDirectoryWrapper aCloudBlobDirectory,
      ArrayList<FileMetadata> aFileMetadataList, final int maxListingCount, 
      final int maxListingDepth) throws Exception {

    // Push the blob directory onto the stack.
    //
    AzureLinkedStack<Iterator<ListBlobItem>> dirIteratorStack = 
        new AzureLinkedStack<Iterator<ListBlobItem>>();

    Iterable<ListBlobItem> blobItems = aCloudBlobDirectory.listBlobs(null,
        false, EnumSet.of(BlobListingDetails.METADATA), null,
        getInstrumentedContext());
    Iterator<ListBlobItem> blobItemIterator = blobItems.iterator();

    if (0 == maxListingDepth || 0 == maxListingCount)
    {
      // Recurrence depth and listing count are already exhausted. Return
      // immediately.
      //
      return;
    }

    // The directory listing depth is unbounded if the maximum listing depth
    // is negative.
    //
    final boolean isUnboundedDepth = (maxListingDepth < 0);

    // Reset the current directory listing depth.
    //
    int listingDepth = 1;

    // Loop until all directories have been traversed in-order. Loop only
    // the following conditions are satisfied:
    // (1) The stack is not empty, and
    // (2) maxListingCount > 0 implies that the number of items in the
    // metadata list is less than the max listing count.
    //
    while (null != blobItemIterator
        && (maxListingCount <= 0 || aFileMetadataList.size() < maxListingCount)) {
      while (blobItemIterator.hasNext()) {
        // Check if the count of items on the list exhausts the maximum
        // listing count.
        //
        if (0 < maxListingCount
            && aFileMetadataList.size() >= maxListingCount) {
          break;
        }

        ListBlobItem blobItem = blobItemIterator.next();

        // Add the file metadata to the list if this is not a blob
        // directory
        // item.
        //
        if (blobItem instanceof CloudBlockBlobWrapper) {
          String blobKey = null;
          CloudBlockBlobWrapper blob = (CloudBlockBlobWrapper) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine format of the blob name depending on whether an absolute
          // path is being used or not.
          //
          //
          blobKey = normalizeKey(blob);

          FileMetadata metadata;
          if (retrieveFolderAttribute(blob)) {
            metadata = new FileMetadata(blobKey, getPermissionStatus(blob),
                BlobMaterialization.Explicit);
          } else {
            metadata = new FileMetadata(
                blobKey,
                properties.getLength(), 
                properties.getLastModified().getTime(),
                getPermissionStatus(blob));
          }

          // Add the directory metadata to the list only if it's not already there.
          //
          FileMetadata existing = getDirectoryInList(aFileMetadataList, blobKey);
          if (existing != null) {
            aFileMetadataList.remove(existing);
          }
          aFileMetadataList.add(metadata);
        } else if (blobItem instanceof CloudBlobDirectoryWrapper) {
          CloudBlobDirectoryWrapper directory = (CloudBlobDirectoryWrapper) blobItem;

          // This is a directory blob, push the current iterator onto
          // the stack of iterators and start iterating through the current
          // directory.
          //
          if (isUnboundedDepth || maxListingDepth > listingDepth)
          {
            // Push the current directory on the stack and increment the listing
            // depth.
            //
            dirIteratorStack.push(blobItemIterator);
            ++listingDepth;

            // The current blob item represents the new directory. Get
            // an iterator for this directory and continue by iterating through 
            // this directory.
            //
            blobItems = directory.listBlobs(null,
                false, EnumSet.noneOf(BlobListingDetails.class), null,
                getInstrumentedContext());
            blobItemIterator = blobItems.iterator();
          } else {
            // Determine format of directory name depending on whether an absolute
            // path is being used or not.
            //
            String dirKey = normalizeKey(directory);

            if (getDirectoryInList(aFileMetadataList, dirKey) == null) {
              // Reached the targeted listing depth. Return metadata for the
              // directory using default permissions.
              //
              // TODO: Something smarter should be done about permissions. Maybe
              // TODO: inherit the permissions of the first non-directory blob.
              //
              FileMetadata directoryMetadata = new FileMetadata(dirKey,
                  defaultPermissionNoBlobMetadata(),
                  BlobMaterialization.Implicit);

              // Add the directory metadata to the list.
              //
              aFileMetadataList.add(directoryMetadata);
            }
          }
        }
      }

      // Traversal of directory tree

      // Check if the iterator stack is empty. If it is set the next blob
      // iterator to null. This will act as a terminator for the for-loop.
      // Otherwise pop the next iterator from the stack and continue looping.
      //
      if (dirIteratorStack.isEmpty()) {
        blobItemIterator = null;
      } else {
        // Pop the next directory item from the stack and decrement the
        // depth.
        //
        blobItemIterator = dirIteratorStack.pop();
        --listingDepth;

        // Assertion: Listing depth should not be less than zero.
        //
        assert listingDepth >= 0 : "Non-negative listing depth expected";
      }
    }
  }

  @Override
  public void delete(String key) throws IOException {
    try {
      versionContainer();
      // Get the blob reference an delete it.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);
      if (blob.exists(getInstrumentedContext())) {
        blob.delete(getInstrumentedContext());
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public void rename(String srcKey, String dstKey)
      throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving " + srcKey + " to " + dstKey);
    }

    try {
      // Attempts rename may occur before opening any streams so first,
      // check if a session exists, if not create a session with the Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg = 
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AssertionError(errMsg);
      }

      versionContainer();
      // Get the source blob and assert its existence. If the source key
      // needs to be normalized then normalize it.
      //
      CloudBlockBlobWrapper srcBlob = getBlobReference(srcKey);

      if (!srcBlob.exists(getInstrumentedContext())) {
        throw new AzureException ("Source blob " + srcKey+ " does not exist.");
      }

      // Get the destination blob. The destination key always needs to be 
      // normalized.
      //
      CloudBlockBlobWrapper dstBlob = getBlobReference(dstKey);

      // Rename the source blob to the destination blob by copying it to
      // the destination blob then deleting it.
      //
      dstBlob.copyFromBlob(srcBlob, getInstrumentedContext());
      srcBlob.delete(getInstrumentedContext());
    } catch (Exception e) {
      // Re-throw exception as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }


  @Override
  public void purge(String prefix) throws IOException {
    try {

      // Attempts to purge may occur before opening any streams so first,
      // check if a session exists, if not create a session with the Azure storage server.
      //
      if (null == storageInteractionLayer) {
        final String errMsg = 
            String.format("Storage session expected for URI '%s' but does not exist.", sessionUri);
        throw new AssertionError(errMsg);
      }

      versionContainer();
      // Get all blob items with the given prefix from the container and delete
      // them.
      //
      Iterable<ListBlobItem> objects = listRootBlobs(prefix, false);
      for (ListBlobItem blobItem : objects) {
        ((CloudBlob) blobItem).delete(DeleteSnapshotsOption.NONE, null, null, getInstrumentedContext());
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  @Override
  public void dump() throws IOException {
  }

  @Override
  public void close() {
    bandwidthGaugeUpdater.close();
  }
}
