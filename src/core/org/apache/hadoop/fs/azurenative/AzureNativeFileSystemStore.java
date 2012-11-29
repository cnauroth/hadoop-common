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
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.permission.FsPermission;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.*;
import com.microsoft.windowsazure.services.core.storage.utils.*;

import static org.apache.hadoop.fs.azurenative.AzureStorageInteractionLayer.*;

class AzureNativeFileSystemStore implements NativeFileSystemStore {

  public static final Log LOG = LogFactory.getLog(AzureNativeFileSystemStore.class);

  private CloudStorageAccount account;
  private AzureStorageInteractionLayer storageInteractionLayer =
      new AzureStorageInteractionLayerImpl();
  private CloudBlobDirectoryWrapper rootDirectory;
  private CloudBlobContainerWrapper container;
  
  // TODO: Storage constants for SAS queries. This is a workaround until
  // TODO: microsoft.windows.azure-api.3.3.jar is available.
  //

  private static final String SIGNATURE = "sig";
  private static final String SIGNED_START = "st";
  private static final String SIGNED_EXPIRY = "se";
  private static final String SIGNED_IDENTIFIER = "si";
  private static final String SIGNED_RESOURCE = "sr";
  private static final String SIGNED_PERMISSIONS = "sp";
  private static final String SIGNED_VERSION = "sv";

  // Constants local to this class.
  //
  private static final String KEY_ACCOUNT_KEY_PREFIX = "fs.azure.account.key.";
  private static final String KEY_ACCOUNT_SAS_PREFIX = "fs.azure.sas.";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_IN = "fs.azure.concurrentConnection.in";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentConnection.out";
  private static final String KEY_STREAM_MIN_READ_SIZE = "fs.azure.stream.min.read.size";
  private static final String KEY_STORAGE_CONNECTION_TIMEOUT = "fs.azure.storage.timeout";
  private static final String KEY_WRITE_BLOCK_SIZE = "fs.azure.write.block.size";

  private static final String PERMISSION_METADATA_KEY = "permission";

  private static final String HTTP_SCHEME = "http";
  private static final String HTTPS_SCHEME = "https";
  private static final String ASV_SCHEME = "asv";
  private static final String ASV_SECURE_SCHEME = "asvs";
  private static final String ASV_URL_AUTHORITY = ".blob.core.windows.net";
  private static final String ASV_AUTHORITY_DELIMITER = "+";
  private static final String AZURE_ROOT_CONTAINER = "$root";

  // Default minimum read size for streams is 64MB.
  //
  private static final int DEFAULT_STREAM_MIN_READ_SIZE = 67108864;

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

    // The URI has a valid authority. Extract the account name name. It is the first
    // component of the ASV URI authority.
    //
    String accountName = authority.split("\\" + ASV_AUTHORITY_DELIMITER, 2)[0];
    if ("".equals(accountName)) {
      // The account name was not specified.
      //
      final String errMsg = 
          String.format("URI '%s' an non-empty account name. Expected URI with a non-empty account",
              uri.toString());
      throw new IllegalArgumentException(errMsg);
    }

    // Return with the container name. It is possible that this name is NULL.
    //
    return accountName;
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
    String containerName = null;
    if (authority.contains(ASV_AUTHORITY_DELIMITER)) {
      containerName = authority.split("\\" + ASV_AUTHORITY_DELIMITER, 2)[1];
    }

    // If the container name is not part of the authority, then the URI specifies the
    // $root container.
    //
    if (null == containerName || "".equals(containerName)) {
      // No container specified, used the default $root container for the account.
      //
      containerName = AZURE_ROOT_CONTAINER;
    }

    // Return with the container name. It is possible that this name is NULL.
    //
    return containerName;
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
   * Connect to Azure storage using shared access signature credentials.
   * 
   * @param uri - URI to target blob
   * @param sasCreds - shared access signature credentials
   * 
   * @throws StorageException raised on errors communicating with Azure storage.
   * @throws IOException raised on errors performing I/O or setting up the session.
   * @throws URISyntaxExceptions raised on creating malformed URI's.
   */
  private void connectUsingSASCredentials(final StorageCredentialsSharedAccessSignature sasCreds)
      throws StorageException, IOException, URISyntaxException {
    // Extract the container name from the URI.
    //
    String containerName = getContainerFromAuthority(sessionUri).toLowerCase();

    // Create the account URI.
    //
    URI accountUri = new URI(getHTTPScheme() + ":" + PATH_DELIMITER + PATH_DELIMITER +
        getAccountFromAuthority(sessionUri) + ASV_URL_AUTHORITY);

    // Create blob service client using the account URI and the shared access signature
    // credentials.
    //
    storageInteractionLayer.createBlobClient(accountUri, sasCreds);

    // Capture the root directory.
    //
    String containerUriString = accountUri.toString() + PATH_DELIMITER + containerName;
    rootDirectory = storageInteractionLayer.getDirectoryReference(containerUriString);

    // Capture the container reference for debugging purposes.
    //
    container = storageInteractionLayer.getContainerReference(containerName);

    // Configure Azure storage session.
    //
    configureAzureStorageSession();
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
    URI storageUri = new URI(getHTTPScheme() + ":" + PATH_DELIMITER + PATH_DELIMITER + 
        getAccountFromAuthority (uri) +
        ASV_URL_AUTHORITY);

    // Create the service client with anonymous credentials.
    //
    storageInteractionLayer.createBlobClient(storageUri);

    // Extract the container name from the URI.
    //
    String containerName = getContainerFromAuthority (uri);
    String containerUri = storageUri.toString() + PATH_DELIMITER + containerName;
    rootDirectory = storageInteractionLayer.getDirectoryReference(containerUri);

    // Capture the container reference for debugging purposes.
    //
    container = storageInteractionLayer.getContainerReference(containerName);

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
      container.create(getInstrumentedContext());
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
      // start an Azure blob client session using an account key or a SAS for the
      // the account.
      // For all URI's do the following checks in order:
      // 1. Validate that <account> can be used with the current Hadoop 
      //    cluster by checking it exists in the list of configured accounts
      //    for the cluster.
      // 2. If URI contains a valid access signature, use the access signature
      //    storage credentials to create an ASV blob client to access the
      //    URI path.
      // 3. If the URI does not contain a valid access signature, look up
      //    the AccountKey in the list of configured accounts for the cluster.
      // 4. If there is no AccountKey, assume anonymous public blob access
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

      // Check if there is a SAS associated with the storage account and
      // container by looking up their key in the job configuration object.
      // Share access signatures take a property key of the form:
      //      fs.azure.sas.<blob storage account name>+<container>
      //
      String containerName = getContainerFromAuthority(sessionUri);
      String propertyValue = sessionConfiguration.get(
          KEY_ACCOUNT_SAS_PREFIX + accountName + 
          ASV_AUTHORITY_DELIMITER + containerName);
      if (null != propertyValue){
        // Check if the connection string is a valid shared access
        // signature.
        //
        String sas = propertyValue.replace(';', '&');
        StorageCredentialsSharedAccessSignature sasCreds = parseAndValidateSAS(sas);
        if (null != sasCreds) {
          // If the SAS credentials were populated then the string
          // is a shared access signature and we should connect using
          // the shared access signature credentials.
          //
          connectUsingSASCredentials(sasCreds);

          // Return to caller.
          //
          return;
        }

        // The account does not have cluster access, throw
        // authorization exception.
        //
        final String errMsg = 
            String.format(
                "Access signature is malformed or invalid." +
                    "Access to account '%s' using configured shared access signature " +
                    "is not authorized.", accountName);
        throw new AzureException(errMsg);
      }

      // No valid shared access signatures are associated with this account. Check
      // whether the account is configured with an account key.
      //
      propertyValue = sessionConfiguration.get(KEY_ACCOUNT_KEY_PREFIX + accountName);
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

  @Override
  public DataOutputStream storefile(String key, FsPermission permission) 
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

      // Get the block blob reference from the store's container and
      // return it.
      //
      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermission(blob, permission);

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
   * Parse the query parameter. If credentials are present, populate a storage
   * credentials shared access signature object.
   * 
   * @param fullURI
   *            - check the query string on the URI
   * @returns StorageCredentialsSharedAccess signature if one is populated, or
   *          null otherwise.
   * 
   * @throws IllegalArgumentException
   *             if any SAS parameter is not found.
   * @throws StorageException
   *             errors occurring during any operation with the Azure runtime
   */
  private StorageCredentialsSharedAccessSignature parseAndValidateSAS(
      final String sasToken) throws StorageException {
    StorageCredentialsSharedAccessSignature sasCreds = null;

    // If the query token is null return with null credentials.
    //
    if (null == sasToken) {
      return sasCreds;
    }

    /**
     * TODO: Explicit parsing of the sas token is preferable. However the
     * TODO: Constants.QueryConstants enumeration is not available in
     * TODO: microsoft.windowsazure-api.0.2.2.jar. The code below can
     * TODO: be lit up when the code base migrates to 
     * TODO: microsoft.windowsazure-api-0.3.1.jar.
     */
    // Reset SAS component parameters to null.
    //
    String signature = null;
    String signedStart = null;
    String signedExpiry = null;
    String signedResource = null;
    String signedPermissions = null;
    String signedIdentifier = null;
    String signedVersion = null;

    boolean sasParameterFound = false;

    // Initialize HashMap with query parameters.
    //
    final HashMap<String, String[]> queryParameters = PathUtility.parseQueryString(sasToken);

    for (final Map.Entry<String, String[]> mapEntry : queryParameters
        .entrySet()) {
      final String lowKey = mapEntry.getKey().toLowerCase(
          Utility.LOCALE_US);

      if (lowKey.equals(SIGNED_START)) {
        signedStart = mapEntry.getValue()[0];
        sasParameterFound = true;
      } else if (lowKey.equals(SIGNED_EXPIRY)) {
        signedExpiry = mapEntry.getValue()[0];
        sasParameterFound = true;
      } else if (lowKey.equals(SIGNED_PERMISSIONS)) {
        signedPermissions = mapEntry.getValue()[0];
        sasParameterFound = true;
      } else if (lowKey.equals(SIGNED_RESOURCE)) {
        signedResource = mapEntry.getValue()[0];
        sasParameterFound = true;
      } else if (lowKey.equals(SIGNED_IDENTIFIER)) {
        signedIdentifier = mapEntry.getValue()[0];
        sasParameterFound = true;
      } else if (lowKey.equals(SIGNED_VERSION)) {
        signedVersion = mapEntry.getValue()[0];
        sasParameterFound = true;
      } else if (lowKey.equals(SIGNATURE)) {
        signature = mapEntry.getValue()[0];
        sasParameterFound = true;
      }
    }

    if (sasParameterFound) {
      if (null == signature) {
        final String errMsg = "Missing mandatory parameter for valid Shared Access Signature";
        throw new IllegalArgumentException(errMsg);
      }

      UriQueryBuilder builder = new UriQueryBuilder();

      if (!Utility.isNullOrEmpty(signedStart)) {
        builder.add(SIGNED_START, signedStart);
      }

      if (!Utility.isNullOrEmpty(signedExpiry)) {
        builder.add(SIGNED_EXPIRY, signedExpiry);
      }

      if (!Utility.isNullOrEmpty(signedPermissions)) {
        builder.add(SIGNED_PERMISSIONS, signedPermissions);
      }

      if (!Utility.isNullOrEmpty(signedResource)) {
        builder.add(SIGNED_RESOURCE, signedResource);
      }

      if (!Utility.isNullOrEmpty(signedIdentifier)) {
        builder.add(SIGNED_IDENTIFIER, signedIdentifier);
      }

      if (!Utility.isNullOrEmpty(signedVersion)) {
        builder.add(SIGNED_VERSION, signedVersion);
      }

      if (!Utility.isNullOrEmpty(signature)) {
        builder.add(SIGNATURE, signature);
      }

      final String token = builder.toString();
      sasCreds = new StorageCredentialsSharedAccessSignature(token);
    }

    // Return shared access signature credentials.
    //
    return sasCreds;
  }


  private static void storePermission(CloudBlockBlobWrapper blob, FsPermission permission) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null == metadata) {
      metadata = new HashMap<String, String> ();
    }
    metadata.put(PERMISSION_METADATA_KEY,  Short.toString(permission.toShort()));
    blob.setMetadata(metadata);
  }

  private static FsPermission getPermission (CloudBlockBlobWrapper blob) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (null != metadata && metadata.containsKey(PERMISSION_METADATA_KEY)) {
      return new FsPermission(Short.parseShort(metadata.get(PERMISSION_METADATA_KEY)));
    } else {
      return FsPermission.getDefault();
    }
  }
  @Override
  public void storeEmptyFile(String key, FsPermission permission) throws IOException {

    // Upload null byte stream directly to the Azure blob store.
    //
    try {
      // Guard against attempts to upload empty byte may occur before opening any streams
      // so first, check if a session exists, if not create a session with the Azure storage
      // server.
      //
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

      CloudBlockBlobWrapper blob = getBlobReference(key);
      storePermission(blob, permission);
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
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs() throws StorageException, URISyntaxException {
    return rootDirectory.listBlobs(
          null, false, EnumSet.noneOf(BlobListingDetails.class), null,
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
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix)
      throws StorageException, URISyntaxException {

    return rootDirectory.listBlobs(aPrefix,
          false, EnumSet.noneOf(BlobListingDetails.class), null,
          getInstrumentedContext());
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
        rootDirectory.getUri().toString() + aPrefix);

    // TODO: BUGBUG-There is a defect in the WindowsAzure SDK whick ignores the use
    // TODO: flat blob listing setting. The listBlobs calls below always traverses the
    // TODO: hierarchical namespace regardless of the value of useFlatBlobListing.
    //
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
        rootDirectory.getUri().toString() + aKey);
    // Return with block blob.
    return blob;
  }

  /**
   * This private method normalizes the key by stripping the container
   * name from the path and returns a path relative to the root directory
   * of the container.
   * 
   * @param aKey - adjust this key to a path relative to the root directory
   * @throws URISyntaxException
   * 
   * @returns normKey
   */
  private String normalizeKey(String aKey) throws URISyntaxException {

    String normKey = aKey;

    URI keyUri = new URI(aKey);
    String keyScheme = keyUri.getScheme();

    if (null == keyScheme) {
      throw new URISyntaxException(keyUri.toString(), "Expecting scheme on URI");
    }

    // Strip the container name from the path and return the path
    // relative to the root directory of the container.
    //
    normKey = keyUri.getPath().split(PATH_DELIMITER, 3)[2];

    // Return the fixed key.
    //
    return normKey;
  }
  
  private OperationContext getInstrumentedContext() {
    OperationContext ret = new OperationContext();
    ret.getResponseReceivedEventHandler().addListener(new StorageEvent<ResponseReceivedEvent>() {
      @Override
      public void eventOccurred(ResponseReceivedEvent eventArg) {
        instrumentation.webRequest();
      }
    });
    return ret;
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
        return new FileMetadata(key, FsPermission.getDefault());
      }

      CloudBlockBlobWrapper blob = getBlobReference(key);

      // Download attributes and return file metadata only if the blob
      // exists.
      //
      if (null != blob && blob.exists(getInstrumentedContext())) {

        LOG.debug("Found it as a file.");

        // The blob exists, so capture the metadata from the blob
        // properties.
        //
        blob.downloadAttributes(getInstrumentedContext());
        BlobProperties properties = blob.getProperties();

        return new FileMetadata(
            key, // Always return denormalized key with metadata.
            properties.getLength(),
            properties.getLastModified().getTime(),
            getPermission(blob));
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
              EnumSet.of(BlobListingDetails.METADATA , BlobListingDetails.SNAPSHOTS),
              null,
              getInstrumentedContext());

      // Check if the directory/container has the blob items.
      //
      if (null != objects) {
        for (ListBlobItem blobItem : objects) {
          if (blobItem.getUri() != null) {
            blob = getBlobReference(blobItem.getUri().getPath().split(PATH_DELIMITER,3)[2]);
            if (blob.exists(getInstrumentedContext())) {
              LOG.debug(
                  "Found blob as a directory-using this file under it to infer its properties" +
                      blobItem.getUri());

              // Found a blob directory under the key, use its properties to infer
              // permissions and the last modified time.
              //
              blob.downloadAttributes(getInstrumentedContext());

              // The key specifies a directory. Create a FileMetadata object which specifies
              // as such.
              //
              // TODO: Maybe there a directory metadata class which extends the file metadata
              // TODO: class or an explicit parameter indicating it is a directory rather than
              // TODO: using polymorphism to distinguish the two.
              //
              return new FileMetadata(key, getPermission(blob));
            }

            // Log that the target URI does not exist.
            //
            LOG.debug("URI obtained but does not  exist: " + blobItem.getUri().toString());
          }
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

  private PartialListing list(String prefix, String delimiter,
      final int maxListingCount, final int maxListingDepth, String priorLastKey) 
          throws IOException {
    try {
      if (0 < prefix.length() && !prefix.endsWith(PATH_DELIMITER)) {
        prefix += PATH_DELIMITER;
      }

      Iterable<ListBlobItem> objects;
      if (prefix.equals("/")) {
        objects = listRootBlobs();
      } else {
        objects = listRootBlobs(prefix);
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
          blobKey = normalizeKey(blob.getUri().toString());

          FileMetadata metadata = new FileMetadata(
              blobKey,
              properties.getLength(),
              properties.getLastModified().getTime(),
              getPermission(blob));

          // Add the metadata to the list.
          //
          fileMetadata.add(metadata);
        } else if (blobItem instanceof CloudBlobDirectoryWrapper) {
          // Determine format of directory name depending on whether an absolute
          // path is being used or not.
          //
          String dirKey = normalizeKey (((CloudBlobDirectoryWrapper) blobItem).getUri().toString());

          // Reached the targeted listing depth. Return metadata for the
          // directory using default permissions.
          //
          // TODO: Something smarter should be done about permissions. Maybe
          // TODO: inherit the permissions of the first non-directory blob.
          //
          FileMetadata directoryMetadata = new FileMetadata(dirKey, FsPermission.getDefault());

          // Add the directory metadata to the list.
          //
          fileMetadata.add(directoryMetadata);

          // Currently at a depth of one, decrement the listing depth for
          // sub-directories.
          //
          buildUpList((CloudBlobDirectoryWrapper) blobItem, fileMetadata,
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
        false, EnumSet.noneOf(BlobListingDetails.class), null,
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
          blobKey = normalizeKey(blob.getUri().toString());

          FileMetadata metadata = new FileMetadata(
              blobKey,
              properties.getLength(), 
              properties.getLastModified().getTime(),
              getPermission(blob));

          // Add the metadata to the list.
          //
          aFileMetadataList.add(metadata);
        } else if (blobItem instanceof CloudBlobDirectoryWrapper) {
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
            blobItems = ((CloudBlobDirectoryWrapper) blobItem).listBlobs(null,
                false, EnumSet.noneOf(BlobListingDetails.class), null,
                getInstrumentedContext());
            blobItemIterator = blobItems.iterator();
          } else {
            // Determine format of directory name depending on whether an absolute
            // path is being used or not.
            //
            String dirKey = normalizeKey (((CloudBlobDirectoryWrapper) blobItem).getUri().toString());

            // Reached the targeted listing depth. Return metadata for the
            // directory using default permissions.
            //
            // TODO: Something smarter should be done about permissions. Maybe
            // TODO: inherit the permissions of the first non-directory blob.
            //
            FileMetadata directoryMetadata = new FileMetadata(dirKey, FsPermission.getDefault());

            // Add the directory metadata to the list.
            //
            aFileMetadataList.add(directoryMetadata);
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

      // Get all blob items with the given prefix from the container and delete
      // them.
      //
      Iterable<ListBlobItem> objects = listRootBlobs(prefix);
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
}
