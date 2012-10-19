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

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.permission.FsPermission;

import com.microsoft.windowsazure.services.blob.client.BlobListingDetails;
import com.microsoft.windowsazure.services.blob.client.BlobOutputStream;
import com.microsoft.windowsazure.services.blob.client.BlobProperties;
import com.microsoft.windowsazure.services.blob.client.BlobRequestOptions;
import com.microsoft.windowsazure.services.blob.client.CloudBlob;
import com.microsoft.windowsazure.services.blob.client.CloudBlobClient;
import com.microsoft.windowsazure.services.blob.client.CloudBlobContainer;
import com.microsoft.windowsazure.services.blob.client.CloudBlobDirectory;
import com.microsoft.windowsazure.services.blob.client.CloudBlockBlob;
import com.microsoft.windowsazure.services.blob.client.ListBlobItem;
import com.microsoft.windowsazure.services.core.storage.CloudStorageAccount;
import com.microsoft.windowsazure.services.core.storage.StorageCredentialsSharedAccessSignature;
import com.microsoft.windowsazure.services.core.storage.StorageException;
import com.microsoft.windowsazure.services.core.storage.utils.Utility;
import com.sun.servicetag.UnauthorizedAccessException;

/**
 * An implementation class for storing blobs in Azure Storage to support the
 * NativeAzureFileSystem class.
 */
class AzureNativeFileSystemStore implements NativeFileSystemStore {
  public static final Log LOG = LogFactory
      .getLog(AzureNativeFileSystemStore.class);

  // Constants local to this class.
  //
  private static final String KEY_CONNECTION_STRING = "fs.azure.storageConnectionString";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_IN = "fs.azure.concurrentConnection.in";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentConnection.out";
  private static final String KEY_STREAM_MIN_READ_SIZE = "fs.azure.stream.min.read.size";
  private static final String KEY_STORAGE_CONNECTION_TIMEOUT = "fs.azure.storage.timeout";
  private static final String KEY_WRITE_BLOCK_SIZE = "fs.azure.write.block.size";

  private static final String PERMISSION_METADATA_KEY = "permission";

  private static final String HTTP_SCHEME = "http";
  private static final String HTTPS_SCHEME = "https";
  private static final String ASV_URL_AUTHORITY = ".blob.core.windows.net";

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

  private CloudStorageAccount account;
  private CloudBlobContainer container;
  private CloudBlobDirectory rootDirectory;
  private CloudBlobClient serviceClient;
  private int concurrentReads = DEFAULT_CONCURRENT_READS;
  private int concurrentWrites = DEFAULT_CONCURRENT_WRITES;

  /**
   * Establish a session with Azure blob storage based on the target URI. The method determines
   * whether or not the URI target contains an explicit account or an implicit default cluster-wide
   * account.
   * 
   * @param uri  - URI for target storage blob.
   * @param conf - reference to configuration object.
   * 
   * @throws IOException errors establishing a session with Azure storage.
   */
  public void initialize(URI uri, Configuration conf) throws IOException {
    try {
      // Inspect the URI authority to determine the account is explicit or implicit. An account
      // is explicit if it takes the absolute log form of the URI 
      // ASV://<account>.blob.core.windows.net/*.
      // Implicit accounts will take the short form ASV://<container>/*. Explicit URI's do the
      // the following checks in order:
      //	1. Validate that <account> can be used with the current Hadoop cluster by checking
      //       it exists in the list of configured accounts for the cluster.
      //	2. If URI contains a valid access signature, use the access signature storage
      //       storage credentials to create an ASV blob client to access the URI path.
      //    3. If the URI does not contain a valid access signature, look up the AccountKey in
      //       the list of configured accounts for the cluster.
      //    4. If there is no AccountKey, assume anonymous public blob access when accessing the
      //       blob.
      //
      // If the account is implicit the retrieve and authenticate the from the cluster default
      // connection string.
      //
      String authUri = uri.getAuthority().toLowerCase();
      if (!authUri.endsWith(ASV_URL_AUTHORITY))
      {
          // This is an implicit path based on the ASV scheme. Connect using
      	  // the connection string from the configuration object. Notice that the default
      	  // connection string already has the AccountName and AccountKey parts of the credentials
      	  // and there is no need to append the AccountName.
          //
          account = CloudStorageAccount.parse(conf.get(KEY_CONNECTION_STRING));
          serviceClient = account.createCloudBlobClient();

          connectUsingConnectionStringCredentials(uri, conf.get(KEY_CONNECTION_STRING), true);
      }
      else
      {
    	  // The account use the account list from the configuration file to determine
    	  // if access to the account is allowed.
    	  //
    	  String connectionString = getAccountConnectionString (authUri, conf);
    	  if (null == connectionString)
    	  {
    		  // The account does not have cluster access, throw authorization exception.
    		  //
    		  final String errMsg = String.format(
			  					"Access to account '%s' not authorized from this cluster.",
			  					authUri); 
    		  throw new UnauthorizedAccessException (errMsg);
    	  }
    	  
    	  // Capture the account name from the uri.
    	  //
    	  String accountName = uri.getAuthority().toLowerCase().split(".", 2)[0];
    	  
    	  // Check if the URI has a valid access signature.
    	  //
    	  StorageCredentialsSharedAccessSignature sasCreds = parseAndValidateSAS(uri, true);
    	  if (null != sasCreds )
    	  {
    		  // Shared access signature exists, so access target using shared access signature
    		  // credentials.
    		  //
    		  connectUsingSASCredentials(uri, sasCreds);
    	  }
    	  else if ("".equals(connectionString))
    	  {
    		  // The connection string is empty implying anonymous public blob access.
    		  //
    		  connectUsingAnonymousCredentials (uri);
    	  }
    	  else
    	  {
    		  // Check if the connection string is a shared access signature.
    		  //
    		  sasCreds = new StorageCredentialsSharedAccessSignature (connectionString);
    		  if (sasCreds.getAccountName().equals(accountName))
    		  {
	    		  // If the SAS credentials were populated then the string is a shared access
    			  // signature and we should connect using the shared access signature
    			  // credentials.
	    		  //
	    		  connectUsingSASCredentials (uri, sasCreds);
    		  }
    		  else
    		  {
	    		  // A non-empty connection string implies that the account key is stored in the
	    		  // configuration file. Use the account key to access blob storage. The 
	    		  // configuration object only contains the key.  Make sure to append the account
	    		  // name to the configuration string.
	    		  //
	    		  String fullConnectionString = connectionString + ";AccountName=" + accountName;
	    		  connectUsingConnectionStringCredentials (uri, fullConnectionString, true);
    		  }
    	  }
      }


      // Set up the minimum stream read block size and the write block size.
      //
      serviceClient.setStreamMinimumReadSizeInBytes(conf.getInt(
          KEY_STREAM_MIN_READ_SIZE, DEFAULT_STREAM_MIN_READ_SIZE));

      serviceClient.setWriteBlockSizeInBytes(conf.getInt(
          KEY_WRITE_BLOCK_SIZE, DEFAULT_WRITE_BLOCK_SIZE));

      // The job may want to specify a timeout to use when engaging the
      // storage service. The default is currently 90 seconds. It may
      // be necessary to increase this value for long latencies in larger
      // jobs. If the timeout specified is greater than zero seconds use it,
      // otherwise use the default service client timeout.
      //
      int storageConnectionTimeout = conf.getInt(KEY_STORAGE_CONNECTION_TIMEOUT, 0);
      if (0 < storageConnectionTimeout) {
        serviceClient.setTimeoutInMs(storageConnectionTimeout * 1000);
      }

      // Set the concurrency values equal to the that specified in the
      // configuration
      // file. If it does not exist, set it to the default value calculated as
      // double the number of CPU cores on the client machine. The concurrency
      // value is minimum of double the cores and the read/write property.
      //
      int cpuCores = 2 * Runtime.getRuntime().availableProcessors();

      concurrentReads = conf.getInt(KEY_CONCURRENT_CONNECTION_VALUE_IN,
          Math.min(cpuCores, DEFAULT_CONCURRENT_READS));

      concurrentWrites = conf.getInt(KEY_CONCURRENT_CONNECTION_VALUE_OUT,
          Math.min(cpuCores, DEFAULT_CONCURRENT_WRITES));
    } catch (Exception e) {
      // Caught exception while attempting to initialize the Azure File System
      // store,
      // re-throw the exception.
      //
      throw new AzureException(e);
    }
  }
  
  /**
   * Get the connection string for the account specified by the URI.
   *
   * @param auth - URI authority containing the account name.
   * @param conf - configuration object.
   * 
   * @return boolean - true if account is accessible from the cluster. False otherwise.
   */
  private String getAccountConnectionString (final String authUri, final Configuration conf)
  {
	  // Capture the account name from the authority.
	  //
	  String accountName = authUri.split(".",2)[0];
	  
	  // Get the connection string and test for its existence.
	  //
	  String connectionString = conf.get(KEY_CONNECTION_STRING + "." + accountName);
	  
	  // Return to caller with the connection string.
	  //
	  return connectionString;
  }
  
  /**
   * Connect to Azure storage using shared access signature credentials.
   * 
   * @param uri			- URI to target blob
   * @param sasCreds	- shared access signature credentials
   * 
   * @throws StorageException raised on errors communicating with Azure storage.
   * @throws IOException raised on errors performing I/O or setting up the session.
   * @throws URISyntaxExceptions raised on creating malformed URI's.
   */
  private void connectUsingSASCredentials (
  						final URI uri, 
  						final StorageCredentialsSharedAccessSignature sasCreds) 
  								throws StorageException, IOException, URISyntaxException
  {
	  // Create blob service client using the shared access signature credentials.
	  //
	  URI accountUri = new URI(HTTPS_SCHEME + "://" + uri.getAuthority() + "/");
	  serviceClient = new CloudBlobClient (accountUri, sasCreds);
	  
	  // Extract the container name from the URI.
	  //
	  String containerName = uri.getPath().split(PATH_DELIMITER,3)[1].toLowerCase();
	  rootDirectory = serviceClient.getDirectoryReference(
			  					PATH_DELIMITER + containerName + PATH_DELIMITER);
	  
	  // Capture the container reference for debugging purposes.
	  //
	  container = serviceClient.getContainerReference(containerName);
  }
  
  /**
   * Connect to Azure storage using anonymous credentials.
   * 
   * @param uri			- URI to target blob (R/O access to public blob)
   * 
   * @throws StorageException raised on errors communicating with Azure storage.
   * @throws IOException raised on errors performing I/O or setting up the session.
   * @throws URISyntaxExceptions raised on creating malformed URI's.
   */
  private void connectUsingAnonymousCredentials (final URI uri) 
		  throws StorageException, IOException, URISyntaxException
  {
	  // Use an HTTP scheme since the URI specifies a publicly accessible container.
	  // Explicitly create a storage URI corresponding to the URI parameter for use
	  // in creating the service client.
	  //
	  URI storageUri = new URI(HTTP_SCHEME + "://" + uri.getAuthority());
	  
	  // Create the service client with anonymous credentials.
	  //
	  serviceClient = new CloudBlobClient(storageUri);
	  
	  // Extract the container name from the URI.
	  //
	  String containerName = uri.getPath().split(PATH_DELIMITER,3)[1].toLowerCase();
	  rootDirectory = serviceClient.getDirectoryReference(
			  					PATH_DELIMITER + containerName + PATH_DELIMITER);
	  
	  // Capture the container reference for debugging purposes.
	  //
	  container = serviceClient.getContainerReference(containerName);
  }
  
  /**
   * Connect to Azure storage using anonymous credentials.
   * 
   * @param uri				- URI to target blob
   * @param connectionString- connection string with Azure storage credentials.
   * @param isFullUri		- URI is a full-length absolute URI.
   * 
   * @throws InvalidKeyException on errors parsing the connection string credentials.
   * @throws StorageException on errors communicating with Azure storage.
   * @throws IOException on errors performing I/O or setting up the session.
   * @throws URISyntaxExceptions on creating malformed URI's.
   */
  private void connectUsingConnectionStringCredentials (
		  				final URI uri,
		  				final String connectionString,
		  				boolean isFullUri) 
		  	throws InvalidKeyException, StorageException, IOException, URISyntaxException
  {  
	  // Capture storage account from the connection string in order to create the blob client.
	  // The blob client will be used to retrieve the container if it exists, otherwise a new
	  // container is created.
	  //
	  account = CloudStorageAccount.parse(connectionString);
	  serviceClient = account.createCloudBlobClient();
	  
	  // Capture the container name and query for the container.
	  //
	  String containerName = uri.getPath().split(PATH_DELIMITER,3)[1].toLowerCase();
	  container = serviceClient.getContainerReference(containerName);
	  
	  // Assertion: The container reference should be non-null.
	  //
	  assert null != container : "Expecting a non-null container.";
	  
	  // Check for the existence of the Azure container. If it does not exist, create one.
	  //
	  if(!container.exists())
	  {
		  container.create();
	  }
	  
	  // Assertion: The container should exist.
	  //
	  assert container.exists() : "Container " + container + " expected but does not exist.";
  }
  

  public DataOutputStream pushout(String key, FsPermission permission)
      throws AzureException {
    try {
      // Check if there is an authenticated account associated with the file
      // this
      // instance of the ASV file system. If not the file system has not been
      // authenticated and all access is anonymous.
      //
      if (!isAuthenticatedAccess()) {
        // Preemptively raise an exception indicating no uploads are allowed to
        // anonymous accounts.
        //
        throw new AzureException(new IOException(
            "Uploads to public accounts using anonymous "
                + "access is prohibited."));
      }

      // Get the block blob reference from the store's container and return it.
      //
      CloudBlockBlob blob = getBlobReference(key);
      storePermission(blob, permission);

      // Set up request options.
      //
      BlobRequestOptions options = new BlobRequestOptions();
      options.setStoreBlobContentMD5(true);
      options.setConcurrentRequestCount(concurrentWrites);

      // Create the output stream for the Azure blob.
      //
      BlobOutputStream outputStream = blob
          .openOutputStream(null, options, null);

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
   * Parse the query parameter.  If credentials are present, populate a storage credentials
   * shared access signature object.
   * 
   * @param fullURI - check the query string on the URI
   * @returns StorageCredentialsSharedAccess signature if one is populated, or null otherwise.
   * 
   * @throws IllegalArgumentException if any SAS parameter is not found.
   * @throws StorageException errors occurring during any operation with the Azure runtime
   */
  private StorageCredentialsSharedAccessSignature parseAndValidateQuery (
		  				final URI fullUri) throws StorageException
  {
	  // Assertion: The full URI is expected to be non-null.
	  //
	  Utility.assertNotNull("fullUri", fullUri);
	  
	  // Capture URI query part.
	  String uriQueryToken = fullUri.getRawQuery();
	  
	  // Populate storage credentials from query token.
	  //
	  StorageCredentialsSharedAccessSignature sasCreds = 
			  new StorageCredentialsSharedAccessSignature (uriQueryToken);
	  
	  // TODO: Temporary workaround to validate the share access signature.
	  // TODO: Compare the account name on the shared access signature credentials
	  // TODO: with the account name on the URI.
	  //
	  String accountName = fullUri.getAuthority().toLowerCase().split(".", 2)[0];
	  if (!sasCreds.getAccountName().equals(accountName))
	  {
		  // Account names do not correspond and we can assume invalid SAS credential.
		  //
		  sasCreds = null;
	  }
	  
	  
	  // Return shared access signature credentials.
	  //
	  return sasCreds;
	  
/**
 * TODO: Explicit parsing of the query token is preferable. However the Constants.QueryConstants
 * TODO: enumeration is not available in microsoft.windowsazure-api.0.2.2.jar. The code below can
 * TODO: be lit up when the code base migrates to microsoft.windowsazure-api-0.3.1.jar.
 */
//	  // Reset SAS component parameters to null.
//	  //
//	  String signature 			= null;
//	  String signedStart 		= null;
//	  String signedExpiry 		= null;
//	  String signedResource 	= null;
//	  String signedPermissions 	= null;
//	  String signedIdentifier 	= null;
//	  String signedVersion 		= null;
//	  
//	  boolean sasParameterFound = false;
//	  
//	  // Initialize HashMap with query parameters.
//	  //
//	  final HashMap<String, String[]> queryParameters = 
//			  						PathUtility.parseQueryString(fullUri.getRawQuery());
//	  
//	  for (final Entry<String,String[]> mapEntry : queryParameters.entrySet())
//	  {
//		  final String lowKey = mapEntry.getKey().toLowerCase(Utility.LOCALE_US);
//		  
//		  if (lowKey.equals(Constants.QueryConstants.SIGNED_START))
//		  {
//			  signedStart = mapEntry.getValue()[0];
//			  sasParameterFound = true;
//		  }
//		  else if (lowKey.equals(Constants.QueryConstants.SIGNED_EXPIRY))
//		  {
//			  signedExpiry = mapEntry.getValue()[0];
//			  sasParameterFound = true;
//		  }
//		  else if (lowKey.equals(Constants.QueryConstants.SIGNED_PERMISSIONS))
//		  {
//			  signedPermissions = mapEntry.getValue()[0];
//			  sasParameterFound = true;
//		  }
//		  else if (lowKey.equals(Constants.QueryConstants.SIGNED_RESOURCE))
//		  {
//			  signedResource = mapEntry.getValue()[0];
//			  sasParameterFound = true;
//		  }
//		  else if (lowKey.equals(Constants.QueryConstants.SIGNED_IDENTIFIER))
//		  {
//			  signedIdentifier = mapEntry.getValue()[0];
//			  sasParameterFound = true;
//		  }
//		  else if (lowKey.equals(Constants.QueryConstants.SIGNED_VERSION))
//		  {
//			  signedVersion = mapEntry.getValue()[0];
//			  sasParameterFound = true;
//		  }
//		  else if (lowKey.equals(Constants.QueryConstants.SIGNATURE))
//		  {
//			  signature = mapEntry.getValue()[0];
//			  sasParameterFound = true;
//		  }
//	  }
//	  
//	  if (sasParameterFound)
//	  {
//		  if (null == signature)
//		  {
//			  final String errMsg = "Missing mandatory parameter for valid Shared Access Signature";
//			  throw new IllegalArgumentException(errMsg);
//		  }
//	  
//		  UriQueryBuilder builder = new UriQueryBuilder();
//		  
//		  if (!Utility.isNullOrEmpty(signedStart))
//		  {
//			  builder.add(Constants.QueryConstants.SIGNED_START, signedStart);
//		  }
//		  
//		  if (!Utility.isNullOrEmpty(signedExpiry))
//		  {
//			  builder.add(Constants.QueryConstants.SIGNED_EXPIRY, signedExpiry);
//		  }
//		  
//		  if (!Utility.isNullOrEmpty(signedPermissions))
//		  {
//			  builder.add(Constants.QueryConstants.SIGNED_PERMISSIONS, signedPermissions);
//		  }
//		  
//		  if (!Utility.isNullOrEmpty(signedResource))
//		  {
//			  builder.add(Constants.QueryConstants.SIGNED_RESOURCE, signedResource);
//		  }
//		  
//		  if (!Utility.isNullOrEmpty(signedIdentifier))
//		  {
//			  builder.add(Constants.QueryConstants.SIGNED_IDENTIFIER, signedIdentifier);
//		  }
//		  
//		  if (!Utility.isNullOrEmpty(signedVersion))
//		  {
//			  builder.add(Constants.QueryConstants.SIGNED_VERSION, signedVersion);
//		  }
//		  
//		  if (!Utility.isNullOrEmpty(signature))
//		  {
//			  builder.add(Constants.QueryConstants.SIGNATURE, signature);
//		  }
//	  
//		  final String token = builder.toString();
//		  sasCreds = new StorageCredentialsSharedAccessSignature(token);
//	  }
//	  
//	  // Return shared access signature credentials.
//	  //
//	  return sasCreds;
  }
  
  /**
   * Parse the URI for shared access signature (SAS) and validate that no other query parameters
   * are passed in with the URI. The SAS will be validated by capturing its corresponding
   * credentials.
   * 
   * @param fullUri    - the complete URI of the blob reference
   * @param usePathUris- true if path style URIs are used.
   * 
   * @returns StorageCredentialsSharedAccessSignature shared access credentials.
   * 
   * @throws URISyntaxException if the full URI is invalid.
   * @throws StorageException if an error occures in the AzureSDK runtime.
   */
  private StorageCredentialsSharedAccessSignature parseAndValidateSAS (
		  	final URI fullUri,
		  	final boolean usePathUris) throws URISyntaxException, StorageException
  {
	  // Assertion: The full URI is expected to be non-null.
	  //
	  Utility.assertNotNull("fullURI", fullUri);
	  
	  // This method expects an absolute URI. If the URI is not absolute, throw an illegal
	  // argument exception.
	  //
	  if(!fullUri.isAbsolute())
	  {
		  final String errMsg = String.format(
				  					"URI '%s' is not an absolute URI. This method only accepts" +
				                    " absolute URIs.", fullUri.toString());
		  throw new IllegalArgumentException(errMsg);
	  }
	  
	  // Parse and validate the query part of the URI. Notice that the URI was
	  // validated as an absolute URI already.
	  //
	  StorageCredentialsSharedAccessSignature sasCreds = parseAndValidateQuery(fullUri);
	  
	  
	  // Return to caller with the SAS credentials.
	  //
	  return sasCreds;
  }

  private static void storePermission(CloudBlob blob, FsPermission permission) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (metadata == null) {
      metadata = new HashMap<String, String>();
    }
    metadata.put(PERMISSION_METADATA_KEY, Short.toString(permission.toShort()));
    blob.setMetadata(metadata);
  }

  private static FsPermission getPermission(CloudBlob blob) {
    HashMap<String, String> metadata = blob.getMetadata();
    if (metadata != null && metadata.containsKey(PERMISSION_METADATA_KEY)) {
      return new FsPermission(Short.parseShort(metadata
          .get(PERMISSION_METADATA_KEY)));
    } else {
      return FsPermission.getDefault();
    }
  }

  public void storeEmptyFile(String key, FsPermission permission)
      throws IOException {
    String normKey = normalizeKey(key);

    // Upload null byte stream directly to the Azure blob store.
    //
    try {
      // Check if there is an authenticated account associated with the file
      // this
      // instance of the ASV file system. If not the file system has not been
      // authenticated and all access is anonymous.
      //
      if (!isAuthenticatedAccess()) {
        // Preemptively raise an exception indicating no uploads are allowed to
        // anonymous accounts.
        //
        throw new Exception(
            "Uploads to to public accounts using anonymous access is prohibited.");
      }

      CloudBlockBlob blob = getBlobReference(normKey);
      storePermission(blob, permission);
      blob.upload(new ByteArrayInputStream(new byte[0]), 0);
    } catch (Exception e) {
      // Caught exception while attempting upload. Re-throw as an Azure storage
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
    if (null == account) {
      // Assertion: All anonymous access uses a root directory when following
      // a path.
      //
      assert useRootDirectory() : "Expected root directory for anonymous access.";

      // Access is not authenticated.
      //
      return false;
    }

    // Access was authenticated.
    return true;
  }

  /**
   * This private method determines whether or not to use the root directory or
   * the container reference depending on whether the original FileSystem object
   * was constructed using the short- or long-form URI.
   * 
   * @returns boolean : true if it the root directory to access the file system
   *          tree and false otherwise.
   */
  private boolean useRootDirectory() {
    return null != rootDirectory;
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container depending on whether the
   * original file system object was constructed with a short- or long-form URI.
   * If the root directory is non-null the URI in the file constructor was in
   * the long form.
   * 
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs() throws StorageException,
      URISyntaxException {
    if (useRootDirectory()) {
      return rootDirectory.listBlobs();
    } else {
      assert null != container : "Expecting a non-null container for Azure store object.";
      return container.listBlobs();
    }
  }

  /**
   * This private method uses the root directory or the original container to
   * list blobs under the directory or container given a specified prefix for
   * the directory depending on whether the original file system object was
   * constructed with a short- or long-form URI. If the root directory is
   * non-null the URI in the file constructor was in the long form.
   * 
   * @param aPrefix : string name representing the prefix of containing blobs.
   * @returns blobItems : iterable collection of blob items.
   * @throws URISyntaxException
   * 
   */
  private Iterable<ListBlobItem> listRootBlobs(String aPrefix)
      throws StorageException, URISyntaxException {
    if (useRootDirectory()) {
      // Normalize the prefix for long form of the URI.
      //
      String normPrefix = normalizeKey(aPrefix);
      return rootDirectory.listBlobs(normPrefix);
    } else {
      assert null != container : "Expecting a non-null container for Azure store object.";
      return container.listBlobs(aPrefix);
    }
  }

  /**
   * This private method uses the root directory or the original container to
   * get the block blob reference depending on whether the original file system
   * object was constructed with a short- or long-form URI. If the root
   * directory is non-null the URI in the file constructor was in the long form.
   * 
   * @param aKey : a key used to query Azure for the block blob.
   * @returns blob : a reference to the Azure block blob corresponding to the
   *          key.
   * @throws URISyntaxException
   * 
   */
  private CloudBlockBlob getBlobReference(String aKey) throws StorageException,
      URISyntaxException {
    // Assertion: The incoming key should be non-null.
    //
    assert null != aKey : "Expected non-null incoming key.";

    CloudBlockBlob blob = null;
    if (useRootDirectory()) {
      blob = rootDirectory.getBlockBlobReference(aKey);
    } else {
      assert null != container : "Expecting a non-null container for Azure store object.";
      blob = container.getBlockBlobReference(aKey);
    }

    // Return with block blob.
    return blob;
  }

  /**
   * This private method normalizes the key based on the format of the
   * originating URI. If the originating URI is in the short form, eg.
   * asv://container/<key>, the method is no-op and returns the original key. If
   * the originating URI is in the long form, eg.
   * asv://<AccountName>.blob.core.windows.net/<container>/*., then the key is
   * prefixed with the container name and the container component has to be
   * removed from the key to return a normalized key.
   * 
   * Note: The format of the originating URI is determined by whether the
   * rootDirectory non-null. A non-null root directory indicates that the
   * originating URI was in the long form.
   * 
   * @param aKey : a key to be normalized
   * 
   * @returns normalizedKey : a normalized key
   */
  private String normalizeKey(String aKey) {
    // Assertion: The incoming key should be non-null.
    //
    assert null != aKey : "Expected non-null incoming key.";

    String normKey = aKey;
    if (useRootDirectory()) {
      // The root directory is non-null so the original URI must have been in
      // the long
      // form. Remove the path prefix. This prefix should correspond to the
      // container
      // name.
      //
      String[] keySplits = aKey.split(PATH_DELIMITER, 2);

      // Assertion: The first component of the split should correspond to the
      // container
      // name.
      //
      assert null != container : "Expected a non-null container for on Azure store";
      assert keySplits[0].equals(container.getName()) : "Expected container: "
          + container.getName();

      // The tail end of the split corresponds to the file path. Note the tail
      // end is not
      // prefixed with the PATH_DELIMITER.
      //
      normKey = keySplits[1];
    }

    // Return the normalized key.
    //
    return normKey;
  }

  /**
   * This private method fixes the scheme on a key based on the format of the
   * originating URI. If the originating URI is in the short form, eg.
   * asv://container/<key>, the method is no-op and returns the original key. If
   * the originating URI is in the long form, eg.
   * asv://<AccountName>.blob.core.windows.net/<container>/*., then the asv://
   * scheme replaces whatever scheme the current key has.
   * 
   * Note: The format of the originating URI is determined by whether the
   * rootDirectory non-null. A non-null root directory indicates that the
   * originating URI was in the long form.
   * 
   * @param aKey : a key to be denormalized
   * @throws URISyntaxException
   * 
   * @returns denormalizedKey : a denormalized key prefixed with the container
   *          name
   */
  private String fixScheme(String aKey) throws Exception {
    // Assertion: The incoming key should be non-null.
    //
    assert null != aKey : "Expected non-null incoming key.";

    String keyAsv = aKey;
    if (useRootDirectory()) {
      URI keyUri = new URI(aKey);
      String keyScheme = keyUri.getScheme();

      if ("".equals(keyScheme)) {
        throw new URISyntaxException(keyUri.toString(),
            "Expecting scheme on URI");
      }

      // Strip the container name from the path and return the path relative to
      // the
      // root directory of the container.
      //
      keyAsv = keyUri.getPath().split(PATH_DELIMITER, 2)[1];
    }

    // Return the normalized key.
    //
    return keyAsv;
  }

  public FileMetadata retrieveMetadata(String key) throws IOException {
    if (LOG.isDebugEnabled())
      LOG.debug("Retreiving metadata for " + key);
    
    String normalizedKey = normalizeKey(key);

    try {
      // Handle the degenerate cases where the key does not exist or the key is
      // a
      // container.
      //
      if (normalizedKey.equals("/")) {
        // The key refers to a container.
        //
        return new FileMetadata(normalizedKey, FsPermission.getDefault());
      }

      CloudBlockBlob blob = getBlobReference(normalizedKey);

      // Download attributes and return file metadata only if the blob exists.
      //
      if (blob != null && blob.exists()) {
        LOG.debug("Found it as a file.");
        // The blob exists, so capture the metadata from the blob properties.
        //
        blob.downloadAttributes();
        BlobProperties properties = blob.getProperties();
        return new FileMetadata(
            key, // Always return denormalized key with metadata.
            properties.getLength(), properties.getLastModified().getTime(),
            getPermission(blob));
      }

      // There is no file with that key name, but maybe it's a folder.
      // Query the container for the list of blobs stored in the container under
      // that key.
      //
      Iterable<ListBlobItem> objects = container
          .listBlobs(normalizedKey, true, EnumSet.of(
              BlobListingDetails.METADATA, BlobListingDetails.SNAPSHOTS), null,
              null);

      // Check to see if the container contains blob items.
      //
      if (null != objects) {
        for (ListBlobItem item : objects) {
          if (item.getUri() != null) {
            blob = getBlobReference(item.getUri().toString());
            if (blob.exists()) {
              LOG.debug("Found it as a directory - using this file under it to infer its properties: "
                  + item.getUri());
              // Found a file with under the key, use its properties to infer
              // permission and last modified time.
              //
              blob.downloadAttributes();
              return new FileMetadata(key, getPermission(blob));
            } else {
              LOG.debug("URI obtained but doesn't exist: "
                  + item.getUri().toString());
            }
          }
        }
      }

      return null;
    } catch (Exception e) {
      // Re-throw the exception as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  public DataInputStream retrieve(String key) throws IOException {
    try {
      // Normalize the key before attempting to get a reference to it.
      //
      String normKey = normalizeKey(key);

      // Get blob reference and open the input buffer stream.
      //
      CloudBlockBlob blob = getBlobReference(normKey);
      BlobRequestOptions options = new BlobRequestOptions();
      options.setConcurrentRequestCount(concurrentReads);
      BufferedInputStream inBufStream = new BufferedInputStream(
          blob.openInputStream(null, options, null));

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

  public DataInputStream retrieve(String key, long startByteOffset)
      throws IOException {
    try {
      // Normalize the key before attempting to get a reference to it.
      //
      String normKey = normalizeKey(key);

      // Get blob reference and open the input buffer stream.
      //
      CloudBlockBlob blob = getBlobReference(normKey);
      BlobRequestOptions options = new BlobRequestOptions();
      options.setConcurrentRequestCount(concurrentReads);

      // Open input stream and seek to the start offset.
      //
      InputStream in = blob.openInputStream(null, options, null);

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

  public PartialListing list(String prefix, final int maxListingCount)
      throws IOException {
    return list(prefix, maxListingCount, null);
  }

  public PartialListing list(String prefix, final int maxListingCount,
      String priorLastKey) throws IOException {
    return list(prefix, PATH_DELIMITER, maxListingCount, priorLastKey);
  }

  public PartialListing listAll(String prefix, final int maxListingCount,
      String priorLastKey) throws IOException {
    return list(prefix, null, maxListingCount, priorLastKey);
  }

  private PartialListing list(String prefix, String delimiter,
      final int maxListingCount, String priorLastKey) throws IOException {
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
        if (0 < maxListingCount && fileMetadata.size() >= maxListingCount) {
          break;
        }

        if (blobItem instanceof CloudBlob) {
          // TODO: Validate that the following code block actually makes
          // TODO: sense. Min Wei tagged it as a hack
          // Fix the scheme on the key.
          String blobKey = null;
          CloudBlob blob = (CloudBlob) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine if this instance of the file system was opened with a
          // long-
          // or short-form URI. If there is a root directory, the URI was in the
          // long form.
          //
          if (useRootDirectory()) {
            // Keys with long form URI's need special treatment. The asv://
            // scheme
            // needs to be added to the key.
            //
            blobKey = fixScheme(blob.getUri().toString());
          } else {
            blobKey = blob.getName();
          }

          FileMetadata metadata = new FileMetadata(blobKey,
              properties.getLength(), properties.getLastModified().getTime(),
              getPermission(blob));

          // Add the metadata to the list.
          //
          fileMetadata.add(metadata);
        } else {
          buildUpList((CloudBlobDirectory) blobItem, fileMetadata,
              maxListingCount);
        }
      }
      // TODO: Original code indicated that this may be a hack.
      //
      priorLastKey = null;
      return new PartialListing(priorLastKey,
          fileMetadata.toArray(new FileMetadata[] {}),
          0 == fileMetadata.size() ? new String[] {} : new String[] { prefix });
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
   * @param list -- a list of file metadata objects for each non-directory blob.
   * 
   * @param maxListingLength -- maximum length of the built up list.
   */

  private void buildUpList(CloudBlobDirectory aCloudBlobDirectory,
      ArrayList<FileMetadata> aFileMetadataList, final int maxListingCount)
      throws Exception {
    // Push the blob directory onto the stack.
    //
    AzureLinkedStack<Iterator<ListBlobItem>> dirIteratorStack = new AzureLinkedStack<Iterator<ListBlobItem>>();

    Iterable<ListBlobItem> blobItems = aCloudBlobDirectory.listBlobs();
    Iterator<ListBlobItem> blobItemIterator = blobItems.iterator();

    // Loop until all directories have been traversed in-order. Loop only the
    // following
    // conditions are satisfied:
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
        if (0 < maxListingCount && aFileMetadataList.size() >= maxListingCount) {
          break;
        }

        ListBlobItem blobItem = blobItemIterator.next();

        // Add the file metadata to the list if this is not a blob directory
        // item.
        //
        if (blobItem instanceof CloudBlob) {
          String blobKey = null;
          CloudBlob blob = (CloudBlob) blobItem;
          BlobProperties properties = blob.getProperties();

          // Determine if this instance of the file system was opened with a
          // long-
          // or short-form URI. If there is a root directory, the URI was in the
          // long form.
          //
          if (useRootDirectory()) {
            // Keys with long form URI's need special treatment. The asv://
            // scheme
            // needs to be added to the key.
            //
            blobKey = fixScheme(blob.getUri().toString());
          } else {
            blobKey = blob.getName();
          }

          FileMetadata metadata = new FileMetadata(blobKey,
              properties.getLength(), properties.getLastModified().getTime(),
              getPermission(blob));

          // Add the metadata to the list.
          //
          aFileMetadataList.add(metadata);
        } else {
          // This is a directory blob, push the current iterator onto the stack
          // of iterators and start iterating through the current directory.
          //
          dirIteratorStack.push(blobItemIterator);

          // The current blob item represents the new directory. Get an iterator
          // for this directory and continue by iterating through this
          // directory.
          //
          blobItems = ((CloudBlobDirectory) blobItem).listBlobs();
          blobItemIterator = blobItems.iterator();
        }
      }

      // Check if the iterator stack is empty. If it is set the next blob
      // iterator to
      // null. This will act as a terminator for the for-loop. Otherwise pop the
      // next
      // iterator from the stack and continue looping.
      //
      if (dirIteratorStack.isEmpty()) {
        blobItemIterator = null;
      } else {
        blobItemIterator = dirIteratorStack.pop();
      }
    }
  }

  public void delete(String key) throws IOException {
    try {
      // Get the blob reference an delete it.
      //
      CloudBlockBlob blob = getBlobReference(key);
      if (blob.exists()) {
        blob.delete();
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  public void rename(String srcKey, String dstKey) throws IOException {
    if (LOG.isDebugEnabled())
      LOG.debug("Moving " + srcKey + " to " + dstKey);
    
    try {
      // Get the source blob and assert its existence.
      //
      CloudBlockBlob srcBlob = getBlobReference(srcKey);
      if (!srcBlob.exists()) {
        throw new AzureException("Source blob " + srcKey + " does not exist.");
      }

      // Get the destination blob
      //
      CloudBlockBlob dstBlob = getBlobReference(dstKey);

      // Rename the source blob to the destination blob by copying it to the
      // destination blob then deleting it.
      //

      dstBlob.copyFromBlob(srcBlob);

      srcBlob.delete();
    } catch (Exception e) {
      // Re-throw exception as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  public void purge(String prefix) throws IOException {
    try {
      // Get all blob items with the given prefix from the container and delete
      // them.
      //
      Iterable<ListBlobItem> objects = listRootBlobs(prefix);
      for (ListBlobItem blobItem : objects) {
        ((CloudBlob) blobItem).delete();
      }
    } catch (Exception e) {
      // Re-throw as an Azure storage exception.
      //
      throw new AzureException(e);
    }
  }

  public void dump() throws IOException {
  }
}
