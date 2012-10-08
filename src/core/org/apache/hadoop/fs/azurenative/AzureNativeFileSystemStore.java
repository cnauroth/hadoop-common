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
import java.util.ArrayList;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureException;

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
import com.microsoft.windowsazure.services.core.storage.StorageException;

class AzureNativeFileSystemStore implements NativeFileSystemStore {
  // Constants local to this class.
  //
  private static final String KEY_CONNECTION_STRING = "fs.azure.storageConnectionString";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_IN = "fs.azure.concurrentConnection.in";
  private static final String KEY_CONCURRENT_CONNECTION_VALUE_OUT = "fs.azure.concurrentConnection.out";
  private static final String KEY_STREAM_MIN_READ_SIZE = "fs.azure.stream.min.read.size";
  private static final String KEY_STORAGE_CONNECTION_TIMEOUT = "fs.azure.write.block.size";
  private static final String KEY_WRITE_BLOCK_SIZE = "fs.azure.write.block.size";

  private static final String HTTP_SCHEME = "http";
  private static final String ASV_URL_AUTHORITY = ".blob.core.windows.net";

  // Default minimum read size for streams is 64MB.
  //
  private static final int DEFAULT_STREAM_MIN_READ_SIZE = 67108864;

  // Default write block size is 4MB.
  //
  private final static int DEFAULT_WRITE_BLOCK_SIZE = 4194304;

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

  public void initialize(URI uri, Configuration conf) throws IOException {
    try {
      // Allocate service client and container name and initialize to null.
      //
      String containerName = null;

      // Based on the URI authority determine if the service client is public
      // (requires no credentials) or private (requires credentials). For
      // now we assume all relative ASV://*.blob.core.windows.net/* are public
      // while absolute ASV://<container>/* schemes are private.
      //
      if (uri.getAuthority().toLowerCase().endsWith(ASV_URL_AUTHORITY)) {
        // HTTP scheme, so the URI specifies a public container.
        // Explicitly create a storage URI corresponding to the URI parameter
        // for use when creating the service client.
        //
        URI storageURI = new URI(HTTP_SCHEME + "://" + uri.getAuthority());

        // Create an service client with anonymous credentials.
        //
        serviceClient = new CloudBlobClient(storageURI);

        // Extract the container name from the URI.
        //
        containerName = uri.getPath().split(PATH_DELIMITER, 3)[1].toLowerCase();
        rootDirectory = serviceClient.getDirectoryReference(PATH_DELIMITER
            + containerName + PATH_DELIMITER);

        // It is good to have a reference to the container for debugging
        // purposes.
        //
        container = serviceClient.getContainerReference(containerName);
      } else {
        // Anything else is treated as an absolute path based on the ASV scheme.
        //
        // Retrieve storage account from the connection string in order to
        // create a blob
        // client. The blob client will be used to retrieve the container if it
        // exists,
        // otherwise a new container is created.
        //
        account = CloudStorageAccount.parse(conf.get(KEY_CONNECTION_STRING));
        serviceClient = account.createCloudBlobClient();

        // Query the container.
        //
        containerName = uri.getAuthority();
        container = serviceClient.getContainerReference(containerName);

        // Assertion: At this point the container should be non-null otherwise
        // one
        // of the get containerReference calls would raise an exception
        // and we will never get to this point.
        //
        assert null != container : "Expecting a non-null container.";

        // Check for the existence of the azure storage container. If it exists
        // use it, otherwise,
        // create a new container.
        //
        if (!container.exists()) {
          container.create();
        }

        // Assertion: The container should exist.
        //
        assert container.exists() : "Container " + containerName
            + " expected but does not exist.";
      }

      // Set up the minimum stream read block size and the write block size.
      //
      serviceClient.setStreamMinimumReadSizeInBytes(conf.getInt(
          KEY_STREAM_MIN_READ_SIZE, DEFAULT_STREAM_MIN_READ_SIZE));

      serviceClient.setWriteBlockSizeInBytes(conf.getInt(
          KEY_STORAGE_CONNECTION_TIMEOUT, DEFAULT_WRITE_BLOCK_SIZE));

      // The job may want to specify a timeout to use when engaging the
      // storage service. The default is currently 90 seconds. It may
      // be necessary to increase this value for long latencies in larger
      // jobs. If the timeout specified is greater than zero seconds use it,
      // otherwise use the default service client timeout.
      //
      int storageConnectionTimeout = conf.getInt(
          KEY_STORAGE_CONNECTION_TIMEOUT, 0);
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

  public DataOutputStream pushout(String key) throws AzureException {
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

  public void storeEmptyFile(String key) throws IOException {
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
      blob.upload(new ByteArrayInputStream(new byte[0]), 0);
    } catch (Exception e) {
      // Caught exception while attempting upload. Re-throw as an Azure storage
      // exception.
      //
      throw new AzureException(e);
    }
  }

  private boolean exists(String key) throws Exception {
    // Query the container for the list of blobs stored in the container.
    //
    CloudBlobDirectory dir = getDirectoryReference(key);
    Iterable<ListBlobItem> blobItems = dir.listBlobs();

    // Check to see if this is a directory/container of blob items.
    //
    if (null != blobItems) {
      if (blobItems.iterator().hasNext()) {
        return true;
      }
    }

    CloudBlockBlob blob = getBlobReference(key);
    if (null != blob) {
      // Check if the blob exists under the current container
      //
      return blob.exists();
    }

    // Blob does not exist.
    //
    return false;
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
   * get a directory reference depending on whether the original file system
   * object was constructed with a short- or long-form URI. If the root
   * directory is non-null the URI in the file constructor was in the long form.
   * 
   * @param aKey : a key used to query Azure for the directory.
   * @returns directory : a reference to the Azure block blob corresponding to
   *          the key.
   * @throws URISyntaxException
   * 
   */
  private CloudBlobDirectory getDirectoryReference(String aKey)
      throws StorageException, URISyntaxException {
    // Assertion: The incoming key should be non-null.
    //
    assert null != aKey : "Expected non-null incoming key.";

    CloudBlobDirectory directory = null;
    if (useRootDirectory()) {
      directory = rootDirectory.getSubDirectoryReference(aKey);
    } else {
      assert null != container : "Expecting a non-null container for Azure store object.";
      directory = container.getDirectoryReference(aKey);
    }

    // Return with block blob.
    return directory;
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
    String normalizedKey = normalizeKey(key);

    try {
      // Handle the degenerate cases where the key does not exist or the key is
      // a
      // container.
      //
      if (normalizedKey.equals("/")) {
        // The key refers to a container.
        //
        return new FileMetadata(normalizedKey);
      }

      // Check for the existence of the key.
      //
      if (!exists(normalizedKey)) {
        // Key does not exist as a root blob or as part of a container.
        //
        return null;
      }

      // Download attributes and return file metadata only if the blob exists.
      //
      FileMetadata metadata = null;
      CloudBlockBlob blob = getBlobReference(normalizedKey);

      if (blob.exists()) {
        // The blob exists, so capture the metadata from the blob properties.
        //
        blob.downloadAttributes();
        BlobProperties properties = blob.getProperties();

        metadata = new FileMetadata(key, // Always return denormalized key with
                                         // metadata.
            properties.getLength(), properties.getLastModified().getTime());
      } else {
        metadata = new FileMetadata(normalizedKey);
      }

      // Return to caller with the metadata.
      //
      return metadata;
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
              properties.getLength(), properties.getLastModified().getTime());

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
              properties.getLength(), properties.getLastModified().getTime());

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
    try {
      // Get the source blob and assert its existence.
      //
      CloudBlockBlob srcBlob = getBlobReference(srcKey);
      assert srcBlob.exists() : "Source blob " + srcKey + " does not exist.";

      // Get the destination blob and assert its existence.
      //
      CloudBlockBlob dstBlob = getBlobReference(dstKey);
      assert dstBlob.exists() : "Destination blob " + dstKey
          + " does not exist.";

      // Rename the source blob to the destination blob by copying it to the
      // destination blob then deleting it.
      //
      dstBlob.copyFromBlob(srcBlob);
      srcBlob.delete();
      srcBlob = null;
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
