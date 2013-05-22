package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.net.*;
import java.util.*;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.*;

/**
 * This is a very thin layer over the methods exposed by the Windows
 * Azure Storage SDK that we need for ASV implementation. This base
 * class has a real implementation that just simply redirects to
 * the SDK, and a memory-backed one that's used for unit tests.
 * 
 *  IMPORTANT: all the methods here must remain very simple redirects
 *  since code written here can't be properly unit tested.
 */
abstract class StorageInterface {
  /**
   * Sets the minimum read block size to use with this Blob service client.
   * 
   * @param minimumReadSize
   *            The maximum block size, in bytes, for reading from a block blob while using a {@link BlobInputStream}
   *            object, ranging from 512 bytes to 64 MB, inclusive.
   */
  public abstract void setStreamMinimumReadSizeInBytes(int minimumReadSize);

  /**
   * Sets the write block size to use with this Blob service client.
   * 
   * @param writeBlockSizeInBytes
   *            The maximum block size, in bytes, for writing to a block blob while using a {@link BlobOutputStream}
   *            object, ranging from 1 MB to 4 MB, inclusive.
   * 
   * @throws IllegalArgumentException
   *             If <code>writeBlockSizeInBytes</code> is less than 1 MB or greater than 4 MB.
   */
  public abstract void setWriteBlockSizeInBytes(int writeBlockSizeInBytes);

  /**
   * Sets the timeout to use when making requests to the storage service.
   * <p>
   * The server timeout interval begins at the time that the complete request has been received by the service, and the
   * server begins processing the response. If the timeout interval elapses before the response is returned to the
   * client, the operation times out. The timeout interval resets with each retry, if the request is retried.
   * 
   * The default timeout interval for a request made via the service client is 90 seconds. You can change this value on
   * the service client by setting this property, so that all subsequent requests made via the service client will use
   * the new timeout interval. You can also change this value for an individual request, by setting the
   * {@link RequestOptions#timeoutIntervalInMs} property.
   * 
   * If you are downloading a large blob, you should increase the value of the timeout beyond the default value.
   * 
   * @param timeoutInMs
   *            The timeout, in milliseconds, to use when making requests to the storage service.
   */
  public abstract void setTimeoutInMs(int timeoutInMs);

  /**
   * Sets the RetryPolicyFactory object to use when making service requests.
   *
   * @param retryPolicyFactory
   *            the RetryPolicyFactory object to use when making service requests.
   */
  public abstract void setRetryPolicyFactory(final RetryPolicyFactory retryPolicyFactory);

  /**
   * Creates a new Blob service client.
   * 
   */
  public abstract void createBlobClient(CloudStorageAccount account);

  /**
   * Creates an instance of the <code>CloudBlobClient</code> class using the specified Blob service endpoint.
   * 
   * @param baseUri
   *            A <code>java.net.URI</code> object that represents the Blob service endpoint used to create the
   *            client.
   */
  public abstract void createBlobClient(URI baseUri);

  /**
   * Creates an instance of the <code>CloudBlobClient</code> class using the specified Blob service endpoint and
   * account credentials.
   * 
   * @param baseUri
   *            A <code>java.net.URI</code> object that represents the Blob service endpoint used to create the
   *            client.
   * @param credentials
   *            A {@link StorageCredentials} object that represents the account credentials.
   */
  public abstract void createBlobClient(URI baseUri,
      StorageCredentials credentials);

  /**
   * Returns a reference to the specified virtual blob directory.
   * <p>
   * A blob directory simplifies working with a hierarchical organization of blobs. A blob directory is a blob name
   * prefix that can be used to navigate a hierarchy. The prefix may end in a delimiter character, but a delimiter is
   * not required; the directory can end in any character.
   * 
   * @param relativeAddress
   *            A <code>String</code> that represents the name of the virtual blob directory, or the absolute URI to
   *            the virtual blob directory.
   * 
   * @return A wrapper over {@link CloudBlobDirectoryWrapper} object that represents the specified virtual blob directory.
   * 
   * @throws StorageException
   *             If a storage service error occurred.
   * @throws URISyntaxException
   *             If the resource URI is invalid.
   */
  public abstract CloudBlobDirectoryWrapper getDirectoryReference(String uri)
      throws URISyntaxException, StorageException;

  /**
   * Returns a reference to a {@link CloudBlobContainerWrapper} object that represents the cloud blob container
   * for the specified address.
   * 
   * @param containerAddress
   *            A <code>String</code> that represents the name of the container, or the absolute URI to the container.
   * @return A {@link CloudBlobContainerWrapper} object that represents a reference to the cloud blob container.
   * 
   * @throws URISyntaxException
   *             If the resource URI is invalid.
   * @throws StorageException
   *             If a storage service error occurred.
   */
  public abstract CloudBlobContainerWrapper getContainerReference(String uri)
      throws URISyntaxException, StorageException;

  /**
   * Returns a reference to a {@link CloudBlockBlobWrapper} object that represents a block blob.
   * 
   * @param blobAddressUri
   *            A <code>String</code> that represents the name of the blob, or the absolute URI to the blob.
   * 
   * @return A {@link CloudBlockBlobWrapper} object that represents a reference to the specified block blob.
   * 
   * @throws StorageException
   *             If a storage service error occurred.
   * @throws URISyntaxException
   *             If the resource URI is invalid.
   */
  public abstract CloudBlockBlobWrapper getBlockBlobReference(String blobAddressUri)
      throws URISyntaxException, StorageException;

  /**
   * A thin wrapper over the {@link CloudBlobDirectory} class that simply redirects calls
   * to the real object except in unit tests.
   */
  public abstract static class CloudBlobDirectoryWrapper
      implements ListBlobItem {
    /**
     * Returns the URI for this directory.
     * 
     * @return A <code>java.net.URI</code> object that represents the URI for this directory.
     */
    public abstract URI getUri();

    /**
     * Returns an enumerable collection of blob items whose names begin with the specified prefix, using the specified
     * flat or hierarchical option, listing details options, request options, and operation context.
     * 
     * @param prefix
     *            A <code>String</code> that represents the prefix of the blob name.
     * @param useFlatBlobListing
     *            <code>true</code> to indicate that the returned list will be flat; <code>false</code> to indicate that
     *            the returned list will be hierarchical.
     * @param listingDetails
     *            A <code>java.util.EnumSet</code> object that contains {@link BlobListingDetails} values that indicate
     *            whether snapshots, metadata, and/or uncommitted blocks are returned. Committed blocks are always
     *            returned.
     * @param options
     *            A {@link BlobRequestOptions} object that specifies any additional options for the request. Specifying
     *            <code>null</code> will use the default request options from the associated service client (
     *            {@link CloudBlobClient}).
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return An enumerable collection of {@link ListBlobItem} objects that represent the block items whose names begin
     *         with the specified prefix in this directory.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     *             If the resource URI is invalid.
     */
    public abstract Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException;
  }

  /**
   * A thin wrapper over the {@link CloudBlobContainer} class that simply redirects calls
   * to the real object except in unit tests.
   */
  public abstract static class CloudBlobContainerWrapper {
    /**
     * Returns the name of the container.
     * 
     * @return A <code>String</code> that represents the name of the container.
     */
    public abstract String getName();

    /**
     * Returns a value that indicates whether the container exists, using the specified operation
     * context.
     * 
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return <code>true</code> if the container exists, otherwise <code>false</code>.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract boolean exists(OperationContext opContext)
        throws StorageException;

    /**
     * Returns the metadata for the container.
     *
     * @return A <code>java.util.HashMap</code> object that represents the metadata for the container.
     */
    public abstract HashMap<String,String> getMetadata();

    /**
     * Sets the metadata for the container.
     *
     * @param metadata
     *            A <code>java.util.HashMap</code> object that represents the metadata being assigned to the container.
     */
    public abstract void setMetadata(HashMap<String,String> metadata);

    /**
     * Downloads the container's attributes, which consist of metadata and properties, using the specified
     * operation context.
     *
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract void downloadAttributes(
        OperationContext opContext) throws StorageException;

    /**
     * Uploads the container's metadata using the specified operation context.
     *
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract void uploadMetadata(OperationContext opContext)
        throws StorageException;

    /**
     * Creates the container using the specified operation context.
     *
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract void create(OperationContext opContext)
        throws StorageException;
  }

  /**
   * A thin wrapper over the {@link CloudBlockBlob} class that simply redirects calls
   * to the real object except in unit tests.
   */
  public abstract static class CloudBlockBlobWrapper
      implements ListBlobItem {
    /**
     * Returns the URI for this blob.
     * 
     * @return A <code>java.net.URI</code> object that represents the URI for the blob.
     */
    public abstract URI getUri();

    /**
     * Returns the metadata for the blob.
     * 
     * @return A <code>java.util.HashMap</code> object that represents the metadata for the blob.
     */
    public abstract HashMap<String,String> getMetadata();

    /**
     * Sets the metadata for the blob.
     * 
     * @param metadata
     *            A <code>java.util.HashMap</code> object that contains the metadata being assigned to the blob.
     */
    public abstract void setMetadata(HashMap<String,String> metadata);

    /**
     * Copies an existing blob's contents, properties, and metadata to this instance of the <code>CloudBlob</code>
     * class, using the specified operation context.
     * 
     * @param sourceBlob
     *            A <code>CloudBlob</code> object that represents the source blob to copy.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     * @throws URISyntaxException
     * 
     */
    public abstract void copyFromBlob(CloudBlockBlobWrapper sourceBlob,
        OperationContext opContext)
        throws StorageException, URISyntaxException;

    /**
     * Deletes the blob using the specified operation context.
     * <p>
     * A blob that has snapshots cannot be deleted unless the snapshots are also deleted. If a blob has snapshots, use
     * the {@link DeleteSnapshotsOption#DELETE_SNAPSHOTS_ONLY} or {@link DeleteSnapshotsOption#INCLUDE_SNAPSHOTS} value
     * in the <code>deleteSnapshotsOption</code> parameter to specify how the snapshots should be handled when the blob
     * is deleted.
     * 
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract void delete(OperationContext opContext)
        throws StorageException;

    /**
     * Checks to see if the blob exists, using the specified operation context.
     * 
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return <code>true</code> if the blob exists, other wise <code>false</code>.
     * 
     * @throws StorageException
     *             f a storage service error occurred.
     */
    public abstract boolean exists(OperationContext opContext)
        throws StorageException;

    /**
     * Populates a blob's properties and metadata using the specified operation context.
     * <p>
     * This method populates the blob's system properties and user-defined metadata. Before reading a blob's properties
     * or metadata, call this method or its overload to retrieve the latest values for the blob's properties and
     * metadata from the Windows Azure storage service.
     * 
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract void downloadAttributes(
        OperationContext opContext) throws StorageException;

    /**
     * Returns the blob's properties.
     * 
     * @return A {@link BlobProperties} object that represents the properties of the blob.
     */
    public abstract BlobProperties getProperties();

    /**
     * Opens a blob input stream to download the blob using the specified operation context.
     * <p>
     * Use {@link CloudBlobClient#setStreamMinimumReadSizeInBytes} to configure the read size.
     * 
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return An <code>InputStream</code> object that represents the stream to use for reading from the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract InputStream openInputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException;

    /**
     * Creates and opens an output stream to write data to the block blob using the specified 
     * operation context.
     * 
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @return A {@link BlobOutputStream} object used to write data to the blob.
     * 
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException;

    /**
     * Uploads the source stream data to the blob, using the specified operation context.
     * 
     * @param sourceStream
     *            An <code>InputStream</code> object that represents the input stream to write to the block blob.
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     * 
     * @throws IOException
     *             If an I/O error occurred.
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract void upload(InputStream sourceStream,
        OperationContext opContext) throws StorageException, IOException;

    /**
     * Uploads the blob's metadata to the storage service using the specified lease ID, request options, and operation
     * context.
     *
     * @param opContext
     *            An {@link OperationContext} object that represents the context for the current operation. This object
     *            is used to track requests to the storage service, and to provide additional runtime information about
     *            the operation.
     *
     * @throws StorageException
     *             If a storage service error occurred.
     */
    public abstract void uploadMetadata(OperationContext opContext)
        throws StorageException;
  }
}
