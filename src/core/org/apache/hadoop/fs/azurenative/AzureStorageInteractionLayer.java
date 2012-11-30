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
abstract class AzureStorageInteractionLayer {
  public abstract void setStreamMinimumReadSizeInBytes(int minimumReadSize);

  public abstract void setWriteBlockSizeInBytes(int writeBlockSizeInBytes);

  public abstract void setTimeoutInMs(int timeoutInMs);

  public abstract void createBlobClient(CloudStorageAccount account);

  public abstract void createBlobClient(URI baseUri);

  public abstract void createBlobClient(URI baseUri,
      StorageCredentials credentials);

  public abstract CloudBlobDirectoryWrapper getDirectoryReference(String uri)
      throws URISyntaxException, StorageException;

  public abstract CloudBlobContainerWrapper getContainerReference(String uri)
      throws URISyntaxException, StorageException;
  
  public abstract CloudBlockBlobWrapper getBlockBlobReference(String blobAddressUri)
      throws URISyntaxException, StorageException;

  public abstract static class CloudBlobDirectoryWrapper
      implements ListBlobItem {
    public abstract URI getUri();
    
    public abstract Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException;
  }

  public abstract static class CloudBlobContainerWrapper {
    public abstract boolean exists(OperationContext opContext)
        throws StorageException;

    public abstract void create(OperationContext opContext)
        throws StorageException;
  }

  public abstract static class CloudBlockBlobWrapper
      implements ListBlobItem {
    public abstract URI getUri();

    public abstract HashMap<String,String> getMetadata();

    public abstract void setMetadata(HashMap<String,String> metadata);

    public abstract void copyFromBlob(CloudBlockBlobWrapper sourceBlob,
        OperationContext opContext)
        throws StorageException, URISyntaxException;

    public abstract void delete(OperationContext opContext)
        throws StorageException;

    public abstract boolean exists(OperationContext opContext)
        throws StorageException;

    public abstract void downloadAttributes(
        OperationContext opContext) throws StorageException;
    
    public abstract BlobProperties getProperties();

    public abstract InputStream openInputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException;

    public abstract OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException;

    public abstract void upload(InputStream sourceStream,
        OperationContext opContext) throws StorageException, IOException;
  }
}
