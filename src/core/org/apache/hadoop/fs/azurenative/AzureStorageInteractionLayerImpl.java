package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.net.*;
import java.util.*;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.*;

/**
 * A real implementation of the Azure interaction layer that
 * just redirects calls to the Windows Azure storage SDK.
 */
class AzureStorageInteractionLayerImpl extends AzureStorageInteractionLayer {
  private CloudBlobClient serviceClient;

  @Override
  public void setStreamMinimumReadSizeInBytes(int minimumReadSize) {
    serviceClient.setStreamMinimumReadSizeInBytes(minimumReadSize);
  }

  @Override
  public void setWriteBlockSizeInBytes(int writeBlockSizeInBytes) {
    serviceClient.setWriteBlockSizeInBytes(writeBlockSizeInBytes);
  }

  @Override
  public void setTimeoutInMs(int timeoutInMs) {
    serviceClient.setTimeoutInMs(timeoutInMs);
  }

  @Override
  public void createBlobClient(CloudStorageAccount account) {
    serviceClient = account.createCloudBlobClient();
  }

  @Override
  public void createBlobClient(URI baseUri) {
    serviceClient = new CloudBlobClient(baseUri);
  }

  @Override
  public void createBlobClient(URI baseUri,
      StorageCredentials credentials) {
    serviceClient = new CloudBlobClient(baseUri, credentials);
  }

  @Override
  public CloudBlobDirectoryWrapper getDirectoryReference(String uri)
      throws URISyntaxException, StorageException {
    return new CloudBlobDirectoryWrapperImpl(
        serviceClient.getDirectoryReference(uri));
  }

  @Override
  public CloudBlobContainerWrapper getContainerReference(String uri)
      throws URISyntaxException, StorageException {
    return new CloudBlobContainerWrapperImpl(
        serviceClient.getContainerReference(uri));
  }

  @Override
  public CloudBlockBlobWrapper getBlockBlobReference(String blobAddressUri)
      throws URISyntaxException, StorageException {
    return new CloudBlockBlobWrapperImpl(
        serviceClient.getBlockBlobReference(blobAddressUri));
  }
  
  /**
   * This iterator wraps every ListBlobItem as they come from the
   * listBlobs() calls to their proper wrapping objects.
   */
  private static class WrappingIterator implements Iterator<ListBlobItem> {
    private final Iterator<ListBlobItem> present;
    
    public WrappingIterator(Iterator<ListBlobItem> present) {
      this.present = present;
    }
    
    public static Iterable<ListBlobItem> Wrap(
        final Iterable<ListBlobItem> present) {
      return new Iterable<ListBlobItem>() {
        @Override
        public Iterator<ListBlobItem> iterator() {
          return new WrappingIterator(present.iterator());
        }
      };
    }

    @Override
    public boolean hasNext() {
      return present.hasNext();
    }

    @Override
    public ListBlobItem next() {
      ListBlobItem unwrapped = present.next();
      if (unwrapped instanceof CloudBlobDirectory) {
        return new CloudBlobDirectoryWrapperImpl(
            (CloudBlobDirectory)unwrapped);
      } else if (unwrapped instanceof CloudBlockBlob) {
        return new CloudBlockBlobWrapperImpl(
            (CloudBlockBlob)unwrapped);
      } else {
        return unwrapped;
      }
    }

    @Override
    public void remove() {
      present.remove();
    }
  }

  static class CloudBlobDirectoryWrapperImpl extends CloudBlobDirectoryWrapper {
    private final CloudBlobDirectory directory;

    public CloudBlobDirectoryWrapperImpl(CloudBlobDirectory directory) {
      this.directory = directory;
    }

    @Override
    public URI getUri() {
      return directory.getUri();
    }

    @Override
    public Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException {
      return WrappingIterator.Wrap(
          directory.listBlobs(prefix, useFlatBlobListing,
          listingDetails, options, opContext));
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return directory.getContainer();
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return directory.getParent();
    }
  }
  
  static class CloudBlobContainerWrapperImpl extends CloudBlobContainerWrapper {
    private final CloudBlobContainer container;

    public CloudBlobContainerWrapperImpl(CloudBlobContainer container) {
      this.container = container;
    }

    @Override
    public boolean exists(OperationContext opContext) throws StorageException {
      return container.exists(null, opContext);
    }
    
    @Override
    public void create(OperationContext opContext) throws StorageException {
      container.create(null, opContext);
    }
  }

  static class CloudBlockBlobWrapperImpl extends CloudBlockBlobWrapper {
    private final CloudBlockBlob blob;
    
    public URI getUri() {
      return blob.getUri();
    }

    public CloudBlockBlobWrapperImpl(CloudBlockBlob blob) {
      this.blob = blob;
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return blob.getMetadata();
    }

    @Override
    public void copyFromBlob(CloudBlockBlobWrapper sourceBlob,
        OperationContext opContext)
        throws StorageException, URISyntaxException {
      blob.copyFromBlob(((CloudBlockBlobWrapperImpl)sourceBlob).blob,
          null, null, null, opContext);
    }

    @Override
    public void delete(OperationContext opContext)
        throws StorageException {
      blob.delete(DeleteSnapshotsOption.NONE, null, null, opContext);
    }

    @Override
    public boolean exists(OperationContext opContext)
        throws StorageException {
      return blob.exists(null, null, opContext);
    }

    @Override
    public void downloadAttributes(
        OperationContext opContext) throws StorageException {
      blob.downloadAttributes(null, null, opContext);
    }

    @Override
    public BlobProperties getProperties() {
      return blob.getProperties();
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      blob.setMetadata(metadata);
    }

    @Override
    public InputStream openInputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return blob.openInputStream(null, options, opContext);
    }

    @Override
    public OutputStream openOutputStream(
        BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return blob.openOutputStream(null, options, opContext);
    }

    @Override
    public void upload(InputStream sourceStream, OperationContext opContext)
        throws StorageException, IOException {
      blob.upload(sourceStream, 0, null, null, opContext);
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return blob.getContainer();
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return blob.getParent();
    }
  }
}
