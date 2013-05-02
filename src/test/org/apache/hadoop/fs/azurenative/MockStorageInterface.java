package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.net.*;
import java.util.*;

import org.apache.commons.io.output.ByteArrayOutputStream;

import com.microsoft.windowsazure.services.blob.client.*;
import com.microsoft.windowsazure.services.core.storage.*;

/**
 * A mock implementation of the Azure Storage interaction
 * layer for unit tests. Just does in-memory storage.
 */
public class MockStorageInterface extends StorageInterface {
  private InMemoryBlockBlobStore backingStore;
  private final ArrayList<PreExistingContainer> preExistingContainers =
      new ArrayList<MockStorageInterface.PreExistingContainer>();

  public InMemoryBlockBlobStore getBackingStore() {
    return backingStore;
  }

  /**
   * Mocks the situation where a container already exists before ASV
   * comes in, i.e. the situation where a user creates a container then
   * mounts ASV on the pre-existing container.
   * @param uri The URI of the container.
   * @param metadata The metadata on the container.
   */
  public void addPreExistingContainer(String uri,
      HashMap<String, String> metadata) {
    preExistingContainers.add(new PreExistingContainer(uri, metadata));
  }

  @Override
  public void setStreamMinimumReadSizeInBytes(int minimumReadSize) {
  }

  @Override
  public void setWriteBlockSizeInBytes(int writeBlockSizeInBytes) {
  }

  @Override
  public void setRetryPolicyFactory(final RetryPolicyFactory retryPolicyFactory) {
  }

  @Override
  public void setTimeoutInMs(int timeoutInMs) {
  }

  @Override
  public void createBlobClient(CloudStorageAccount account) {
    backingStore = new InMemoryBlockBlobStore();
  }

  @Override
  public void createBlobClient(URI baseUri) {
    backingStore = new InMemoryBlockBlobStore();
  }

  @Override
  public void createBlobClient(URI baseUri, StorageCredentials credentials) {
    backingStore = new InMemoryBlockBlobStore();
  }

  @Override
  public CloudBlobDirectoryWrapper getDirectoryReference(String uri)
      throws URISyntaxException, StorageException {
    return new MockCloudBlobDirectoryWrapper(new URI(
        uri.endsWith("/") ? uri : uri + "/"));
  }

  @Override
  public CloudBlobContainerWrapper getContainerReference(String uri)
      throws URISyntaxException, StorageException {
    MockCloudBlobContainerWrapper container = new MockCloudBlobContainerWrapper(uri);
    // Check if we have a pre-existing container with that name, and prime
    // the wrapper with that knowledge if it's found.
    for (PreExistingContainer existing : preExistingContainers) {
      if (uri.equalsIgnoreCase(existing.containerUri)) {
        // We have a pre-existing container. Mark the wrapper as created and
        // make sure we use the metadata for it.
        container.created = true;
        backingStore.setContainerMetadata(existing.containerMetadata);
        break;
      }
    }
    return container;
  }

  @Override
  public CloudBlockBlobWrapper getBlockBlobReference(String blobAddressUri)
      throws URISyntaxException, StorageException {
    return new MockCloudBlockBlobWrapper(new URI(blobAddressUri), null, 0);
  }

  class MockCloudBlobContainerWrapper extends CloudBlobContainerWrapper {
    private boolean created = false;
    private HashMap<String, String> metadata;
    private final String uri;

    public MockCloudBlobContainerWrapper(String uri) {
      this.uri = uri;
    }

    @Override
    public String getName() {
        return uri;
    }

    @Override
    public boolean exists(OperationContext opContext) throws StorageException {
      return created;
    }

    @Override
    public void create(OperationContext opContext) throws StorageException {
      created = true;
      backingStore.setContainerMetadata(metadata);
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return metadata;
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      this.metadata = metadata;
    }

    @Override
    public void downloadAttributes(OperationContext opContext)
        throws StorageException {
      metadata = backingStore.getContainerMetadata();
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException {
      backingStore.setContainerMetadata(metadata);
    }
  }

  private static class PreExistingContainer {
    final String containerUri;
    final HashMap<String, String> containerMetadata;

    public PreExistingContainer(String uri,
        HashMap<String, String> metadata) {
      this.containerUri = uri;
      this.containerMetadata = metadata;
    }
  }

  class MockCloudBlobDirectoryWrapper extends CloudBlobDirectoryWrapper {
    private URI uri;

    public MockCloudBlobDirectoryWrapper(URI uri) {
      this.uri = uri;
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return null;
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return null;
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public Iterable<ListBlobItem> listBlobs(String prefix,
        boolean useFlatBlobListing, EnumSet<BlobListingDetails> listingDetails,
        BlobRequestOptions options, OperationContext opContext)
        throws URISyntaxException, StorageException {
      ArrayList<ListBlobItem> ret = new ArrayList<ListBlobItem>();
      String fullPrefix = prefix == null ?
          uri.toString() :
          new URI(
              uri.getScheme(),
              uri.getAuthority(),
              uri.getPath() + prefix,
              uri.getQuery(),
              uri.getFragment()).toString();
      boolean includeMetadata = listingDetails.contains(BlobListingDetails.METADATA);
      HashSet<String> addedDirectories = new HashSet<String>();
      for (InMemoryBlockBlobStore.ListBlobEntry current : backingStore.listBlobs(
          fullPrefix, includeMetadata)) {
        int indexOfSlash = current.getKey().indexOf('/', fullPrefix.length());
        if (useFlatBlobListing || indexOfSlash < 0) {
          ret.add(new MockCloudBlockBlobWrapper(
              new URI(current.getKey()),
              current.getMetadata(),
              current.getContentLength()));
        } else {
          String directoryName = current.getKey().substring(0, indexOfSlash);
          if (!addedDirectories.contains(directoryName)) {
            addedDirectories.add(current.getKey());
            ret.add(new MockCloudBlobDirectoryWrapper(new URI(
                directoryName + "/")));
          }
        }
      }
      return ret;
    }
  }
  
  class MockCloudBlockBlobWrapper extends CloudBlockBlobWrapper {
    private URI uri;
    private HashMap<String, String> metadata =
        new HashMap<String, String>();
    private BlobProperties properties;

    public MockCloudBlockBlobWrapper(URI uri, HashMap<String, String> metadata,
        int length) {
      this.uri = uri;
      this.metadata = metadata;
      refreshProperties(false);
    }

    private void refreshProperties(boolean getMetadata) {
      if (backingStore.exists(uri.toString())) {
        byte[] content = backingStore.getContent(uri.toString());
        properties = new BlobProperties();
        properties.setLength(content.length);
        properties.setLastModified(
            Calendar.getInstance(TimeZone.getTimeZone("UTC")).getTime());
        if (getMetadata) {
          metadata = backingStore.getMetadata(uri.toString());
        }
      }
    }

    @Override
    public CloudBlobContainer getContainer() throws URISyntaxException,
        StorageException {
      return null;
    }

    @Override
    public CloudBlobDirectory getParent() throws URISyntaxException,
        StorageException {
      return null;
    }

    @Override
    public URI getUri() {
      return uri;
    }

    @Override
    public HashMap<String, String> getMetadata() {
      return metadata;
    }

    @Override
    public void setMetadata(HashMap<String, String> metadata) {
      this.metadata = metadata;
    }

    @Override
    public void copyFromBlob(CloudBlockBlobWrapper sourceBlob,
        OperationContext opContext) throws StorageException, URISyntaxException {
      backingStore.copy(sourceBlob.getUri().toString(), uri.toString());
    }

    @Override
    public void delete(OperationContext opContext) throws StorageException {
      backingStore.delete(uri.toString());
    }

    @Override
    public boolean exists(OperationContext opContext) throws StorageException {
      return backingStore.exists(uri.toString());
    }

    @Override
    public void downloadAttributes(OperationContext opContext)
        throws StorageException {
      refreshProperties(true);
    }

    @Override
    public BlobProperties getProperties() {
      return properties;
    }

    @Override
    public InputStream openInputStream(BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return new ByteArrayInputStream(backingStore.getContent(uri.toString()));
    }

    @Override
    public OutputStream openOutputStream(BlobRequestOptions options,
        OperationContext opContext) throws StorageException {
      return backingStore.upload(uri.toString(), metadata);
    }

    @Override
    public void upload(InputStream sourceStream, OperationContext opContext)
        throws StorageException, IOException {
      ByteArrayOutputStream allContent = new ByteArrayOutputStream();
      allContent.write(sourceStream);
      backingStore.setContent(uri.toString(), allContent.toByteArray(),
          metadata);
      refreshProperties(false);
    }

    @Override
    public void uploadMetadata(OperationContext opContext)
        throws StorageException {
      backingStore.setContent(uri.toString(),
          backingStore.getContent(uri.toString()), metadata);
    }
  }
}
