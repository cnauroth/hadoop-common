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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
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


class AzureNativeFileSystemStore implements NativeFileSystemStore
{
	// Constants local to this class.
	//
	private final String keyConnString = "fs.azure.storageConnectionString";
	@Deprecated
	private final String keyConcurrentConnectionValue   = "fs.azure.concurrentConnection";
	private final String keyConcurrentConnectionValueIn = "fs.azure.concurrentConnection.in";
	private final String keyConcurrentConnectionValueOut= "fs.azure.concurrentConnection.out";
	private final String keyStreamMinReadSize 			= "fs.azure.stream.min.read.size";
	private final String keyWriteBlockSize 				= "fs.azure.write.block.size";
	private final String keyStorageConnectionTimeout	= "fs.azure.storage.timeout";
	
	// Default minimum read size for streams is 64MB.
	//
	private final int DEFAULT_STREAM_MIN_READ_SIZE = 67108864;
	
	// Default write block size is 4MB.
	//
	private final int DEFAULT_WRITE_BLOCK_SIZE = 4194304;
	
	// DEFAULT concurrency for writes and reads.
	//
	private static final int DEFAULT_CONCURRENT_READS = 4;
	private static final int DEFAULT_CONCURRENT_WRITES= 8;
	
	
	
	private CloudStorageAccount account;
	private CloudBlobContainer container;
	private int concurrentValue = 2;
	private int concurrentReads = DEFAULT_CONCURRENT_READS;
	private int concurrentWrites= DEFAULT_CONCURRENT_WRITES;

	public void initialize(URI uri, Configuration conf)
		throws IOException 
	{
		try
		{
			// Retrieve storage account from the connection string in order to create a blob
			// client.  The blob client will be used to retrieve the container if it exists, otherwise
			// a new container is created.
			//
			account = CloudStorageAccount.parse (conf.get(keyConnString));
			CloudBlobClient serviceClient = account.createCloudBlobClient ();
			
			// Set up the minimum stream read block size and the write block size.
			//
			serviceClient.setStreamMinimumReadSizeInBytes(
									conf.getInt(keyStreamMinReadSize,
									DEFAULT_STREAM_MIN_READ_SIZE));
							
			serviceClient.setWriteBlockSizeInBytes(
									conf.getInt(keyWriteBlockSize,
									DEFAULT_WRITE_BLOCK_SIZE));
			
			// The job may want to specify a timeout to use when engaging the
			// storage service. The default is currently 90 seconds. It may
			// be necessary to increase this value for long latencies in larger
			// jobs. If the timeout specified is greater than zero seconds use it,
			// otherwise use the default service client timeout.
			//
			int storageConnectionTimeout = conf.getInt(keyStorageConnectionTimeout, 0);
			if (0 < storageConnectionTimeout)
			{
				serviceClient.setTimeoutInMs(storageConnectionTimeout * 1000);
			}

			// Query the container.
			//
			String containerName = uri.getAuthority ();
			container = serviceClient.getContainerReference (containerName);

			// Check for the existence of the azure storage container.  If it exists use it, otherwise,
			// create a new container.
			//
			if (!container.exists ()) {
				container.create ();
			}

			// Assertion: The container should exist.
			//
			assert container.exists() : "Container " + containerName +
									    " expected but does not exist.";

			// Set the concurrency values equal to the that specified in the configuration
			// file. If it does not exist, set it to the default value calculated as
			// double the number of CPU cores on the client machine. The concurrency
			// value is minimum of double the cores and the read/write property.
			//
			int cpuCores = 2 * Runtime.getRuntime().availableProcessors();
			concurrentValue = conf.getInt (
								keyConcurrentConnectionValue, 
								Math.min (cpuCores, concurrentValue));
									
			concurrentReads	= conf.getInt(
								keyConcurrentConnectionValueIn,
				            	Math.min(cpuCores, DEFAULT_CONCURRENT_READS));
					            	
			concurrentWrites= conf.getInt(
								keyConcurrentConnectionValueOut,
                                Math.min(cpuCores, DEFAULT_CONCURRENT_WRITES));
		}
		catch (Exception e) 
		{
			// Caught exception while attempting to initialize the Azure File System store,
			// re-throw the exception.
			//
			throw new AzureException(e);
		}
	}
	
	@Deprecated /* Replaced by the pushout method. */
	
	public void storeFile (String key, File file, byte [] md5Hash)
		throws IOException
	{
		FileInputStream in = null;
		
		try
		{
			in = new FileInputStream (file);
			CloudBlockBlob blob = container.getBlockBlobReference(key);
			BlobRequestOptions options = new BlobRequestOptions();
			if (null != md5Hash)
			{
				options.setStoreBlobContentMD5 (true);
				blob.getProperties().setContentMD5(new String(md5Hash));
			}
			options.setConcurrentRequestCount(concurrentValue);
			blob.upload(in, file.length(), null, options, null);
			blob.uploadProperties();
		}
		catch (Exception e)
		{
			// Re-throw I/O exception as an Azure storage exception.
			//
			throw new AzureException (e);
		}
		finally
		{
			if (null != in)
			{
				try
				{
					in.close ();
				}
				catch (IOException e)
				{
					// TODO: See if we can do more than eat up the exception
					// TODO: here. Ignoring exceptions is bad style.
				}
			}
		}
	}

	public DataOutputStream pushout (String key)
		throws IOException
	{
		try
		{
			// Get the block blob reference from the store's container and return it.
			//
			CloudBlockBlob blob = container.getBlockBlobReference (key);

			// Set up request options.
			//
			BlobRequestOptions options = new BlobRequestOptions ();
			options.setStoreBlobContentMD5(true);
			options.setConcurrentRequestCount(concurrentWrites);

			// Create the output stream for the Azure blob.
			//
			BlobOutputStream outputStream = blob.openOutputStream(null, options, null);
			
			// Return to caller with DataOutput stream.
			//
			DataOutputStream dataOutStream = new DataOutputStream (outputStream);
			return dataOutStream;
		}
		catch (Exception e)
		{
			// Caught exception while attempting to open the blob output stream. Re-throw
			// as an Azure storage exception.
			//
			throw new AzureException(e);
		}
	}
	

	public void storeEmptyFile(String key)
		throws IOException
	{
		// Upload null byte stream directly to the Azure blob store.
		//
		try
		{
			CloudBlockBlob blob = container.getBlockBlobReference(key);
			blob.upload(new ByteArrayInputStream(new byte[0]), 0);
		}
		catch (Exception e)
		{
			// Caught exception while attempting upload. Re-throw as an Azure storage
			// exception.
			//
			throw new AzureException (e);
		}
	}

	private boolean exists(String key)
		throws Exception
	{
		// Query the container for the list of blobs stored in the container.
		//
		CloudBlobDirectory dir = container.getDirectoryReference(key);
		Iterable<ListBlobItem> objects = dir.listBlobs ();

		// Check to see if the container contains blob items.
		//
		if (null != objects)
		{
			if (objects.iterator().hasNext())
			{
				return true;
			}
		}

		CloudBlockBlob blob = container.getBlockBlobReference(key);
		if (null != blob)
		{
			// Check if the blob exists under the current container
			//
			return blob.exists();
		}

		// Blob does not exist.
		//
		return false;
	}

	public FileMetadata retrieveMetadata(String key)
		throws IOException
	{
		try
		{
			// Handle the degenerate cases where the key does not exist or the key is a 
			// container.
			//
			if (key.equals("/"))
			{
				// The key refers to a container.
				//
				return new FileMetadata(key);
			}

			// Check for the existence of the key.
			//
			if (!exists (key))
			{
				// Key does not exist as a root blob or as part of a container.
				//
				return null;
			}

			// Download attributes and return file metadata only if the blob exists.
			//
			FileMetadata metadata = null;
			CloudBlockBlob blob = container.getBlockBlobReference(key);
			if (blob.exists ())
			{
				// The blob exists, so capture the metadata from the blob properties.
				//
				blob.downloadAttributes ();
				BlobProperties properties = blob.getProperties ();
				metadata = new FileMetadata(
								key,
								properties.getLength(),
								properties.getLastModified ().getTime());
			}
			else
			{
				metadata = new FileMetadata(key);
			}

			// Return to caller with the metadata.
			//
			return  metadata;
		}
		catch (Exception e)
		{
			// Re-throw the exception as an Azure storage exception.
			//
			throw new AzureException(e);
		}
	}

	@Deprecated
	public DataInputStream retrieve(Configuration conf, String key)
		throws IOException
	{
		try
		{
			// Query Azure storage for blob reference and set concurrency
			// options.
			//
			CloudBlob blob = container.getBlockBlobReference (key);
			BlobRequestOptions options = new BlobRequestOptions ();
			options.setConcurrentRequestCount(concurrentReads);
			options.setDisableContentMD5Validation(false);
			
			
			// Create a temporary file backing the output and input file
			// streams. This file will be deleted when the JVM terminates
			// normally.
			//
			File dir = new File(conf.get("fs.azure.buffer.dir"));
		    if (!dir.mkdirs() && !dir.exists())
		    {
		        throw new IOException("Cannot create Azure buffer directory: " + dir);
		    }
		    File f = File.createTempFile("download-", ".tmp", dir);
		    f.deleteOnExit();			
			
			// Create the file output stream and start download of the file.
            // TODO: Determine if the download blocks until the full blob is
		    // TODO: is streamed to the file or will it do the download on a
		    // TODO: background thread.
		    //
			OutputStream outStream = new FileOutputStream(f);
			blob.download(outStream);
			
			// Create the input stream which will be returned to the caller.
			//
			FileInputStream inStream = new FileInputStream (f);
			BufferedInputStream inBufStream = new BufferedInputStream (inStream);
			DataInputStream inDataStream = new DataInputStream(inBufStream);
			
			return inDataStream;
		}
		catch (Exception e)
		{
			// Re-throw as an Azure storage exception.
			//
			throw new AzureException (e);
		}
	}

	public DataInputStream retrieve(String key)
		throws IOException
	{
		try
		{
			CloudBlockBlob blob = container.getBlockBlobReference (key);
			BlobRequestOptions options = new BlobRequestOptions ();
			options.setConcurrentRequestCount(concurrentReads);
			BufferedInputStream inBufStream = new BufferedInputStream (
													blob.openInputStream(null, options, null));
			
			// Return a data input stream.
			//
			DataInputStream inDataStream = new DataInputStream(inBufStream);
			return inDataStream;
		}
		catch (Exception e)
		{
			// Re-throw as an Azure storage exception.
			//
			throw new AzureException(e);
		}
	}


	public DataInputStream retrieve(String key, long startByteOffset)
		throws IOException
	{
		try
		{
			CloudBlockBlob blob = container.getBlockBlobReference (key);
			BlobRequestOptions options = new BlobRequestOptions ();
			options.setConcurrentRequestCount(concurrentReads);

			// Open input stream and seek to the start offset.
			//
			InputStream in = blob.openInputStream(null, options, null);
			
			// Create a data input stream.
			//
		    DataInputStream inDataStream = new DataInputStream(in);
			inDataStream.skip(startByteOffset);
			return inDataStream;
		}
		catch (Exception e)
		{
			// Re-throw as an Azure storage exception.
			//
			throw new AzureException(e);
		}
	}
	
	public PartialListing list(String prefix, final int maxListingCount)
		throws IOException
	{
		return list(prefix, maxListingCount, null);
	}

	public PartialListing list (
							String prefix,
							final int maxListingCount,
							String priorLastKey)
		throws IOException
	{
		return list(prefix, PATH_DELIMITER, maxListingCount, priorLastKey);
	}

	public PartialListing listAll (
							String prefix,
							final int maxListingCount,
							String priorLastKey)
		throws IOException
	{
		return list(prefix, null, maxListingCount, priorLastKey);
	}

	private PartialListing list (
							String prefix,
							String delimiter,
							final int maxListingCount,
							String priorLastKey)
		throws IOException
	{
		try
		{
			if (0 < prefix.length() && !prefix.endsWith(PATH_DELIMITER))
			{
				prefix += PATH_DELIMITER;
			}

			Iterable<ListBlobItem> objects;
			if (prefix.equals("/"))
			{
				objects = container.listBlobs ();
			}
			else
			{
				objects = container.listBlobs (prefix);
			}

			ArrayList<FileMetadata> fileMetadata = new ArrayList<FileMetadata>();
			for (ListBlobItem blobItem : objects)
			{
				// Check that the maximum listing count is not exhausted.
				//
				if (0 < maxListingCount && fileMetadata.size() >= maxListingCount)
				{
					break;
				}

				if (blobItem instanceof CloudBlob)
				{
					CloudBlob blob = (CloudBlob) blobItem;
					BlobProperties properties = blob.getProperties ();

					// TODO: Validate that the following code block actually  makes
					// TODO: sense. Min Wei tagged it as a hack
					priorLastKey = blob.getName();
					FileMetadata metadata = 
								new FileMetadata (
										blob.getName (),
										properties.getLength (),
										properties.getLastModified().getTime ()
										);
					fileMetadata.add (metadata);
				}
				else
				{
					buildUpList (
							(CloudBlobDirectory) blobItem,
							fileMetadata, 
							maxListingCount);
				}
			}
			// TODO: Original code indicated that this may be a hack.
			//
			priorLastKey = null;
			return new PartialListing(
							priorLastKey,
							fileMetadata.toArray(new FileMetadata[]{}),
							0 == fileMetadata.size() ? new String []{} : 
							                           new String [] {prefix});
		}
		catch (Exception e)
		{
			// Re-throw as an Azure storage exception.
			//
			throw new AzureException (e);
		}
	}
	
  	
  	/*
  	 * Build up a metadata list of blobs in an Azure blob directory. This method uses a in-order
  	 * first traversal of blob directory structures to maintain the sorted order of the blob
  	 * names.
  	 * 
  	 * @param dir 	           -- Azure blob directory
  	 * @param list	           -- a list of file metadata objects for each non-directory blob.
  	 * @param maxListingLength -- maximum length of the built up list.
  	 * 
  	 */

  	private void buildUpList (
  					CloudBlobDirectory aCloudBlobDirectory,
  					ArrayList<FileMetadata> aFileMetadataList,
  					final int maxListingCount)
  		throws Exception
  	{
  		// Push the blob directory onto the stack.
  		//
  		AzureLinkedStack<Iterator<ListBlobItem>> dirIteratorStack = 
  										new AzureLinkedStack<Iterator<ListBlobItem>>();
  		
  		Iterable<ListBlobItem> blobItems = aCloudBlobDirectory.listBlobs();
  		Iterator<ListBlobItem> blobItemIterator = blobItems.iterator();
  		
  		// Loop until all directories have been traversed in-order. Loop only the following
  		// conditions are satisfied:
  		//		(1) The stack is not empty, and
  		//      (2) maxListingCount > 0 implies that the number of items in the
  		//          metadata list is less than the max listing count.
  		//
  		while (null != blobItemIterator && 
  			   (maxListingCount <= 0 || aFileMetadataList.size() < maxListingCount))
  		{
  			while (blobItemIterator.hasNext())
  			{
  				// Check if the count of items on the list exhausts the maximum
  				// listing count.
  				//
				if (0 < maxListingCount && aFileMetadataList.size() >= maxListingCount)
				{
					break;
				}
				
				ListBlobItem blobItem = blobItemIterator.next();
				
  				// Add the file metadata to the list if this is not a blob directory
  				// item.
  				//
  				if (blobItem instanceof CloudBlob)
  				{
  					CloudBlob blob = (CloudBlob) blobItem;
  					BlobProperties properties = blob.getProperties();
  					FileMetadata metadata = new FileMetadata (
  													blob.getName (),
  													properties.getLength(),
  													properties.getLastModified().getTime());
  					
  					// Add the metadata to the list.
  					//
  					aFileMetadataList.add(metadata);
  				}
  				else
  				{
  					// This is a directory blob, push the current iterator onto the stack
  					// of iterators and start iterating through the current directory.
  					//
  					dirIteratorStack.push(blobItemIterator);
  					
  					// The current blob item represents the new directory.  Get an iterator
  					// for this directory and continue by iterating through this directory.
  					//
  					blobItems = ((CloudBlobDirectory) blobItem).listBlobs();
  					blobItemIterator = blobItems.iterator();
  				}
  			}
  			
  			// Check if the iterator stack is empty.  If it is set the next blob iterator to
  			// null. This will act as a terminator for the for-loop. Otherwise pop the next
  			// iterator from the stack and continue looping.
  			//
  			if (dirIteratorStack.isEmpty())
  			{
  				blobItemIterator = null;
  			}
  			else
  			{
  				blobItemIterator = dirIteratorStack.pop();
  			}
  		}
  	}
  	
	public void delete(String key)
		throws IOException
	{
		try
		{
			// Get the blob reference an delete it.
			//
			CloudBlockBlob blob = container.getBlockBlobReference(key);
			if (blob.exists ())
			{
				blob.delete();
			}
		}
		catch (Exception e)
		{
			// Re-throw as an Azure storage exception.
			//
			throw new AzureException(e);
		}
	}

	public void rename(String srcKey, String dstKey)
		throws IOException
	{
		try
		{
			// Get the source blob and assert its existence.
			//
			CloudBlockBlob srcBlob = container.getBlockBlobReference(srcKey);
			assert srcBlob.exists () : "Source blob " +
				                       srcKey +
				                       " does not exist.";

			// Get the destination blob and assert its existence.
			//
			CloudBlockBlob dstBlob = container.getBlockBlobReference(dstKey);
			assert dstBlob.exists () : "Destination blob " +
									   dstKey +
									   " does not exist.";

			// Rename the source blob to the destination blob by copying it to the
			// destination blob then deleting it.
			//
			dstBlob.copyFromBlob (srcBlob);
			srcBlob.delete ();
			srcBlob = null;
		}
		catch (Exception e)
		{
			// Re-throw exception as an Azure storage exception.
			//
			throw new AzureException(e);
		}
	}

	public void purge(String prefix)
		throws IOException
	{
		try
		{
			// Get all blob items with the given prefix from the container and delete them.
			//
			Iterable<ListBlobItem> objects = container.listBlobs(prefix);
			for (ListBlobItem blobItem : objects)
			{
				((CloudBlob) blobItem).delete();
			}
		}
		catch (Exception e)
		{
			// Re-throw as an Azure storage exception.
			//
			throw new AzureException(e);
		}
	}

	public void dump ()
		throws IOException
	{
	}
}
