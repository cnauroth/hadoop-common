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

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

/**
 * <p>
 * A {@link FileSystem} for reading and writing files stored on <a
 * href="http://store.azure.com/">Windows Azure</a>. This implementation is
 * blob-based and stores files on Azure in their native form so they can be read
 * by other Azure tools.
 * </p>
 */
public class NativeAzureFileSystem extends FileSystem {

  public static final Log LOG = LogFactory.getLog(NativeAzureFileSystem.class);

  static final String PATH_DELIMITER = Path.SEPARATOR;
  static final String AZURE_TEMP_FOLDER = "_$azuretmpfolder$";

  private static final String FOLDER_SUFFIX = "_$folder$";
  private static final String keyMaxRetries = "fs.azure.maxRetries";
  private static final String keySleepTime = "fs.azure.sleepTimeSeconds";

  private static final int AZURE_MAX_FLUSH_RETRIES = 3;
  private static final int AZURE_MAX_READ_RETRIES = 3;
  private static final int AZURE_BACKOUT_TIME = 100;
  private static final int AZURE_LIST_ALL = -1;

  private static final long MAX_AZURE_BLOCK_SIZE_DEFAULT = 64 * 1024 * 1024L;

  private static int DEFAULT_MAX_RETRIES = 4;
  private static int DEFAULT_SLEEP_TIME_SECONDS = 10;

  private class NativeAzureFsInputStream extends FSInputStream {
    private InputStream in;
    private final String key;
    private long pos = 0;

    public NativeAzureFsInputStream(DataInputStream in, String key) {
      this.in = in;
      this.key = key;
    }

    /*
     * Reads the next byte of data from the input stream. The value byte is
     * returned as an integer in the range 0 to 255. If no byte is available
     * because the end of the stream has been reached, the value -1 is returned.
     * This method blocks until input data is available, the end of the stream
     * is detected, or an exception is thrown.
     * 
     * @returns int An integer corresponding to the byte read.
     */
    @Override
    public synchronized int read() throws IOException {
      int result = 0;
      for (int readAttempts = 0; readAttempts < AZURE_MAX_READ_RETRIES; readAttempts++) {
        try {
          result = in.read();
          if (result != -1) {
            pos++;
            if (statistics != null) {
              statistics.incrementBytesRead(1);
            }
          }

          // The read completed successfully, break to return with the
          // result.
          //
          break;
        } catch (IOException e) {
          // Log the exception and print the stack trace.
          //
          LOG.warn("Caught I/O exception on read" + e);
          if (AZURE_MAX_READ_RETRIES > readAttempts) {
            // Delay the next attempt with a linear back out.
            //
            try {
              Thread.sleep(readAttempts * AZURE_BACKOUT_TIME);
            } catch (InterruptedException e1) {
              // Thread interrupted from sleep. Continue with flush
              // processing.
              LOG.warn("Thread interrupted from sleep with exception" + e);
              LOG.warn("Continuing read operation...");
            }
          } else {
            // Read attempts exhausted so dump the current stack and re-throw
            // the exception to indicated that the flush failed.
            //
            e.printStackTrace();
            throw new IOException(e);
          }
        }
      }

      // Return to the caller with the result.
      //
      return result;
    }

    /*
     * Reads up to len bytes of data from the input stream into an array of
     * bytes. An attempt is made to read as many as len bytes, but a smaller
     * number may be read. The number of bytes actually read is returned as an
     * integer. This method blocks until input data is available, end of file is
     * detected, or an exception is thrown. If len is zero, then no bytes are
     * read and 0 is returned; otherwise, there is an attempt to read at least
     * one byte. If no byte is available because the stream is at end of file,
     * the value -1 is returned; otherwise, at least one byte is read and stored
     * into b.
     * 
     * @param b -- the buffer into which data is read
     * 
     * @param off -- the start offset in the array b at which data is written
     * 
     * @param len -- the maximum number of bytes read
     * 
     * @ returns int The total number of byes read into the buffer, or -1 if
     * there is no more data because the end of stream is reached.
     */
    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
      int result = 0;

      for (int readAttempts = 0; readAttempts < AZURE_MAX_READ_RETRIES; readAttempts++) {
        try {
          result = in.read(b, off, len);
          if (result > 0) {
            pos += result;
          }
          if (statistics != null && result > 0) {
            statistics.incrementBytesRead(result);
          }

          // The read completed successfully, break to return with the
          // result.
          //
          break;
        } catch (IOException e) {
          // Log the exception and print the stack trace.
          //
          LOG.warn("Caught I/O exception on read" + e);
          if (AZURE_MAX_READ_RETRIES > readAttempts) {
            // Delay the next attempt with a linear back out.
            //
            try {
              Thread.sleep(readAttempts * AZURE_BACKOUT_TIME);
            } catch (InterruptedException e1) {
              // Thread interrupted from sleep. Continue with flush
              // processing.
              LOG.warn("Thread interrupted from sleep with exception" + e);
              LOG.warn("Continuing read operation...");
            }
          } else {
            // Read attempts exhausted so dump the current stack and re-throw
            // the exception to indicated that the flush failed.
            //
            e.printStackTrace();
            throw new IOException(e);
          }
        }
      }

      // Return to the caller with the result.
      //
      return result;
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
      in.close();
      in = store.retrieve(key, pos);
      this.pos = pos;
    }

    @Override
    public synchronized long getPos() throws IOException {
      return pos;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      return false;
    }
  }

  private class NativeAzureFsOutputStream extends OutputStream {
    private String key;
    private String keyEncoded;
    private OutputStream out;
    private ByteArrayOutputStream buffer;
    private AzureRingBuffer<ByteArrayOutputStream> outRingBuffer;

    private static final int S_DEFAULT_RINGBUFFER_CAPACITY = 4;
    private static final int S_STREAM_BUFFERSIZE = 4 * 1024 * 1024; // 4MB
                                                                    // buffers.

    public NativeAzureFsOutputStream(OutputStream out, String aKey,
        String anEncodedKey) throws IOException {
      // Check input arguments. The output stream should be non-null and the
      // keys
      // should be valid strings.
      //
      if (null == out) {
        throw new IllegalArgumentException(
            "Illegal argument: the output stream is null.");
      }

      if (null == aKey || 0 == aKey.length()) {
        throw new IllegalArgumentException(
            "Illegal argument the key string is null or empty");
      }

      if (null == anEncodedKey || 0 == anEncodedKey.length()) {
        throw new IllegalArgumentException(
            "Illegal argument the encoded key string is null or empty");
      }

      // Initialize the member variables with the incoming parameters.
      //
      this.out = out;
      buffer = new ByteArrayOutputStream(S_STREAM_BUFFERSIZE);

      setKey(aKey);
      setEncodedKey(anEncodedKey);

      // Create and initialize the ring buffer collection of output byte
      // streams.
      //
      outRingBuffer = new AzureRingBuffer<ByteArrayOutputStream>(
          S_DEFAULT_RINGBUFFER_CAPACITY);
    }

    @Override
    public void flush() throws IOException {
      // There may be storage or I/O exceptions on the flush. Loop to retry any
      // failed
      // flushes and exit when flush is successful.
      //
      boolean isFlushed = false;
      for (int flushAttempts = 0; !isFlushed
          && flushAttempts < AZURE_MAX_FLUSH_RETRIES; flushAttempts++) {
        // Make an attempt to flush the current outstanding buffers out to Azure
        // storage.
        //
        try {
          // Flush any remaining buffers in the output stream.
          //
          out.flush();

          // Iterate through the ring buffer from head to tail writing
          // the byte array output stream buffers to the stream and
          // removing them.
          //
          while (!outRingBuffer.isEmpty()) {
            ByteArrayOutputStream outByteStream = outRingBuffer.remove();
            outByteStream.writeTo(out);
          }

          // Assertion: At this point the ring buffer should be exhausted.
          //
          assert outRingBuffer.isEmpty() : "Empty ring buffer expected after writing all "
              + "contents to output stream.";

          // Flush the output stream to ensure all the newly added
          // buffers are written to the Azure blob.
          //
          out.flush();

          // The current buffers were successfully flushed to the remote Azure
          // store.
          //
          isFlushed = true;
        } catch (IOException e) {
          // Log the exception and print the stack trace.
          //
          LOG.warn("Caught I/O exception" + e);
          if (AZURE_MAX_FLUSH_RETRIES > flushAttempts) {
            // Delay the next attempt with a linear back out.
            //
            try {
              Thread.sleep(flushAttempts * AZURE_BACKOUT_TIME);
            } catch (InterruptedException e1) {
              // Thread interrupted from sleep. Continue with flush
              // processing.
              LOG.warn("Thread interrupte from sleep with exception" + e);
              LOG.warn("Continuing flush operation...");
            }
          } else {
            // Flush attempts exhausted so dump the current stack and re-throw
            // the exception to indicated that the flush failed.
            //
            e.printStackTrace();
            throw new IOException(e);
          }
        }
      }

      // Create a new byte array output buffer stream for subsequent writes.
      //
      if (null == buffer) {
        buffer = new ByteArrayOutputStream(S_STREAM_BUFFERSIZE);
      }
    }

    @Override
    public synchronized void close() throws IOException {
      // Check if the stream is already closed.
      //
      if (null == buffer) {
        assert null == outRingBuffer : "Expected no ring buffer for a stream that's "
            + "already closed.";

        // Return to caller because the stream appears to have been
        // closed already.
        //
        return;
      }

      // Flush all current output buffer streams.
      //
      this.flush();

      // Assertion: The ring buffer should be empty as a result of the flush.
      //
      assert (outRingBuffer.isEmpty()) : "Unexpected non-empty ring buffer after flush.";

      // Write out the current contents of the current buffer to the stream and
      // flush again.
      //
      if (null != buffer && 0 < buffer.size()) {
        // Write the current byte array output buffer stream to the output
        // stream.
        //
        buffer.writeTo(out);

        // Flush the output stream again. This will be a no-op if the current
        // buffer
        // was zero length.
        //
        out.flush();
      }

      // Close the output stream and decode the key for the output stream
      // before returning to the caller.
      //
      out.close();
      restoreKey();

      // GC the ring buffer and the current buffer.
      //
      outRingBuffer = null;
      buffer = null;
    }

    /**
     * Writes the specified byte to this output stream. The general contract for
     * write is that one byte is written to the output stream. The byte to be
     * written is the eight low-order bits of the argument b. The 24 high-order
     * bits of b are ignored.
     * 
     * @param b 32-bit integer of block of 4 bytes
     */
    @Override
    public void write(int b) throws IOException {

      // if full, add the current stream to ring buffer
      // and create a new stream
      processCurrentStreamIfFull();

      // Ignore the high-order 24 bits and write low-order byte to the
      // current offset.
      //
      // m_buffer.write(intToByteArray(b), 0, 1);
      buffer.write(b);
    }

    /**
     * Writes b.length bytes from the specified byte array to this output
     * stream. The general contract for write(b) is that it should have exactly
     * the same effect as the call write(b, 0, b.length).
     * 
     * @param b Block of bytes to be written to the output stream.
     */
    @Override
    public void write(byte[] b) throws IOException {
      // Write the byte array to the output byte array stream.
      //
      buffer.write(b, 0, b.length);
    }

    // Adds the current stream buffer to ring buffer
    // if it is full. Also creates a new stream buffer
    // for streaming new writes.
    // TODO: need a better name for this
    private void processCurrentStreamIfFull() throws IOException {
      if (buffer.size() >= S_STREAM_BUFFERSIZE) {
        addCurrentStreamToRingBuffer();

        // Create a new byte array output stream.
        //
        buffer = new ByteArrayOutputStream(S_STREAM_BUFFERSIZE);
      }
    }

    // adds the current stream to ring buffer
    private void addCurrentStreamToRingBuffer() throws IOException {
      // Add the current byte array output stream to the ring buffer.
      //
      if (!outRingBuffer.offer(buffer)) {
        // The ring buffer is full. Flush contents of the ring buffer to the
        // output stream.
        //
        // Block flushing any remaining buffer in the output stream out to Azure
        // storage. Typically this should be a no-op since most of the buffer
        // would
        // have been written out while filling the ring buffer.
        //
        // TODO Rather than wait for flush completion it may be better
        // TODO to get notification that the first write is completed.
        //
        out.flush();

        // Iterate through the ring buffer from head to tail writing
        // the byte array output stream buffers to the stream and
        // removing them.
        //
        while (!outRingBuffer.isEmpty()) {
          ByteArrayOutputStream outByteStream = outRingBuffer.remove();
          outByteStream.writeTo(out);
        }

        // Assertion: At this point the ring buffer should be empty since all
        // the outstanding buffers have been flushed to the output stream
        //
        assert outRingBuffer.isEmpty() : "Ring buffer containing residual byte array streams unexpected.";

        // Add the current byte array output stream to the ring buffer.
        //
        outRingBuffer.add(buffer);
      }
    }

    /**
     * Writes <code>len</code> from the specified byte array starting at offset
     * <code>off</code> to the output stream. The general contract for write(b,
     * off, len) is that some of the bytes in the array <code>
     * b</code b> are written to the output stream in order; element
     * <code>b[off]</code> is the first byte written and
     * <code>b[off+len-1]</code> is the last byte written by this operation.
     * 
     * @param b Byte array to be written.
     * @param off Write this offset in stream.
     * @param len Number of bytes to be written.
     */
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      // Byte array must be non-null.
      //
      if (null == b) {
        // Raise null pointer exception.
        //
        throw new NullPointerException(
            "Attempt to write from a null byte block.");
      }
      // Check if offset and length don't overrun byte array boundary
      // before attempting to write to the byte block.
      //
      if (0 > off || 0 > len || off + len > b.length) {
        // Buffer overrun on the byte array b.
        //
        throw new IndexOutOfBoundsException(
            "Attempt to write byte block with invalid offset and length.");
      }

      // Assertion: The byte array output stream should exist.
      //
      assert (null != buffer) : "Unexpected null byte array output stream (1).";

      // if the buffer is full, add it to ring buffer
      // and create a new buffer
      processCurrentStreamIfFull();

      if (buffer.size() + len <= S_STREAM_BUFFERSIZE) {
        // The whole byte sub-array can fit into the current stream buffer.
        //
        buffer.write(b, off, len);
      } else {
        // buffer size should always be less than S_STREAM_BUFFERSIZE
        // as we are calling processCurrentStreamIfFull
        // before we reach here
        assert (buffer.size() < S_STREAM_BUFFERSIZE) : "Unexpected buffer size:"
            + buffer.size();

        // The byte sub-array writes past the end of the buffer stream.
        // Write what fits and add the current byte array out put buffer
        // stream to the ring buffer.
        //
        int lenPartial = 0;
        lenPartial = buffer.size() + len - S_STREAM_BUFFERSIZE;

        buffer.write(b, off, len - lenPartial);

        // the remaining length in b that needs to be written
        int remainingLength = lenPartial;

        // the offset in b from which bytes should be written
        int newOffset = off + len - lenPartial;

        // each time the loop runs, a full buffer is added to m_outRingBuffer
        // and a new buffer is created. So, if a buffer is not full, the loop
        // should not run
        // as there is no need to add m_buffer to ring buffer yet
        while (buffer.size() >= S_STREAM_BUFFERSIZE) {
          // current stream would be full because of the previous write
          processCurrentStreamIfFull();

          if (remainingLength > 0) {
            int streamBufferLength = Math.min(remainingLength,
                S_STREAM_BUFFERSIZE);
            buffer.write(b, newOffset, streamBufferLength);

            // update the remaining length and newOffset
            remainingLength = remainingLength - streamBufferLength;
            newOffset = newOffset + streamBufferLength;
          } else {
            break;
          }
        }

      }// end of else

    }

    /**
     * Get the blob name.
     * 
     * @return String Blob name.
     */
    public String getKey() {
      return key;
    }

    /**
     * Set the blob name.
     * 
     * @param key Blob name.
     */
    public void setKey(String key) {
      this.key = key;
    }

    /**
     * Get the blob name.
     * 
     * @return String Blob name.
     */
    public String getEncodedKey() {
      return keyEncoded;
    }

    /**
     * Set the blob name.
     * 
     * @param anEncodedKey Blob name.
     */
    public void setEncodedKey(String anEncodedKey) {
      this.keyEncoded = anEncodedKey;
    }

    /**
     * Restore the original key name from the m_key member variable. Note: The
     * output file stream is created with an encoded blob store key to guarantee
     * load balancing on the front end of the Azure storage partition servers.
     * The create also includes the name of the original key value which is
     * stored in the m_key member variable. This method should only be called
     * when the stream is closed.
     * 
     * @param anEncodedKey Encoding of the original key stored in m_key member.
     */
    private void restoreKey() throws IOException {
      store.rename(getEncodedKey(), getKey());
    }
  }

  private URI uri;
  private NativeFileSystemStore store;
  private Path workingDir;
  private long blockSize;

  public NativeAzureFileSystem() {
    // set store in initialize()
  }

  public NativeAzureFileSystem(NativeFileSystemStore store) {
    this.store = store;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    if (store == null) {
      store = createDefaultStore(conf);
    }
    store.initialize(uri, conf);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/user", System.getProperty("user.name"))
        .makeQualified(this);
    this.blockSize = conf.getLong("fs.azure.block.size", MAX_AZURE_BLOCK_SIZE_DEFAULT);
  }

  private static NativeFileSystemStore createDefaultStore(Configuration conf) {
    AzureNativeFileSystemStore store = new AzureNativeFileSystemStore();

    // TODO: Remove literals to improve portability and facilitate future
    // TODO: future changes to constants.
    // Get the base policy from the configuration file.
    //
    RetryPolicy basePolicy = RetryPolicies.retryUpToMaximumCountWithFixedSleep(
        conf.getInt(keyMaxRetries, DEFAULT_MAX_RETRIES),
        conf.getLong(keySleepTime, DEFAULT_SLEEP_TIME_SECONDS),
        TimeUnit.SECONDS);

    // Set up the exception policy map.
    //
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap = new HashMap<Class<? extends Exception>, RetryPolicy>();

    // Add base policies to the exception policy map.
    //
    exceptionToPolicyMap.put(IOException.class, basePolicy);
    exceptionToPolicyMap.put(AzureException.class, basePolicy);

    // Create a policy for the storeFile method by adding it to the method name
    // to
    // policy map.
    //
    RetryPolicy methodPolicy = RetryPolicies.retryByException(
        RetryPolicies.TRY_ONCE_THEN_FAIL, exceptionToPolicyMap);
    Map<String, RetryPolicy> methodNameToPolicyMap = new HashMap<String, RetryPolicy>();
    methodNameToPolicyMap.put("storeFile", methodPolicy);

    return (NativeFileSystemStore) RetryProxy.create(
        NativeFileSystemStore.class, store, methodNameToPolicyMap);
  }

  private static String pathToKey(Path path) {
    // Convert the path to a URI to parse the scheme, the authority, and the
    // path from the path object.
    //
    URI tmpUri = path.toUri();
    String pathUri = tmpUri.getPath();

    // The scheme and authority is valid. If the path does not exist add a "/"
    // separator to list the root of the container.
    //
    Path newPath = path;
    if ("".equals(pathUri)) {
      newPath = new Path(tmpUri.toString() + Path.SEPARATOR);
    }

    // Verify path is absolute if the path refers to a windows drive scheme.
    //
    if (!newPath.isAbsolute()) {
      throw new IllegalArgumentException("Path must be absolute: " + path);
    }

    String key = newPath.toUri().getPath();
    if (key.length() == 1) {
      return key;
    } else {
      return key.substring(1); // remove initial slash
    }
  }

  private static Path keyToPath(String key) {
    if (key.equals("/")) {
      return new Path("/"); // container
    }
    return new Path("/" + key);
  }

  private Path makeAbsolute(Path path) {
    if (path.isAbsolute()) {
      return path;
    }
    return new Path(workingDir, path);
  }

  /** This optional operation is not yet supported. */
  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
      throws IOException {
    throw new IOException("Not supported");
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission,
      boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating file " + f.toString());
    }
    if (exists(f) && !overwrite) {
      throw new IOException("File already exists:" + f);
    }

    // Open the output blob stream based on the encoded key.
    //
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    String keyEncoded = encodeKey(key);

    // The key is encoded to point to a common container at the storage server.
    // This reduces the number of splits on the server side when load balancing.
    // Ingress to Azure storage can take advantage of earlier splits. We remove
    // the root path to the key and prefix a random GUID to the tail (or leaf
    // filename) of the key. Keys are thus broadly and randomly distributed over
    // a single container to ease load balancing on the storage server. When the
    // blob is committed it is renamed to its earlier key. Uncommitted blocks
    // are not cleaned up and we leave it to Azure storage to garbage collect
    // these
    // blocks.
    //
    OutputStream bufOutStream = new NativeAzureFsOutputStream(store.pushout(
        keyEncoded, permission), key, keyEncoded);

    // Construct the data output stream from the buffered output stream.
    //
    FSDataOutputStream fsOut = new FSDataOutputStream(bufOutStream, statistics);

    // Return data output stream to caller.
    //
    return fsOut;
  }

  @Override
  @Deprecated
  public boolean delete(Path path) throws IOException {
    return delete(path, true);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Deleting file " + f.toString());
    }
    FileStatus status;
    try {
      status = getFileStatus(f);
    } catch (FileNotFoundException e) {
      return false;
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (status.isDir()) {
      FileStatus[] contents = listStatus(f, true);
      if (!recursive && contents.length > 0) {
        throw new IOException("Directory " + f.toString() + " is not empty.");
      }
      for (FileStatus p : contents) {
        if (!delete(p.getPath(), recursive)) {
          return false;
        }
      }
      store.delete(key + FOLDER_SUFFIX);
    } else {
      store.delete(key);
    }
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting file status for " + f.toString());
    }
    
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (key.length() == 1) { // root always exists
      return newDirectory(null, absolutePath);
    }

    // We first need to check if the passed path is actually a (potentially empty)
    // folder by checking for the place-holder x_$folder$ blob. If we find that we return it. 
    FileMetadata meta = store.retrieveMetadata(key + FOLDER_SUFFIX);
    if (meta != null) {
      LOG.debug("Found it as an empty folder.");
      return newDirectory(meta, absolutePath);
    } else {
      // It's not an empty folder, try to retrieve it as just that key.
      // If the store finds it, then it's either a file blob, or a folder that was
      // implicitly created; e.g. someone created a/b/c without creating a/b, and then
      // asked for a/b.
      meta = store.retrieveMetadata(key);
      if (meta != null) {
        if (!meta.isDir()) {
          if (LOG.isDebugEnabled())
            LOG.debug("Found it as a file.");
          return newFile(meta, absolutePath);
        } else {
          if (LOG.isDebugEnabled())
            LOG.debug("Found it as a folder (with files in it).");
          return newDirectory(meta, absolutePath);
        }
      }
    }

    throw new FileNotFoundException(absolutePath
        + ": No such file or directory.");
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Retrieves the status of the given path if it's a file, or of all the files/directories
   * within it if it's a directory.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    return listStatus(f, false);
  }

  /**
   * <p>
   * If <code>f</code> is a file, this method will make a single call to Azure.
   * If <code>f</code> is a directory, this method will make a maximum of
   * (<i>n</i> / 1000) + 2 calls to Azure, where <i>n</i> is the total number of
   * files and directories contained directly in <code>f</code>.
   * </p>
   */
  private FileStatus[] listStatus(Path f, boolean recursive) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Listing status for " + f.toString());
    }
    
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    Set<FileStatus> status = new TreeSet<FileStatus>();
    FileMetadata meta = store.retrieveMetadata(key);

    if (meta != null) {
      if (!meta.isDir()) {
        if (LOG.isDebugEnabled())
          LOG.debug("Found it as a file.");
        return new FileStatus[] { newFile(meta, absolutePath) };
      }
      PartialListing listing = store.list(key, AZURE_LIST_ALL);
      for (FileMetadata fileMetadata : listing.getFiles()) {
        String currentKey = fileMetadata.getKey();
        if (!recursive && currentKey.indexOf('/', key.length() + 1) > 0) {
          // It's within an inner directory, skip.
          continue;
        }
        if (currentKey.endsWith(FOLDER_SUFFIX)) {
          String rawKey = currentKey.substring(0, currentKey.length() -
              FOLDER_SUFFIX.length() - 1);
          status.add(newDirectory(fileMetadata, keyToPath(rawKey)));
        } else {
          status.add(newFile(fileMetadata, keyToPath(currentKey)));
        }
      }
      if (LOG.isDebugEnabled())
        LOG.debug("Found it as a directory with " + status.size()
            + " files in it.");
    } else {
      if (LOG.isDebugEnabled())
        LOG.debug("Didn't find any metadata for it.");
    }

    return status.toArray(new FileStatus[0]);
  }

  static class PermissiveFileStatus extends FileStatus {

    public PermissiveFileStatus(long length, long blockSize, boolean isdir,
        long modification_time, Path path, FsPermission permission) {
      super(length, isdir, 1, blockSize, modification_time, 0,
          permission, null, null, path);
    }

    @Override
    public boolean isOwnedByUser(String user, String [] userGroups) {
      return true;
    }

  }

  private FileStatus newFile(FileMetadata meta, Path path) {
    return new PermissiveFileStatus(meta.getLength(), blockSize, false,
        meta.getLastModified(), path.makeQualified(this), meta.getPermission());
  }

  private FileStatus newDirectory(FileMetadata meta, Path path) {
    return new PermissiveFileStatus(0, 0, true, 0, path.makeQualified(this),
        meta == null ? FsPermission.getDefault() : meta.getPermission());
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Creating directory " + f.toString());
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    store.storeEmptyFile(key + FOLDER_SUFFIX, permission);

    // otherwise throws exception
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Opening file " + f.toString());
    }
    if (!exists(f)) {
      throw new FileNotFoundException(f.toString());
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    return new FSDataInputStream(new BufferedFSInputStream(
        new NativeAzureFsInputStream(store.retrieve(key), key), bufferSize));
  }

  private boolean existsAndIsFile(Path f) throws IOException {

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    if (key.length() == 0) {
      return false;
    }

    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null && !meta.isDir()) {
      // Azure object with given key exists, so this is a file
      return true;
    }

    if (meta != null && meta.isDir()) {
      // Azure object with given key exists, so this is a directory
      return false;
    }

    PartialListing listing = store.list(key, 1, null);
    if (listing.getFiles().length > 0 || listing.getCommonPrefixes().length > 0) {
      // Non-empty directory
      return false;
    }

    throw new FileNotFoundException(absolutePath
        + ": No such file or directory");
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving " + src.toString() + " to " + dst.toString());
    }

    String srcKey = pathToKey(makeAbsolute(src));

    if (srcKey.length() == 0) {
      // Cannot rename root of file system
      return false;
    }

    // Figure out the final destination
    String dstKey;
    try {
      boolean dstIsFile = existsAndIsFile(dst);
      if (dstIsFile) {
        // Attempting to overwrite a file using rename()
        return false;
      } else {
        // Move to within the existent directory
        dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      }
    } catch (FileNotFoundException e) {
      // dst doesn't exist, so we can proceed
      dstKey = pathToKey(makeAbsolute(dst));
      try {
        if (!getFileStatus(dst.getParent()).isDir()) {
          return false; // parent dst is a file
        }
      } catch (FileNotFoundException ex) {
        return false; // parent dst does not exist
      }
    }

    try {
      boolean srcIsFile = existsAndIsFile(src);
      if (srcIsFile) {
        store.rename(srcKey, dstKey);
      } else {
        // Move everything inside the folder.
        //
        String priorLastKey = null;
        do {
          // Get a listing of all blobs in the folder.
          //
          PartialListing listing = store.listAll(srcKey, AZURE_LIST_ALL,
              priorLastKey);

          // Rename all the file in the folder.
          //
          for (FileMetadata file : listing.getFiles()) {
            store.rename(file.getKey(),
                dstKey + file.getKey().substring(srcKey.length()));
          }
          priorLastKey = listing.getPriorLastKey();
        } while (priorLastKey != null);
        if (store.retrieveMetadata(srcKey + FOLDER_SUFFIX) != null) {
          store.rename(srcKey + FOLDER_SUFFIX, dstKey + FOLDER_SUFFIX);
        }
      }
      return true;
    } catch (FileNotFoundException e) {
      // Source file does not exist;
      return false;
    } catch (OutOfMemoryError e1) {
      // TODO: Does it make sense to print an error message here since there
      // TODO: maybe no memory to even print the message?
      System.err.println("Encountered out of memory error possibly due to "
          + "an attempt to rename too many files");
      return false;
    }
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = newDir;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * Encode the key with a random prefix for load balancing in Azure storage.
   * Upload data to a random temporary file then do storage side renaming to
   * recover the original key.
   * 
   * @param aKey
   * @param numBuckets
   * @return Encoded version of the original key.
   */
  private static String encodeKey(String aKey) {
    // Get the tail end of the key name.
    //
    String fileName = aKey.substring(aKey.lastIndexOf(Path.SEPARATOR) + 1,
        aKey.length());

    // Construct the randomized prefix of the file name. The prefix ensures the
    // file always
    // drops into the same folder but with a varying tail key name.
    //
    String filePrefix = AZURE_TEMP_FOLDER + Path.SEPARATOR
        + UUID.randomUUID().toString();

    // Concatenate the randomized prefix with the tail of the key name.
    //
    String randomizedKey = filePrefix + fileName;

    // Return to the caller with the randomized key.
    //
    return randomizedKey;
  }

}
