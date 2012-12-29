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
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
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

  static final String AZURE_BLOCK_SIZE_PROPERTY_NAME = "fs.azure.block.size";
  static final String PATH_DELIMITER = Path.SEPARATOR;
  static final String AZURE_TEMP_FOLDER = "_$azuretmpfolder$";

  private static final String keyMaxRetries = "fs.azure.maxRetries";
  private static final String keySleepTime = "fs.azure.sleepTimeSeconds";

  private static final int AZURE_MAX_FLUSH_RETRIES = 3;
  private static final int AZURE_MAX_READ_RETRIES = 3;
  private static final int AZURE_BACKOUT_TIME = 100;
  private static final int AZURE_LIST_ALL = -1;
  private static final int AZURE_UNBOUNDED_DEPTH = -1;

  private static final long MAX_AZURE_BLOCK_SIZE = 64 * 1024 * 1024L;

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

          if (null != statistics) {
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
    private static final int S_STREAM_BUFFERSIZE = 4 * 1024 * 1024; // 4MB buffers.

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
      this.buffer = new ByteArrayOutputStream(S_STREAM_BUFFERSIZE);

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
          assert outRingBuffer.isEmpty() : 
            "Empty ring buffer expected after writing all contents to output stream.";

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
        assert null == outRingBuffer : 
          "Expected no ring buffer for a stream that's already closed.";

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
      assert (outRingBuffer.isEmpty()) : 
        "Unexpected non-empty ring buffer after flush.";

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
        assert outRingBuffer.isEmpty() : 
          "Ring buffer containing residual byte array streams unexpected.";

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
      assert (null != buffer) : 
        "Unexpected null byte array output stream (1).";

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
        //
        assert (buffer.size() < S_STREAM_BUFFERSIZE) : 
          "Unexpected buffer size:" + buffer.size();

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

        // each time the loop runs, a full buffer is added to outRingBuffer
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
  private AzureNativeFileSystemStore actualStore;
  private Path workingDir;
  private long blockSize = MAX_AZURE_BLOCK_SIZE;
  private AzureFileSystemInstrumentation instrumentation;
  private static boolean suppressRetryPolicy = false;

  public NativeAzureFileSystem() {
    // set store in initialize()
    //
  }

  public NativeAzureFileSystem(NativeFileSystemStore store) {
    this.store = store;
  }

  /**
   * Suppress the default retry policy for the Storage, useful in unit
   * tests to test negative cases without waiting forever.
   */
  static void suppressRetryPolicy() {
    suppressRetryPolicy = true;
  }

  /**
   * Undo the effect of suppressRetryPolicy.
   */
  static void resumeRetryPolicy() {
    suppressRetryPolicy = false;
  }

  @Override
  public void initialize(URI uri, Configuration conf)
      throws IOException, IllegalArgumentException {
    super.initialize(uri, conf);

    if (store == null) {
      store = createDefaultStore(conf);
    }

    // Make sure the metrics system is available before interacting with Azure
    AzureFileSystemMetricsSystem.fileSystemStarted();
    String sourceName = "AzureFileSystemMetrics",
        sourceDesc = "Azure Storage Volume File System metrics";
    instrumentation = DefaultMetricsSystem.INSTANCE.register(sourceName,
        sourceDesc, new AzureFileSystemInstrumentation(conf));
    AzureFileSystemMetricsSystem.registerSource(sourceName, sourceDesc,
        instrumentation);
    store.initialize(uri, conf, instrumentation);
    setConf(conf);
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = new Path("/user", 
        System.getProperty("user.name")).makeQualified(this);
    this.blockSize = conf.getLong(AZURE_BLOCK_SIZE_PROPERTY_NAME, MAX_AZURE_BLOCK_SIZE);
  }

  private NativeFileSystemStore createDefaultStore(Configuration conf) {
    actualStore = new AzureNativeFileSystemStore();

    if (suppressRetryPolicy) {
      actualStore.suppressRetryPolicy();
      return actualStore;
    }

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
    Map<Class<? extends Exception>, RetryPolicy> exceptionToPolicyMap =
        new HashMap<Class<? extends Exception>, RetryPolicy>();

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
        NativeFileSystemStore.class, actualStore, methodNameToPolicyMap);
  }

  // TODO: The logic for this method is confusing as to whether it strips the
  // last slash or not (it adds it in the beginning, then strips it at the end).
  // We should revisit that.
  private String pathToKey(Path path) {
    // Convert the path to a URI to parse the scheme, the authority, and the
    // path from the path object.
    //
    URI tmpUri = path.toUri();
    String pathUri = tmpUri.getRawPath();

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

    String key = null;
    key = newPath.toUri().getRawPath();
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

  /**
   * For unit test purposes, retrieves the AzureNativeFileSystemStore store
   * backing this file system.
   * @return The store object.
   */
  AzureNativeFileSystemStore getStore() {
    return actualStore;
  }

  /**
   * Gets the metrics source for this file system.
   * This is mainly here for unit testing purposes.
   *
   * @return the metrics source.
   */
  public AzureFileSystemInstrumentation getInstrumentation() {
    return instrumentation;
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

    if (LOG.isDebugEnabled()){
      LOG.debug("Creating file: " + f.toString());
    }

    // Only check for existence (requires a web request) if we're not
    // overwriting.
    if (!overwrite && exists(f)) {
      throw new IOException("File already exists:" + f);
    }

    // Open the output blob stream based on the encoded key.
    //
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    String keyEncoded = encodeKey(key);

    PermissionStatus permissionStatus = createPermissionStatus(permission);

    // First create a blob at the real key, pointing back to the temporary file
    // This accomplishes a few things:
    // 1. Makes sure we can create a file there.
    // 2. Makes it visible to other concurrent threads/processes/nodes what we're
    //    doing.
    // 3. Makes it easier to restore/cleanup data in the event of us crashing.
    //
    store.storeEmptyLinkFile(key, keyEncoded, permissionStatus);

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
    OutputStream bufOutStream = new NativeAzureFsOutputStream(
        store.storefile(keyEncoded, permissionStatus),
        key,
        keyEncoded);

    // Construct the data output stream from the buffered output stream.
    //
    FSDataOutputStream fsOut = new FSDataOutputStream(bufOutStream, statistics);
    
    // Increment the counter
    instrumentation.fileCreated();

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

    if (LOG.isDebugEnabled()){
      LOG.debug("Deleting file: " + f.toString());
    }

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    // Capture the metadata for the path.
    //
    FileMetadata metaFile = store.retrieveMetadata(key);

    if (null == metaFile) {
      // The path to be deleted does not exist.
      //
      return false;
    }
    
    // The path exists, determine if it is a folder containing objects,
    // an empty folder, or a simple file and take the appropriate actions.
    //
    if (!metaFile.isDir()){
      // The path specifies a file. We need to check the parent path
      // to make sure it's a proper materialized directory before we
      // delete the file. Otherwise we may get into a situation where
      // the file we were deleting was the last one in an implicit directory
      // (e.g. the blob store only contains the blob a/b and there's no
      // corresponding directory blob a) and that would implicitly delete
      // the directory as well, which is not correct.
      //
      Path parentPath = absolutePath.getParent();
      if (parentPath.getParent() != null) {// Not root
        String parentKey = pathToKey(parentPath);
        FileMetadata parentMetadata = store.retrieveMetadata(parentKey);
        if (!parentMetadata.isDir()) {
          // Invalid state: the parent path is actually a file. Throw.
          //
          throw new AzureException("File " + f + " has a parent directory " +
              parentPath + " which is also a file. Can't resolve.");
        }
        if (parentMetadata.getBlobMaterialization() ==
            BlobMaterialization.Implicit) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Found an implicit parent directory while trying to" +
                " delete the file " + f + ". Creating the directory blob for" +
                " it in " + parentKey + ".");
          }
          store.storeEmptyFolder(parentKey,
              createPermissionStatus(FsPermission.getDefault()));
        }
      }
      store.delete(key);
      instrumentation.fileDeleted();
    } else {
      // The path specifies a folder. Recursively delete all entries under the
      // folder. List all the blobs in the current folder.
      //
      String priorLastKey = null;
      PartialListing listing = store.listAll(key, AZURE_LIST_ALL, 1, priorLastKey);
      FileMetadata [] contents = listing.getFiles();
      if (!recursive && contents.length > 0) {
        // The folder is non-empty and recursive delete was not specified.
        // Throw an exception indicating that a non-recursive delete was
        // specified for a non-empty folder.
        //
        throw new IOException("Non-recursive delete of non-empty directory " + f.toString());
      }

      // Delete all the files in the folder.
      //
      for (FileMetadata p : contents){
        // Tag on the directory name found as the suffix of the suffix of the parent
        // directory to get the new absolute path.
        //
        String suffix = p.getKey().substring(p.getKey().lastIndexOf(PATH_DELIMITER));
        if (!p.isDir()){
          store.delete(key + suffix);
          instrumentation.fileDeleted();
        } else {
          // Recursively delete contents of the sub-folders. Notice this also
          // deletes the blob for the directory.
          //
          if(!delete(new Path(f.toString() + suffix), true)){
            return false;
          }
        }
      }
      store.delete(key);
      instrumentation.directoryDeleted();
    }

    // File or directory was successfully deleted.
    //
    return true;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Getting the file status for " + f.toString());
    }

    // Capture the absolute path and the path to key.
    //
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    if (key.length() == 0) { // root always exists
      return newDirectory(null, absolutePath);
    }

    // The path is either a folder or a file. Retrieve metadata to
    // determine if it is a directory or file.
    //
    FileMetadata meta = store.retrieveMetadata(key);
    if (meta != null) {
      if (meta.isDir())
      {
        // The path is a folder with files in it.
        //
        if (LOG.isDebugEnabled()) {
          LOG.debug("Path " + f.toString() + "is a folder.");
        }

        // Return reference to the directory object.
        //
        return newDirectory(meta, absolutePath);      
      }

      // The path is a file.
      //
      if (LOG.isDebugEnabled())
      {
        LOG.debug("Found the path: " + f.toString() + " as a file.");
      }

      // Return with reference to a file object.
      //
      return newFile(meta, absolutePath);
    }

    // File not found. Throw exception no such file or directory.
    // Note: Should never get to this point since the root always exists.
    //
    throw new FileNotFoundException(
        absolutePath + ": No such file or directory.");
  }

  @Override
  public URI getUri() {
    return uri;
  }

  /**
   * Retrieve the status of a given path if it is a file, or of all the contained
   * files if it is a directory.
   */
  @Override
  public FileStatus[] listStatus(Path f) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Listing status for " + f.toString());
    }

    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);
    Set<FileStatus> status = new TreeSet<FileStatus>();
    FileMetadata meta = store.retrieveMetadata(key);

    if (meta != null) {
      if (!meta.isDir()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Found path as a file");
        }
        return new FileStatus[] { newFile(meta, absolutePath) };
      }
      String partialKey = null;
      PartialListing listing = store.list(key, AZURE_LIST_ALL, 1, partialKey);
      for (FileMetadata fileMetadata : listing.getFiles()) {
        Path subpath = keyToPath(fileMetadata.getKey());

        // Test whether the metadata represents a file or directory and
        // add the appropriate metadata object.
        //
        // Note: There was a very old bug here where directories were added
        //       to the status set as files flattening out recursive listings
        //       using "-lsr" down the file system hierarchy.
        //
        if (fileMetadata.isDir()) {
          // Make sure we hide the temp upload folder
          //
          if (fileMetadata.getKey().equals(AZURE_TEMP_FOLDER)) {
            // Don't expose that.
            //
            continue;
          }
          status.add(newDirectory(fileMetadata, subpath));
        } else {
          status.add(newFile(fileMetadata, subpath));
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Found path as a directory with " + status.size() + " files in it.");
      }
    } else {
      // There is no metadata found for the path.
      //
      if (LOG.isDebugEnabled()) {
        LOG.debug("Did not find any metadata for path: " + key);
      }

      return null;
    }

    return status.toArray(new FileStatus[0]);
  }

  private FileStatus newFile(FileMetadata meta, Path path) {
    return new FileStatus (
        meta.getLength(),
        false,
        1,
        blockSize,
        meta.getLastModified(),
        0,
        meta.getPermissionStatus().getPermission(),
        meta.getPermissionStatus().getUserName(),
        meta.getPermissionStatus().getGroupName(),
        path.makeQualified(this));
  }

  private FileStatus newDirectory(FileMetadata meta, Path path) {
    return new FileStatus (
        0,
        true,
        1,
        blockSize,
        meta == null ? 0 : meta.getLastModified(),
        0,
        meta == null ? FsPermission.getDefault() : meta.getPermissionStatus().getPermission(),
        meta == null ? "" : meta.getPermissionStatus().getUserName(),
        meta == null ? "" : meta.getPermissionStatus().getGroupName(),
        path.makeQualified(this));
  }

  /**
   * Creates the PermissionStatus object to use for the given permission,
   * based on the current user in context.
   * @param permission The permission for the file.
   * @return The permission status object to use.
   * @throws IOException If login fails in getCurrentUser
   */
  private PermissionStatus createPermissionStatus(FsPermission permission)
      throws IOException {
    // Create the permission status for this file based on current user
    return new PermissionStatus(
        UserGroupInformation.getCurrentUser().getShortUserName(),
        null,
        permission);
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {

    if (LOG.isDebugEnabled()){
      LOG.debug("Creating directory: " + f.toString());
    }

    Path absolutePath = makeAbsolute(f);

    // Check that there is no file in the parent chain of the given path.
    for (Path current = absolutePath, parent = current.getParent();
        parent != null; // Stop when you get to the root
        current = parent, parent = current.getParent()) {
      FileMetadata currentMetadata = store.retrieveMetadata(pathToKey(current));
      if (currentMetadata != null && !currentMetadata.isDir()) {
        throw new IOException("Cannot create directory " + f + " because " +
            current + " is an existing file.");
      }
    }

    String key = pathToKey(absolutePath);
    store.storeEmptyFolder(key, createPermissionStatus(permission));
    instrumentation.directoryCreated();

    // otherwise throws exception
    return true;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    if (LOG.isDebugEnabled()){
      LOG.debug("Opening file: " + f.toString());
    }

    if (!exists(f)) {
      throw new FileNotFoundException(f.toString());
    }
    Path absolutePath = makeAbsolute(f);
    String key = pathToKey(absolutePath);

    return new FSDataInputStream(new BufferedFSInputStream(
        new NativeAzureFsInputStream(store.retrieve(key), key), bufferSize));
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving " + src + " to " + dst);
    }
    String srcKey = pathToKey(makeAbsolute(src));

    if (srcKey.length() == 0) {
      // Cannot rename root of file system
      return false;
    }

    // Figure out the final destination
    Path absoluteDst = makeAbsolute(dst);
    String dstKey = pathToKey(absoluteDst);
    FileMetadata dstMetadata = store.retrieveMetadata(dstKey);
    if (dstMetadata != null && dstMetadata.isDir()) {
      // It's an existing directory.
      dstKey = pathToKey(makeAbsolute(new Path(dst, src.getName())));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Destination " + dst +
            " is a directory, adjusted the destination to be " + dstKey);
      }
    } else if (dstMetadata != null) {
      // Attempting to overwrite a file using rename()
      if (LOG.isDebugEnabled()) {
        LOG.debug("Destination " + dst +
            " is an already existing file, failing the rename.");
      }
      return false;      
    } else {
      // Check that the parent directory exists.
      FileMetadata parentOfDestMetadata =
          store.retrieveMetadata(pathToKey(absoluteDst.getParent()));
      if (parentOfDestMetadata == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Parent of the destination " + dst +
              " doesn't exist, failing the rename.");
        }
        return false;
      } else if (!parentOfDestMetadata.isDir()) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Parent of the destination " + dst +
              " is a file, failing the rename.");
        }
        return false;
      }
    }
    FileMetadata srcMetadata = store.retrieveMetadata(srcKey);
    if (srcMetadata == null) {
      // Source doesn't exist
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source " + src + " doesn't exist, failing the rename.");
      }
      return false;
    } else if (!srcMetadata.isDir()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source " + src + " found as a file, renaming.");
      }
      store.rename(srcKey, dstKey);
    } else {
      // Move everything inside the folder.
      //
      String priorLastKey = null;

      // Calculate the index of the part of the string to be moved. That
      // is everything on the path up to the folder name.
      //
      do {
        // List all blobs rooted at the source folder.
        //
        PartialListing listing = store.listAll(srcKey, AZURE_LIST_ALL,
            AZURE_UNBOUNDED_DEPTH, priorLastKey);

        // Rename all the files in the folder.
        //
        for (FileMetadata file : listing.getFiles()) {
          // Rename all non-directory entries under the folder to point to the
          // final destination.
          //
          if (!file.isDir()) {
            String srcName = file.getKey();
            String suffix  = srcName.substring(srcKey.length());
            String dstName = dstKey + suffix;  
            store.rename(srcName, dstName);
          }
        }
        priorLastKey = listing.getPriorLastKey();
      } while (priorLastKey != null);
      // Rename the top level empty blob for the folder.
      //
      if (srcMetadata.getBlobMaterialization() ==
          BlobMaterialization.Explicit) {
        store.rename(srcKey, dstKey);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Renamed " + src + " to " + dst + " successfully.");
    }
    return true;
  }

  /**
   * Set the working directory to the given directory.
   */
  @Override
  public void setWorkingDirectory(Path newDir) {
    workingDir = makeAbsolute(newDir);
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public void close() throws IOException {
    // Call the base close() to close any resources there.
    super.close();
    // Close the store to close any resources there - e.g. the bandwidth
    // updater thread would be stopped at this time.
    store.close();
    // Notify the metrics system that this file system is closed, which may
    // trigger one final metrics push to get the accurate final file system
    // metrics out.
    AzureFileSystemMetricsSystem.fileSystemClosed();
  }

  public void recoverFilesWithDanglingTempData(Path root) {
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
