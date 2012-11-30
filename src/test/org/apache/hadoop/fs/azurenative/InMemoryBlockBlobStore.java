package org.apache.hadoop.fs.azurenative;

import java.io.*;
import java.util.*;

public class InMemoryBlockBlobStore {
  private final HashMap<String, Entry> blobs =
      new HashMap<String, Entry>();

  public synchronized Iterable<String> getKeys() {
    return blobs.keySet();
  }

  public synchronized byte[] getContent(String key) {
    return blobs.get(key).content;
  }

  @SuppressWarnings("unchecked")
  public synchronized void setContent(String key, byte[] value,
      HashMap<String, String> metadata) {
    blobs.put(key, new Entry(value, (HashMap<String, String>)metadata.clone()));
  }
  
  public OutputStream upload(final String key,
      final HashMap<String, String> metadata) {
    setContent(key, new byte[0], metadata);
    return new ByteArrayOutputStream() {
      @Override
      public void flush()
          throws IOException {
        super.flush();
        setContent(key, toByteArray(), metadata);
      }
    };
  }

  public synchronized void copy(String sourceKey, String destKey) {
    blobs.put(destKey, blobs.get(sourceKey));
  }

  public synchronized void delete(String key) {
    blobs.remove(key);
  }

  public synchronized boolean exists(String key) {
    return blobs.containsKey(key);
  }

  @SuppressWarnings("unchecked")
  public synchronized HashMap<String, String> getMetadata(String key) {
    return (HashMap<String, String>)blobs.get(key).metadata.clone();
  }

  private static class Entry {
    private byte[] content;
    private HashMap<String, String> metadata;
 
    public Entry(byte[] content, HashMap<String, String> metadata) {
      this.content = content;
      this.metadata = metadata;
    }
  }
}
