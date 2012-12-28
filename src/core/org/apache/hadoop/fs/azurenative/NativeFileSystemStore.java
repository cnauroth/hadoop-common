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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azure.AzureException;
import org.apache.hadoop.fs.permission.*;

/**
 * <p>
 * An abstraction for a key-based {@link File} store.
 * </p>
 */
interface NativeFileSystemStore {

  void initialize(URI uri, Configuration conf, AzureFileSystemInstrumentation instrumentation)
      throws IOException, IllegalArgumentException;

  void storeEmptyFolder(String key, PermissionStatus permissionStatus) throws AzureException;

  FileMetadata retrieveMetadata(String key) throws IOException;

  DataInputStream retrieve(String key) throws IOException;

  DataInputStream retrieve(String key, long byteRangeStart) throws IOException;
  
  DataOutputStream storefile(String key, PermissionStatus permissionStatus) throws AzureException;

  void storeEmptyLinkFile(String key, String tempBlobKey,
      PermissionStatus permissionStatus) throws AzureException;

  PartialListing list(String prefix, final int maxListingCount, final int maxListingDepth)
      throws IOException;

  PartialListing list(String prefix, final int maxListingCount, final int maxListingDepth,
      String priorLastKey) throws IOException;

  PartialListing listAll(String prefix, final int maxListingCount, final int maxListingDepth,
      String priorLastKey) throws IOException;

  void delete(String key) throws IOException;

  void rename(String srcKey, String dstKey) throws IOException;


  /**
   * Delete all keys with the given prefix. Used for testing.
   * 
   * @throws IOException
   */
  void purge(String prefix) throws IOException;

  /**
   * Diagnostic method to dump state to the console.
   * 
   * @throws IOException
   */
  void dump() throws IOException;

  void close();
}
