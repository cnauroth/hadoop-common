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

import org.apache.hadoop.fs.permission.FsPermission;

/**
 * <p>
 * Holds basic metadata for a file stored in a {@link NativeFileSystemStore}.
 * </p>
 */
class FileMetadata {
  private final String key;
  private final long length;
  private final long lastModified;
  private final boolean isDir;
  private final FsPermission permission;

  public FileMetadata(String key, long length, long lastModified, FsPermission permission) {
    this.key = key;
    this.length = length;
    this.lastModified = lastModified;
    this.isDir = false;
    this.permission = permission;
  }

  public FileMetadata(String key, FsPermission permission) {
    this.key = key;
    this.isDir = true;
    this.length = 0;
    this.lastModified = 0;
    this.permission = permission;
  }

  public boolean isDir() {
    return isDir;
  }

  public String getKey() {
    return key;
  }

  public long getLength() {
    return length;
  }

  public long getLastModified() {
    return lastModified;
  }
  
  public FsPermission getPermission() {
    return permission;
  }

  @Override
  public String toString() {
    return "FileMetadata[" + key + ", " + length + ", " + lastModified + ", " + permission + "]";
  }
}
