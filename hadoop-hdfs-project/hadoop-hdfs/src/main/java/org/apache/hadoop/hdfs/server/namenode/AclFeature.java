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

package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

/**
 * Feature that represents the ACLs of the inode.
 */
@InterfaceAudience.Private
public class AclFeature implements INode.Feature {
  public static final ImmutableList<AclEntry> EMPTY_ENTRY_LIST =
    ImmutableList.of();

  private final int [] entries;

  public AclFeature(int[] entries) {
    this.entries = entries;
  }

  /**
   * Get the number of entries present
   */
  int getEntriesSize() {
    return entries.length;
  }

  /**
   * Get the entry at the specified position
   * @param pos Position of the entry to be obtained
   * @return integer representation of AclEntry
   * @throws IndexOutOfBoundsException if pos out of bound
   */
  int getEntryAt(int pos) {
    Preconditions.checkPositionIndex(pos, entries.length,
        "Invalid position for AclEntry");
    return entries[pos];
  }
}
