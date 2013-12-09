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

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

/**
 * ALL operations that get/set permissions/ACLs route through here.
 * Let's take that to its extreme and see what happens.
 * Encapsulation!
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class AclManager {
  private final ConcurrentHashMap<INode, ManagedAcl> inodeAclMap =
    new ConcurrentHashMap<INode, ManagedAcl>();

  static class ManagedAcl {
    Set<AclEntry> entries;
    boolean stickyBit;

    boolean isAcl() {
      return false;
    }

    boolean isPermissions() {
      return true;
    }
  }

  public void assignAcl(INode inode, Set<AclEntry> entries, boolean stickyBit) {
  }

  public void assignPermission() {
  }

  public ManagedAcl getAcl(INode inode) {
    return null;
  }

  public void releaseAcl(INode inode) {
  }

  public FsPermission getFsPermission(INode inode) {
    return new FsPermission(inode.getFsPermissionShort());
  }

  public FsPermission getFsPermission(INode inode, Snapshot snapshot) {
    return new FsPermission(inode.getFsPermissionShort(snapshot));
  }

  public void setFsPermission(FsPermission permission, INode inode) {
    inode.setFsPermissionShort(permission.toShort());
  }

  public void setFsPermission(FsPermission permission, INode inode,
      Snapshot snapshot, INodeMap inodeMap) throws QuotaExceededException {
    inode.setFsPermissionShort(permission.toShort(), snapshot, inodeMap);
  }

  // TODO: all the public API methods, FSNamesystem will delegate to here
}
