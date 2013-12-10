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

import static org.apache.hadoop.hdfs.server.namenode.AclMergeFunctions.*;

import java.util.List;

import com.google.common.base.Function;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.Acl;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.snapshot.Snapshot;

/**
 * AclManager handles all operations that get and set permissions or ACLs on
 * inodes.  This is intended to encapsulate the logic for in-memory
 * representation, fsimage persistence and edit logging for ACLs.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class AclManager {

  public FsPermission getFsPermission(INode inode) {
    return new FsPermission(inode.getFsPermissionShort());
  }

  public FsPermission getFsPermission(INode inode, Snapshot snapshot) {
    return new FsPermission(inode.getFsPermissionShort(snapshot));
  }

  public void setFsPermission(INode inode, FsPermission permission) {
    inode.setFsPermissionShort(permission.toShort());
  }

  public void setFsPermission(INode inode, Snapshot snapshot, INodeMap inodeMap,
      FsPermission permission) throws QuotaExceededException {
    inode.setFsPermissionShort(permission.toShort(), snapshot, inodeMap);
  }

  public Acl getAcl(INode inode) {
    return getAclByIndex(fromShortToIndex(inode.getFsPermissionShort()), inode);
  }

  public Acl getAcl(INode inode, Snapshot snapshot)
      throws QuotaExceededException {
    return getAclByIndex(fromShortToIndex(
      inode.getFsPermissionShort(snapshot)), inode);
  }

  public void modifyAclEntries(INode inode, final List<AclEntry> aclSpec) {
    doINodeAclModification(inode, new Function<Acl, Acl>() {
      public Acl apply(Acl existingAcl) {
        return null;
      }
    });
  }

  public void modifyAclEntries(INode inode, Snapshot snapshot,
      INodeMap inodeMap, List<AclEntry> aclSpec) throws QuotaExceededException {
    doINodeAclModification(inode, snapshot, inodeMap, new Function<Acl, Acl>() {
      public Acl apply(Acl existingAcl) {
        return null;
      }
    });
  }

  public void removeAcl(INode inode) {
    doINodeAclModification(inode, mergeRemoveAcl());
  }

  public void removeAcl(INode inode, Snapshot snapshot, INodeMap inodeMap)
      throws QuotaExceededException {
    doINodeAclModification(inode, snapshot, inodeMap, mergeRemoveAcl());
  }

  public void removeAclEntries(INode inode, List<AclEntry> aclSpec) {
    doINodeAclModification(inode, mergeRemoveAclEntries(aclSpec));
  }

  public void removeAclEntries(INode inode, Snapshot snapshot,
      INodeMap inodeMap, List<AclEntry> aclSpec) throws QuotaExceededException {
    doINodeAclModification(inode, snapshot, inodeMap,
      mergeRemoveAclEntries(aclSpec));
  }

  public void removeDefaultAcl(INode inode) {
    doINodeAclModification(inode, new Function<Acl, Acl>() {
      public Acl apply(Acl existingAcl) {
        return null;
      }
    });
  }

  public void removeDefaultAcl(INode inode, Snapshot snapshot,
      INodeMap inodeMap) throws QuotaExceededException {
    doINodeAclModification(inode, snapshot, inodeMap, new Function<Acl, Acl>() {
      public Acl apply(Acl existingAcl) {
        return null;
      }
    });
  }

  public void setAcl(INode inode, List<AclEntry> aclSpec) {
    doINodeAclModification(inode, new Function<Acl, Acl>() {
      public Acl apply(Acl existingAcl) {
        return null;
      }
    });
  }

  public void setAcl(INode inode, Snapshot snapshot, INodeMap inodeMap,
      List<AclEntry> aclSpec) throws QuotaExceededException {
    doINodeAclModification(inode, snapshot, inodeMap, new Function<Acl, Acl>() {
      public Acl apply(Acl existingAcl) {
        return null;
      }
    });
  }

  private void doINodeAclModification(INode inode, Function<Acl, Acl> merge) {
    Acl existingAcl = getAcl(inode);
    Acl modifiedAcl = merge.apply(existingAcl);
    int modifiedAclIndex = getIndexByAcl(modifiedAcl);
    short fsPermissionShort = fromIndexToShort(modifiedAclIndex);
    inode.setFsPermissionShort(fsPermissionShort);
  }

  private void doINodeAclModification(INode inode, Snapshot snapshot,
      INodeMap inodeMap, Function<Acl, Acl> merge)
      throws QuotaExceededException {
    Acl existingAcl = getAcl(inode, snapshot);
    Acl modifiedAcl = merge.apply(existingAcl);
    int modifiedAclIndex = getIndexByAcl(modifiedAcl);
    short fsPermissionShort = fromIndexToShort(modifiedAclIndex);
    inode.setFsPermissionShort(fsPermissionShort, snapshot, inodeMap);
  }

  private Acl getAclByIndex(int index, INode inode) {
    long inodeId = inode.getId();
    return null;
  }

  private int getIndexByAcl(Acl acl) {
    return 0;
  }

  private static short fromIndexToShort(int index) {
    return 0;
  }

  private static int fromShortToIndex(short fsPermissionShort) {
    return 0;
  }
}
