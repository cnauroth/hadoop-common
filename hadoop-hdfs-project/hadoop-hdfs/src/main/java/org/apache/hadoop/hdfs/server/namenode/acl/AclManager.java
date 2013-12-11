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
package org.apache.hadoop.hdfs.server.namenode.acl;

import static org.apache.hadoop.hdfs.server.namenode.acl.AclTransformation.*;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.Acl;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.PermissionSource;
import org.apache.hadoop.hdfs.server.namenode.PermissionTarget;

/**
 * AclManager handles all operations that get and set permissions or ACLs on
 * inodes.  This is intended to encapsulate the logic for in-memory
 * representation, fsimage persistence and edit logging for ACLs.
 */
@InterfaceAudience.LimitedPrivate({"HDFS"})
public class AclManager {
  private final FSNamesystem namesystem;

  public AclManager(FSNamesystem namesystem) {
    this.namesystem = namesystem;
  }

  public FsPermission getFsPermission(PermissionSource source) {
    assert namesystem.hasReadLock();
    return new FsPermission(source.getFsPermissionShort());
  }

  public void setFsPermission(PermissionTarget target,
      FsPermission permission) throws QuotaExceededException {
    assert namesystem.hasWriteLock();
    target.setFsPermissionShort(permission.toShort());
  }

  public Acl getAcl(PermissionSource source) {
    assert namesystem.hasReadLock();
    return getAclByIndex(fromShortToIndex(source.getFsPermissionShort()),
      source.getINode());
  }

  public Acl modifyAcl(PermissionTarget target,
      AclTransformation transformation) throws QuotaExceededException {
    assert namesystem.hasWriteLock();
    Acl existingAcl = getAcl(target);
    Acl modifiedAcl = transformation.apply(existingAcl);
    int modifiedAclIndex = getIndexByAcl(modifiedAcl);
    short fsPermissionShort = fromIndexToShort(modifiedAclIndex);
    target.setFsPermissionShort(fsPermissionShort);
    return modifiedAcl;
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
