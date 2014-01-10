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

import java.util.List;

import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;

@InterfaceAudience.Private
final class AclStorage {

  public static List<AclEntry> readINodeAcl(INodeWithAdditionalFields inode,
      int snapshotId) {
    FsPermission perm = inode.getPermissionStatus(snapshotId).getPermission();
    if (perm.getAclBit()) {
      List<AclEntry> featureEntries = inode.getAclFeature().getEntries();
      ScopedAclEntries scoped = new ScopedAclEntries(featureEntries);
      List<AclEntry> accessEntries = scoped.getAccessEntries();
      List<AclEntry> defaultEntries = scoped.getDefaultEntries();
      List<AclEntry> existingAcl = Lists.newArrayListWithCapacity(
        featureEntries.size() + 3);
      if (accessEntries != null) {
        existingAcl.add(new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.USER)
          .setPermission(perm.getUserAction())
          .build());
        existingAcl.addAll(accessEntries);
        existingAcl.add(new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.MASK)
          .setPermission(perm.getGroupAction())
          .build());
        existingAcl.add(new AclEntry.Builder()
          .setScope(AclEntryScope.ACCESS)
          .setType(AclEntryType.OTHER)
          .setPermission(perm.getOtherAction())
          .build());
      } else {
        existingAcl.addAll(getMinimalAcl(perm));
      }
      if (defaultEntries != null) {
        existingAcl.addAll(defaultEntries);
      }
      return existingAcl;
    } else {
      return getMinimalAcl(perm);
    }
  }

  public static void updateINodeAcl(INodeWithAdditionalFields inode,
      List<AclEntry> newAcl, int snapshotId) throws QuotaExceededException {
    FsPermission perm = inode.getPermissionStatus(snapshotId).getPermission();
    final FsPermission newPerm;
    if (newAcl.size() > 3) {
      ScopedAclEntries scoped = new ScopedAclEntries(newAcl);
      List<AclEntry> accessEntries = scoped.getAccessEntries();
      List<AclEntry> defaultEntries = scoped.getDefaultEntries();
      List<AclEntry> featureEntries = Lists.newArrayListWithCapacity(
        (accessEntries != null ? accessEntries.size() - 3 : 0) +
        (defaultEntries != null ? defaultEntries.size() : 0));
      newPerm = new FsPermission(accessEntries.get(0).getPermission(),
        accessEntries.get(accessEntries.size() - 2).getPermission(),
        accessEntries.get(accessEntries.size() - 1).getPermission(),
        perm.getStickyBit(), true);
      if (accessEntries.size() > 3) {
        featureEntries.addAll(
          accessEntries.subList(1, accessEntries.size() - 2));
      }
      if (defaultEntries != null) {
        featureEntries.addAll(defaultEntries);
      }
      AclFeature aclFeature = inode.getAclFeature();
      if (aclFeature == null) {
        aclFeature = new AclFeature();
        inode.addAclFeature(aclFeature);
      }
      aclFeature.setEntries(featureEntries);
    } else {
      if (perm.getAclBit()) {
        inode.removeAclFeature();
      }
      newPerm = new FsPermission(newAcl.get(0).getPermission(),
        newAcl.get(1).getPermission(),
        newAcl.get(2).getPermission(),
        perm.getStickyBit(), false);
    }
    inode.setPermission(newPerm, snapshotId);
  }

  /**
   * There is no reason to instantiate this class.
   */
  private AclStorage() {
  }

  private static List<AclEntry> getMinimalAcl(FsPermission perm) {
    return Lists.newArrayList(
      new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.USER)
        .setPermission(perm.getUserAction())
        .build(),
      new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.GROUP)
        .setPermission(perm.getGroupAction())
        .build(),
      new AclEntry.Builder()
        .setScope(AclEntryScope.ACCESS)
        .setType(AclEntryType.OTHER)
        .setPermission(perm.getOtherAction())
        .build());
  }
}
