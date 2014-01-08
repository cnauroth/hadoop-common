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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

/** 
 * Class that helps in checking file system permission.
 * The state of this class need not be synchronized as it has data structures that
 * are read-only.
 * 
 * Some of the helper methods are gaurded by {@link FSNamesystem#readLock()}.
 */
class FSPermissionChecker {
  static final Log LOG = LogFactory.getLog(UserGroupInformation.class);

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INode inode, int snapshotId,
      FsAction access) {
    return toAccessControlString(inode, snapshotId, access, null);
  }

  /** @return a string for throwing {@link AccessControlException} */
  private String toAccessControlString(INode inode, int snapshotId,
      FsAction access, AclFeature acl) {
    StringBuilder sb = new StringBuilder("Permission denied: ")
      .append("user=").append(user).append(", ")
      .append("access=").append(access).append(", ")
      .append("inode=\"").append(inode.getFullPathName()).append("\":")
      .append(inode.getUserName(snapshotId)).append(':')
      .append(inode.getGroupName(snapshotId)).append(':')
      .append(inode.isDirectory() ? 'd' : '-')
      .append(inode.getFsPermission(snapshotId));
    if (acl != null) {
      sb.append(':').append(StringUtils.join(",", acl.getEntries()));
    }
    return sb.toString();
  }

  private final UserGroupInformation ugi;
  private final String user;  
  /** A set with group namess. Not synchronized since it is unmodifiable */
  private final Set<String> groups;
  private final boolean isSuper;

  FSPermissionChecker(String fsOwner, String supergroup,
      UserGroupInformation callerUgi) {
    ugi = callerUgi;
    HashSet<String> s = new HashSet<String>(Arrays.asList(ugi.getGroupNames()));
    groups = Collections.unmodifiableSet(s);
    user = ugi.getShortUserName();
    isSuper = user.equals(fsOwner) || groups.contains(supergroup);
  }

  /**
   * Check if the callers group contains the required values.
   * @param group group to check
   */
  public boolean containsGroup(String group) {return groups.contains(group);}

  public String getUser() {
    return user;
  }
  
  public boolean isSuperUser() {
    return isSuper;
  }
  
  /**
   * Verify if the caller has the required permission. This will result into 
   * an exception if the caller is not allowed to access the resource.
   */
  public void checkSuperuserPrivilege()
      throws AccessControlException {
    if (!isSuper) {
      throw new AccessControlException("Access denied for user " 
          + user + ". Superuser privilege is required");
    }
  }
  
  /**
   * Check whether current user have permissions to access the path.
   * Traverse is always checked.
   *
   * Parent path means the parent directory for the path.
   * Ancestor path means the last (the closest) existing ancestor directory
   * of the path.
   * Note that if the parent path exists,
   * then the parent path and the ancestor path are the same.
   *
   * For example, suppose the path is "/foo/bar/baz".
   * No matter baz is a file or a directory,
   * the parent path is "/foo/bar".
   * If bar exists, then the ancestor path is also "/foo/bar".
   * If bar does not exist and foo exists,
   * then the ancestor path is "/foo".
   * Further, if both foo and bar do not exist,
   * then the ancestor path is "/".
   *
   * @param doCheckOwner Require user to be the owner of the path?
   * @param ancestorAccess The access required by the ancestor of the path.
   * @param parentAccess The access required by the parent of the path.
   * @param access The access required by the path.
   * @param subAccess If path is a directory,
   * it is the access required of the path and all the sub-directories.
   * If path is not a directory, there is no effect.
   * @param resolveLink whether to resolve the final path component if it is
   * a symlink
   * @throws AccessControlException
   * @throws UnresolvedLinkException
   * 
   * Guarded by {@link FSNamesystem#readLock()}
   * Caller of this method must hold that lock.
   */
  void checkPermission(String path, INodeDirectory root, boolean doCheckOwner,
      FsAction ancestorAccess, FsAction parentAccess, FsAction access,
      FsAction subAccess, boolean resolveLink)
      throws AccessControlException, UnresolvedLinkException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("ACCESS CHECK: " + this
          + ", doCheckOwner=" + doCheckOwner
          + ", ancestorAccess=" + ancestorAccess
          + ", parentAccess=" + parentAccess
          + ", access=" + access
          + ", subAccess=" + subAccess
          + ", resolveLink=" + resolveLink);
    }
    // check if (parentAccess != null) && file exists, then check sb
    // If resolveLink, the check is performed on the link target.
    final INodesInPath inodesInPath = root.getINodesInPath(path, resolveLink);
    final int snapshotId = inodesInPath.getPathSnapshotId();
    final INode[] inodes = inodesInPath.getINodes();
    int ancestorIndex = inodes.length - 2;
    for(; ancestorIndex >= 0 && inodes[ancestorIndex] == null;
        ancestorIndex--);
    checkTraverse(inodes, ancestorIndex, snapshotId);

    final INode last = inodes[inodes.length - 1];
    if (parentAccess != null && parentAccess.implies(FsAction.WRITE)
        && inodes.length > 1 && last != null) {
      checkStickyBit(inodes[inodes.length - 2], last, snapshotId);
    }
    if (ancestorAccess != null && inodes.length > 1) {
      check(inodes, ancestorIndex, snapshotId, ancestorAccess);
    }
    if (parentAccess != null && inodes.length > 1) {
      check(inodes, inodes.length - 2, snapshotId, parentAccess);
    }
    if (access != null) {
      check(last, snapshotId, access);
    }
    if (subAccess != null) {
      checkSubAccess(last, snapshotId, subAccess);
    }
    if (doCheckOwner) {
      checkOwner(last, snapshotId);
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkOwner(INode inode, int snapshotId
      ) throws AccessControlException {
    if (inode != null && user.equals(inode.getUserName(snapshotId))) {
      return;
    }
    throw new AccessControlException("Permission denied");
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkTraverse(INode[] inodes, int last, int snapshotId
      ) throws AccessControlException {
    for(int j = 0; j <= last; j++) {
      check(inodes[j], snapshotId, FsAction.EXECUTE);
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkSubAccess(INode inode, int snapshotId, FsAction access
      ) throws AccessControlException {
    if (inode == null || !inode.isDirectory()) {
      return;
    }

    Stack<INodeDirectory> directories = new Stack<INodeDirectory>();
    for(directories.push(inode.asDirectory()); !directories.isEmpty(); ) {
      INodeDirectory d = directories.pop();
      check(d, snapshotId, access);

      for(INode child : d.getChildrenList(snapshotId)) {
        if (child.isDirectory()) {
          directories.push(child.asDirectory());
        }
      }
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void check(INode[] inodes, int i, int snapshotId, FsAction access
      ) throws AccessControlException {
    check(i >= 0? inodes[i]: null, snapshotId, access);
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void check(INode inode, int snapshotId, FsAction access
      ) throws AccessControlException {
    if (inode == null) {
      return;
    }
    // TODO: handling of INodeReference?
    AclFeature acl = inode instanceof INodeWithAdditionalFields ?
      ((INodeWithAdditionalFields)inode).getAclFeature() : null;
    if (acl == null) {
      checkFsPermission(inode, snapshotId, access);
    } else {
      checkAcl(inode, snapshotId, access, acl);
    }
  }

  private void checkFsPermission(INode inode, int snapshotId, FsAction access
      ) throws AccessControlException {
    FsPermission mode = inode.getFsPermission(snapshotId);

    if (user.equals(inode.getUserName(snapshotId))) { //user class
      if (mode.getUserAction().implies(access)) { return; }
    }
    else if (groups.contains(inode.getGroupName(snapshotId))) { //group class
      if (mode.getGroupAction().implies(access)) { return; }
    }
    else { //other class
      if (mode.getOtherAction().implies(access)) { return; }
    }
    throw new AccessControlException(
      toAccessControlString(inode, snapshotId, access));
  }

  /**
   * Checks requested access against an Access Control List.  This logic relies
   * on receiving the ACL entries in sorted order.  This is assumed to be true,
   * because the ACL modification methods in {@link AclTransformation} sort the
   * resulting entries.
   *
   * @param inode INode accessed inode
   * @param snapshotId int snapshot ID
   * @param access FsAction requested permission
   * @param acl AclFeature containing ACL entries of inode
   * @throws AccessControlException if the ACL denies permission
   */
  private void checkAcl(INode inode, int snapshotId, FsAction access,
      AclFeature acl) throws AccessControlException {
    // Find the closest matching entry for the user.
    AclEntry matchingEntry = null, mask = null;
    boolean userIsGroupMember = false;
    for (AclEntry entry: acl.getEntries()) {
      AclEntryType type = entry.getType();
      String name = entry.getName();
      if (type == AclEntryType.USER && name == null) {
        // Use owner entry if user is owner.  Don't need mask, so exit early.
        if (user.equals(inode.getUserName(snapshotId))) {
          matchingEntry = entry;
          break;
        }
      } else if (type == AclEntryType.USER) {
        // Use named user entry if user matches name.
        if (user.equals(name)) {
          matchingEntry = entry;
        }
      } else if (type == AclEntryType.GROUP) {
        // Use group entry (unnamed or named) if user is a member and entry
        // grants access.  If user is a member of multiple groups that have
        // entries that grant access, then it doesn't matter which is chosen, so
        // skip iterations after first match.
        if (matchingEntry != null) {
          continue;
        }
        String group = name == null ? inode.getGroupName(snapshotId) : name;
        if (groups.contains(group)) {
          userIsGroupMember = true;
          if (entry.getPermission().implies(access)) {
            matchingEntry = entry;
          }
        }
      } else if (type == AclEntryType.MASK) {
        // Save mask for later.
        mask = entry;
      } else {
        // Use other entry if no match and user not a member of groups that
        // denied access.
        if (matchingEntry == null && !userIsGroupMember) {
          matchingEntry = entry;
        }
        break;
      }
    }

    // Determine final enforced permissions by applying the mask if necessary.
    final FsAction enforcedPerm;
    if (matchingEntry != null) {
      AclEntryType type = matchingEntry.getType();
      String name = matchingEntry.getName();
      if ((type == AclEntryType.USER && name == null) ||
          (type == AclEntryType.OTHER)) {
        // Owner and other entries are not masked.
        enforcedPerm = matchingEntry.getPermission();
      } else {
        // Named user, owning group and named group entries are masked.
        enforcedPerm = matchingEntry.getPermission().and(mask.getPermission());
      }
    } else {
      // This happens if user was a member of any groups, but access was denied.
      enforcedPerm = null;
    }

    // Enforce the chosen permissions.
    if (enforcedPerm == null || !enforcedPerm.implies(access)) {
      throw new AccessControlException(
        toAccessControlString(inode, snapshotId, access, acl));
    }
  }

  /** Guarded by {@link FSNamesystem#readLock()} */
  private void checkStickyBit(INode parent, INode inode, int snapshotId
      ) throws AccessControlException {
    if(!parent.getFsPermission(snapshotId).getStickyBit()) {
      return;
    }

    // If this user is the directory owner, return
    if(parent.getUserName(snapshotId).equals(user)) {
      return;
    }

    // if this user is the file owner, return
    if(inode.getUserName(snapshotId).equals(user)) {
      return;
    }

    throw new AccessControlException("Permission denied by sticky bit setting:" +
      " user=" + user + ", inode=" + inode);
  }

  /**
   * Whether a cache pool can be accessed by the current context
   *
   * @param pool CachePool being accessed
   * @param access type of action being performed on the cache pool
   * @throws AccessControlException if pool cannot be accessed
   */
  public void checkPermission(CachePool pool, FsAction access)
      throws AccessControlException {
    FsPermission mode = pool.getMode();
    if (isSuperUser()) {
      return;
    }
    if (user.equals(pool.getOwnerName())
        && mode.getUserAction().implies(access)) {
      return;
    }
    if (groups.contains(pool.getGroupName())
        && mode.getGroupAction().implies(access)) {
      return;
    }
    if (mode.getOtherAction().implies(access)) {
      return;
    }
    throw new AccessControlException("Permission denied while accessing pool "
        + pool.getPoolName() + ": user " + user + " does not have "
        + access.toString() + " permissions.");
  }
}
