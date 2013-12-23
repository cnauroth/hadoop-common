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

import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Unit tests covering FSPermissionChecker.
 */
public class TestFSPermissionChecker {
  private static final long PREFERRED_BLOCK_SIZE = 128 * 1024 * 1024;
  private static final short REPLICATION = 3;
  private static final String SUPERGROUP = "supergroup";
  private static final String SUPERUSER = "superuser";
  private static final UserGroupInformation BRUCE =
    UserGroupInformation.createUserForTesting("bruce", new String[] { });
  private static final UserGroupInformation DIANA =
    UserGroupInformation.createUserForTesting("diana", new String[] { "sales" });
  private static final UserGroupInformation CLARK =
    UserGroupInformation.createUserForTesting("clark", new String[] { "execs" });

  private INodeDirectory inodeRoot;

  @Before
  public void setUp() {
    PermissionStatus permStatus = PermissionStatus.createImmutable(SUPERUSER,
      SUPERGROUP, FsPermission.createImmutable((short)0755));
    inodeRoot = new INodeDirectory(INodeId.ROOT_INODE_ID,
      INodeDirectory.ROOT_NAME, permStatus, 0L);
  }

  @Test
  public void testAclOwner() throws IOException {
    INodeFile inodeFile = createINodeFile(inodeRoot, "file1", "bruce", "execs",
      (short)0640);
    addAcl(inodeFile,
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, USER, "diana", READ),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, MASK, READ),
      aclEntry(ACCESS, OTHER, NONE));
    assertPermissionGranted(BRUCE, "/file1", READ);
    assertPermissionGranted(BRUCE, "/file1", WRITE);
    assertPermissionGranted(BRUCE, "/file1", READ_WRITE);
    assertPermissionDenied(BRUCE, "/file1", EXECUTE);
    assertPermissionGranted(DIANA, "/file1", READ);
    assertPermissionDenied(DIANA, "/file1", WRITE);
  }

  @Test
  public void testAclNamedUser() {
  }

  @Test
  public void testAclNamedUserMask() {
  }

  @Test
  public void testAclGroup() {
  }

  @Test
  public void testAclGroupDeny() {
  }

  @Test
  public void testAclGroupMask() {
  }

  @Test
  public void testAclNamedGroup() {
  }

  @Test
  public void testAclNamedGroupDeny() {
  }

  @Test
  public void testAclNamedGroupMask() {
  }

  @Test
  public void testAclOther() {
  }

  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      FsAction permission) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .setPermission(permission)
      .build();
  }

  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      String name, FsAction permission) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .setName(name)
      .setPermission(permission)
      .build();
  }

  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type,
      String name) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .setName(name)
      .build();
  }

  private static AclEntry aclEntry(AclEntryScope scope, AclEntryType type) {
    return new AclEntry.Builder()
      .setScope(scope)
      .setType(type)
      .build();
  }

  private void addAcl(INodeWithAdditionalFields inode, AclEntry... acl) {
    AclFeature aclFeature = new AclFeature();
    aclFeature.setEntries(Arrays.asList(acl));
    inode.addAclFeature(aclFeature);
  }

  private void assertPermissionGranted(UserGroupInformation user, String path,
      FsAction access) throws IOException {
    new FSPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(path,
      inodeRoot, false, null, null, access, null, true);
  }

  private void assertPermissionDenied(UserGroupInformation user, String path,
      FsAction access) throws IOException {
    try {
      new FSPermissionChecker(SUPERUSER, SUPERGROUP, user).checkPermission(path,
        inodeRoot, false, null, null, access, null, true);
      fail("expected AccessControlException for user + " + user + ", path = " +
        path + ", access = " + access);
    } catch (AccessControlException e) {
      // expected
    }
  }

  private static INodeFile createINodeFile(INodeDirectory parent, String name,
      String owner, String group, short perm) throws IOException {
    PermissionStatus permStatus = PermissionStatus.createImmutable(owner, group,
      FsPermission.createImmutable(perm));
    INodeFile inodeFile = new INodeFile(INodeId.GRANDFATHER_INODE_ID,
      name.getBytes("UTF-8"), permStatus, 0L, 0L, null, REPLICATION,
      PREFERRED_BLOCK_SIZE);
    parent.addChild(inodeFile);
    return inodeFile;
  }
}
