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

import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

/**
 * Tests NameNode interaction for all ACL modification APIs.
 */
public class TestNameNodeAcl {

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fs;

  private Path path;
  private int pathCount = 0;

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @AfterClass
  public static void shutdown() throws Exception {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() {
    path = new Path("/p" + ++pathCount);
  }

  @Test
  public void testModifyAclEntries() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesOnlyAccess() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesOnlyDefault() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesMinimal() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesCustomMask() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesStickyBit() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesNullPath() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesPathNotFound() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesNullAclSpec() throws IOException {
    fail();
  }

  @Test
  public void testModifyAclEntriesDefaultOnFile() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclEntries() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclEntriesOnlyAccess() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclEntriesOnlyDefault() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclEntriesMinimal() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclEntriesStickyBit() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclEntriesNullPath() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclEntriesPathNotFound() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclEntriesNullAclSpec() throws IOException {
    fail();
  }

  @Test
  public void testRemoveDefaultAcl() throws IOException {
    fail();
  }

  @Test
  public void testRemoveDefaultAclOnlyAccess() throws IOException {
    fail();
  }

  @Test
  public void testRemoveDefaultAclOnlyDefault() throws IOException {
    fail();
  }

  @Test
  public void testRemoveDefaultAclStickyBit() throws IOException {
    fail();
  }

  @Test
  public void testRemoveDefaultAclNullPath() throws IOException {
    fail();
  }

  @Test
  public void testRemoveDefaultAclPathNotFound() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAcl() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclMinimalAcl() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclStickyBit() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclNullPath() throws IOException {
    fail();
  }

  @Test
  public void testRemoveAclPathNotFound() throws IOException {
    fail();
  }

  @Test
  public void testSetAcl() throws IOException {
    fail();
  }

  @Test
  public void testSetAclOnlyAccess() throws IOException {
    fail();
  }

  @Test
  public void testSetAclOnlyDefault() throws IOException {
    FileSystem.mkdirs(fs, path, FsPermission.createImmutable((short)0644));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE));
    fs.setAcl(path, aclSpec);
    AclStatus s = fs.getAclStatus(path);
    AclEntry[] returned = s.getEntries().toArray(new AclEntry[0]);
    assertArrayEquals(new AclEntry[] {
      aclEntry(ACCESS, USER, READ_WRITE),
      aclEntry(ACCESS, GROUP, READ),
      aclEntry(ACCESS, OTHER, READ),
      aclEntry(DEFAULT, USER, READ_WRITE),
      aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
      aclEntry(DEFAULT, GROUP, READ),
      aclEntry(DEFAULT, MASK, READ_EXECUTE),
      aclEntry(DEFAULT, OTHER, READ) }, returned);
    assertPermission((short)02644);
    assertAclFeature(true);
  }

  @Test
  public void testSetAclMinimal() throws IOException {
    fail();
  }

  @Test
  public void testSetAclCustomMask() throws IOException {
    fail();
  }

  @Test
  public void testSetAclStickyBit() throws IOException {
    fail();
  }

  @Test
  public void testSetAclNullPath() throws IOException {
    fail();
  }

  @Test
  public void testSetAclPathNotFound() throws IOException {
    fail();
  }

  @Test
  public void testSetAclNullAclSpec() throws IOException {
    fail();
  }

  @Test
  public void testSetAclDefaultOnFile() throws IOException {
    fail();
  }

  @Test
  public void testSetPermission() throws IOException {
    fail();
  }

  @Test
  public void testSetPermissionOnlyAccess() throws IOException {
    fail();
  }

  @Test
  public void testSetPermissionOnlyDefault() throws IOException {
    fail();
  }

  private void assertAclFeature(boolean expectAclFeature) throws IOException {
    INode inode = cluster.getNamesystem().getFSDirectory().getRoot()
      .getNode(path.toUri().getPath(), false);
    assertNotNull(inode);
    assertTrue(inode instanceof INodeWithAdditionalFields);
    AclFeature aclFeature = ((INodeWithAdditionalFields)inode).getAclFeature();
    if (expectAclFeature) {
      assertNotNull(aclFeature);
    } else {
      assertNull(aclFeature);
    }
  }

  private void assertPermission(short perm) throws IOException {
    assertEquals(FsPermission.createImmutable(perm),
      fs.getFileStatus(path).getPermission());
  }
}
