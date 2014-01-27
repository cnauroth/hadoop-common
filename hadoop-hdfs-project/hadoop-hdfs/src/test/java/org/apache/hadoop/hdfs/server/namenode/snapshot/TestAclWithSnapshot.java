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
package org.apache.hadoop.hdfs.server.namenode.snapshot;

import static org.apache.hadoop.hdfs.server.namenode.AclTestHelpers.*;
import static org.apache.hadoop.fs.permission.AclEntryScope.*;
import static org.apache.hadoop.fs.permission.AclEntryType.*;
import static org.apache.hadoop.fs.permission.FsAction.*;
import static org.junit.Assert.*;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.protocol.NSQuotaExceededException;
import org.apache.hadoop.hdfs.protocol.SnapshotAccessControlException;
import org.apache.hadoop.hdfs.server.namenode.NameNode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeAdapter;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

/**
 * Tests interaction of ACLs with snapshots.
 */
public class TestAclWithSnapshot {
  private static final UserGroupInformation BRUCE =
    UserGroupInformation.createUserForTesting("bruce", new String[] { });
  private static final UserGroupInformation DIANA =
    UserGroupInformation.createUserForTesting("diana", new String[] { });

  private static MiniDFSCluster cluster;
  private static Configuration conf;
  private static FileSystem fsAsBruce;
  private static FileSystem fsAsDiana;
  private static DistributedFileSystem hdfs;
  private static int pathCount = 0;
  private static Path path;
  private static String snapshotName;
  private static Path snapshotPath;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws Exception {
    conf = new Configuration();
    initCluster(true);
  }

  @AfterClass
  public static void shutdown() throws Exception {
    IOUtils.cleanup(null, hdfs, fsAsBruce, fsAsDiana);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Before
  public void setUp() {
    ++pathCount;
    path = new Path("/p" + pathCount);
    snapshotName = "snapshot" + pathCount;
    snapshotPath = new Path(path, new Path(".snapshot", snapshotName));
  }

  @Test
  public void testOriginalAclEnforcedForSnapshotAfterChange() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(path, aclSpec);
    // assert bruce can access path
    // assert diana cannot access path
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    // check getAclStatus for path
    // check getAclStatus for snapshot
    // assert bruce can access snapshot
    // assert diana cannot access snapshot
    aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "diana", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(path, aclSpec);
    // check getAclStatus for path
    // check getAclStatus for snapshot
    // assert bruce cannot access path
    // assert diana can access path
    // assert bruce can access snapshot
    // assert diana cannot access snapshot
    doCheckpointAndRestart();
    // check getAclStatus for path
    // check getAclStatus for snapshot
    // assert bruce cannot access path
    // assert diana can access path
    // assert bruce can access snapshot
    // assert diana cannot access snapshot
    fail();
  }

  @Test
  public void testOriginalAclEnforcedForSnapshotAfterRemoval() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(ACCESS, USER, ALL),
      aclEntry(ACCESS, USER, "bruce", READ_EXECUTE),
      aclEntry(ACCESS, GROUP, NONE),
      aclEntry(ACCESS, OTHER, NONE));
    hdfs.setAcl(path, aclSpec);
    // assert bruce can access path
    // assert diana cannot access path
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    // check getAclStatus for path
    // check getAclStatus for snapshot
    // assert bruce can access snapshot
    // assert diana cannot access snapshot
    hdfs.removeAcl(path);
    // check getAclStatus for path
    // check getAclStatus for snapshot
    // assert bruce cannot access path
    // assert diana cannot access path
    // assert bruce can access snapshot
    // assert diana cannot access snapshot
    doCheckpointAndRestart();
    // check getAclStatus for path
    // check getAclStatus for snapshot
    // assert bruce cannot access path
    // assert diana can access path
    // assert bruce can access snapshot
    // assert diana cannot access snapshot
    fail();
  }

  @Test
  public void testDefaultAclNotCopiedToAccessAclOfNewSnapshot()
      throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", READ_EXECUTE));
    hdfs.modifyAclEntries(path, aclSpec);
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    // assert bruce cannot access snapshot
    // check getAclStatus for path
    // check getAclStatus for snapshot
    fail();
  }

  @Test
  public void testModifyAclEntriesSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", READ_EXECUTE));
    exception.expect(SnapshotAccessControlException.class);
    hdfs.modifyAclEntries(snapshotPath, aclSpec);
  }

  @Test
  public void testRemoveAclEntriesSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce"));
    exception.expect(SnapshotAccessControlException.class);
    hdfs.removeAclEntries(snapshotPath, aclSpec);
  }

  @Test
  public void testRemoveDefaultAclSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    exception.expect(SnapshotAccessControlException.class);
    hdfs.removeDefaultAcl(snapshotPath);
  }

  @Test
  public void testRemoveAclSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    exception.expect(SnapshotAccessControlException.class);
    hdfs.removeAcl(snapshotPath);
  }

  @Test
  public void testSetAclSnapshotPath() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce"));
    exception.expect(SnapshotAccessControlException.class);
    hdfs.setAcl(snapshotPath, aclSpec);
  }

  @Test
  public void testChangeAclExceedsQuota() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    hdfs.setQuota(path, 2, HdfsConstants.QUOTA_DONT_SET);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", READ_EXECUTE));
    hdfs.modifyAclEntries(path, aclSpec);
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", NONE));
    exception.expect(NSQuotaExceededException.class);
    hdfs.modifyAclEntries(path, aclSpec);
  }

  @Test
  public void testRemoveAclExceedsQuota() throws Exception {
    FileSystem.mkdirs(hdfs, path, FsPermission.createImmutable((short)0700));
    hdfs.setQuota(path, 2, HdfsConstants.QUOTA_DONT_SET);
    List<AclEntry> aclSpec = Lists.newArrayList(
      aclEntry(DEFAULT, USER, "bruce", READ_EXECUTE));
    hdfs.modifyAclEntries(path, aclSpec);
    SnapshotTestHelper.createSnapshot(hdfs, path, snapshotName);
    exception.expect(NSQuotaExceededException.class);
    hdfs.removeAcl(path);
  }

  /**
   * Enter safe mode, save a new checkpoint, and restart NameNode.
   *
   * @throws Exception if any step fails
   */
  private static void doCheckpointAndRestart() throws Exception {
    NameNode nameNode = cluster.getNameNode();
    NameNodeAdapter.enterSafeMode(nameNode, false);
    NameNodeAdapter.saveNamespace(nameNode);
    shutdown();
    initCluster(false);
  }

  /**
   * Initialize the cluster, wait for it to become active, and get FileSystem
   * instances for our test users.
   *
   * @param format if true, format the NameNode and DataNodes before starting up
   * @throws Exception if any step fails
   */
  private static void initCluster(boolean format) throws Exception {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).format(format)
      .build();
    cluster.waitActive();
    FileSystem fs = cluster.getFileSystem();
    assertTrue(fs instanceof DistributedFileSystem);
    hdfs = (DistributedFileSystem)fs;
    fsAsBruce = DFSTestUtil.getFileSystemAs(BRUCE, conf);
    fsAsDiana = DFSTestUtil.getFileSystemAs(DIANA, conf);
  }
}
