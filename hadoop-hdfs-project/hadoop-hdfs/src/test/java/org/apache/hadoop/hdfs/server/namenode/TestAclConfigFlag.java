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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.AclException;
import org.apache.hadoop.io.IOUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Lists;

/**
 * Tests that the configuration flag that controls support for ACLs is off by
 * default and causes all attempted operations related to ACLs to fail.
 */
public class TestAclConfigFlag {
  private static final Path PATH = new Path("/path");

  private static MiniDFSCluster cluster;
  private static DistributedFileSystem fs;

  @Rule
  public ExpectedException exception = ExpectedException.none();

  @BeforeClass
  public static void init() throws Exception {
    Configuration conf = new Configuration();
    // not explicitly setting ACLs enabled to false, should be false by default
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

  @Test
  public void testModifyAclEntries() throws Exception {
    fs.mkdirs(PATH);
    expectException();
    fs.modifyAclEntries(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));
  }

  @Test
  public void testRemoveAclEntries() throws Exception {
    fs.mkdirs(PATH);
    expectException();
    fs.removeAclEntries(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));
  }

  @Test
  public void testRemoveDefaultAcl() throws Exception {
    fs.mkdirs(PATH);
    expectException();
    fs.removeAclEntries(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));
  }

  @Test
  public void testRemoveAcl() throws Exception {
    fs.mkdirs(PATH);
    expectException();
    fs.removeAcl(PATH);
  }

  @Test
  public void testSetAcl() throws Exception {
    fs.mkdirs(PATH);
    expectException();
    fs.setAcl(PATH, Lists.newArrayList(
      aclEntry(DEFAULT, USER, "foo", READ_WRITE)));
  }

  @Test
  public void testGetAclStatus() throws Exception {
    fs.mkdirs(PATH);
    expectException();
    fs.getAclStatus(PATH);
  }

  /**
   * We expect an AclException, and we want the exception text to state the
   * configuration key that controls ACL support.
   */
  private void expectException() {
    exception.expect(AclException.class);
    exception.expectMessage(DFSConfigKeys.DFS_NAMENODE_ACLS_ENABLED_KEY);
  }
}
