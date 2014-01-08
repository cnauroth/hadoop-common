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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;

public class TestNameNodeAcl {

  private static MiniDFSCluster cluster;
  private static Configuration conf;

  @BeforeClass
  public static void setUp() throws Exception {
    conf = new Configuration();

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testSetAcl() throws IOException {
    Path p = new Path("/p");
    FileSystem fs = cluster.getFileSystem();
    fs.create(p).close();
    AclEntry e = new AclEntry.Builder().setName("foo")
        .setPermission(READ_EXECUTE).setScope(DEFAULT).setType(USER).build();
    fs.setAcl(p, Lists.newArrayList(e));
    AclStatus s = fs.getAclStatus(p);
    AclEntry[] returned = Lists.newArrayList(s.getEntries()).toArray(
        new AclEntry[0]);
    Assert.assertArrayEquals(new AclEntry[] {
        aclEntry(ACCESS, USER, READ_WRITE),
        aclEntry(ACCESS, GROUP, READ),
        aclEntry(ACCESS, OTHER, READ),
        aclEntry(DEFAULT, USER, READ_WRITE),
        aclEntry(DEFAULT, USER, "foo", READ_EXECUTE),
        aclEntry(DEFAULT, GROUP, READ),
        aclEntry(DEFAULT, MASK, READ_EXECUTE),
        aclEntry(DEFAULT, OTHER, READ) }, returned);
  }
}
