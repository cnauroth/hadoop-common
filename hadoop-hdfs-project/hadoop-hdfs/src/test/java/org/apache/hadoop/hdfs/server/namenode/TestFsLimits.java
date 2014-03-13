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

import static org.apache.hadoop.hdfs.server.common.Util.fileAsURI;
import static org.apache.hadoop.util.Time.now;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSLimitException.MaxDirectoryItemsExceededException;
import org.apache.hadoop.hdfs.protocol.FSLimitException.PathComponentTooLongException;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.NamenodeRole;
import org.junit.Before;
import org.junit.Test;

public class TestFsLimits {
  static Configuration conf;
  static INode[] inodes;
  static FSDirectory fs;
  static boolean fsIsReady;
  
  static PermissionStatus perms
    = new PermissionStatus("admin", "admin", FsPermission.getDefault());

  static private FSImage getMockFSImage() {
    FSEditLog editLog = mock(FSEditLog.class);
    FSImage fsImage = mock(FSImage.class);
    when(fsImage.getEditLog()).thenReturn(editLog);
    return fsImage;
  }

  static private FSNamesystem getMockNamesystem() {
    FSNamesystem fsn = mock(FSNamesystem.class);
    when(
        fsn.createFsOwnerPermissions((FsPermission)anyObject())
    ).thenReturn(
         new PermissionStatus("root", "wheel", FsPermission.getDefault())
    );
    return fsn;
  }
  
  private static class MockFSDirectory extends FSDirectory {
    public MockFSDirectory() throws IOException {
      super(getMockFSImage(), getMockNamesystem(), conf);
      setReady(fsIsReady);
      NameNode.initMetrics(conf, NamenodeRole.NAMENODE);
    }
  }

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    conf.set(DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY,
             fileAsURI(new File(MiniDFSCluster.getBaseDirectory(),
                                "namenode")).toString());

    fs = null;
    fsIsReady = true;
  }

  @Test
  public void testDefaultMaxComponentLength() {
    int maxComponentLength = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);
    assertEquals(0, maxComponentLength);
  }
  
  @Test
  public void testDefaultMaxDirItems() {
    int maxDirItems = conf.getInt(
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_DEFAULT);
    assertEquals(0, maxDirItems);
  }

  @Test
  public void testNoLimits() throws Exception {
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", null);
    mkdirs("/4444", null);
    mkdirs("/55555", null);
    mkdirs("/" + HdfsConstants.DOT_SNAPSHOT_DIR,
        HadoopIllegalArgumentException.class);
  }

  @Test
  public void testMaxComponentLength() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", PathComponentTooLongException.class);
    mkdirs("/4444", PathComponentTooLongException.class);
  }

  @Test
  public void testMaxComponentLengthRename() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 2);

    mkdirs("/5", null);
    rename("/5", "/555", PathComponentTooLongException.class);
    rename("/5", "/55", null);

    mkdirs("/6", null);
    deprecatedRename("/6", "/666", PathComponentTooLongException.class);
    deprecatedRename("/6", "/66", null);
  }

  @Test
  public void testMaxDirItems() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", MaxDirectoryItemsExceededException.class);
    mkdirs("/4444", MaxDirectoryItemsExceededException.class);
  }

  @Test
  public void testMaxDirItemsRename() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/2", null);

    mkdirs("/2/3", null);
    rename("/2/3", "/3", MaxDirectoryItemsExceededException.class);
    rename("/2/3", "/1/3", null);

    mkdirs("/2/4", null);
    deprecatedRename("/2/4", "/4", MaxDirectoryItemsExceededException.class);
    deprecatedRename("/2/4", "/1/4", null);
  }

  @Test
  public void testMaxComponentsAndMaxDirItems() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", MaxDirectoryItemsExceededException.class);
    mkdirs("/4444", PathComponentTooLongException.class);
  }

  @Test
  public void testDuringEditLogs() throws Exception {
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY, 3);
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_DIRECTORY_ITEMS_KEY, 2);
    fsIsReady = false;
    
    mkdirs("/" + HdfsConstants.DOT_SNAPSHOT_DIR,
        HadoopIllegalArgumentException.class);
    mkdirs("/1", null);
    mkdirs("/22", null);
    mkdirs("/333", null);
    mkdirs("/4444", null);
  }

  private void mkdirs(String name, Class<?> expected)
  throws Exception {
    lazyInitFSDirectory();
    Class<?> generated = null;
    try {
      fs.mkdirs(name, perms, false, now());
    } catch (Throwable e) {
      generated = e.getClass();
      e.printStackTrace();
    }
    assertEquals(expected, generated);
  }

  private void rename(String src, String dst, Class<?> expected)
      throws Exception {
    lazyInitFSDirectory();
    Class<?> generated = null;
    try {
      fs.renameTo(src, dst, false, new Rename[] { });
    } catch (Throwable e) {
      generated = e.getClass();
      e.printStackTrace();
    }
    assertEquals(expected, generated);
  }

  @SuppressWarnings("deprecation")
  private void deprecatedRename(String src, String dst, Class<?> expected)
      throws Exception {
    lazyInitFSDirectory();
    Class<?> generated = null;
    try {
      fs.renameTo(src, dst, false);
    } catch (Throwable e) {
      generated = e.getClass();
    }
    assertEquals(expected, generated);
  }

  private static void lazyInitFSDirectory() throws IOException {
    // have to create after the caller has had a chance to set conf values
    if (fs == null) {
      fs = new MockFSDirectory();
    }
  }
}
