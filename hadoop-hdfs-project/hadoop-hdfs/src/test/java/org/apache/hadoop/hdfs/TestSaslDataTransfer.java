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
package org.apache.hadoop.hdfs;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.junit.Assert.*;

import java.io.File;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.KerberosSecurityTestcase;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestSaslDataTransfer extends KerberosSecurityTestcase {

  private MiniDFSCluster cluster;
  private HdfsConfiguration conf;
  private FileSystem fs;

  @Before
  public void init() throws Exception {
    File hdfsKtb = new File(getWorkDir(), "hdfs.keytab");
    getKdc().createPrincipal(hdfsKtb, "hdfs/localhost");
    String hdfsKeytabPath = hdfsKtb.getAbsolutePath();

    conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeytabPath);
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY,
      "hdfs/localhost@" + getKdc().getRealm());
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, hdfsKeytabPath);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
      "hdfs/localhost@" + getKdc().getRealm());
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, hdfsKeytabPath);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY,
      "hdfs/localhost@" + getKdc().getRealm());
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "auth,auth-int,auth-conf");
    //cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();
  }

  @After
  public void shutdown() {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
  }

  @Test
  public void testClientAuth() throws Exception {
    DFSTestUtil.createFile(fs, new Path("/file1"), 1024, (short)3, 0);
    String file1Content = DFSTestUtil.readFile(fs, new Path("/file1"));
  }
}
