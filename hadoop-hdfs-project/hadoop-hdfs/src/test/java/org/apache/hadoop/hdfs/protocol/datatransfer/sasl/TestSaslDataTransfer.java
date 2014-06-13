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
package org.apache.hadoop.hdfs.protcol.datatransfer.sasl;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSaslDataTransfer {

  private static String TEST_FILE_CONTENT = "testing SASL";

  private static MiniDFSCluster cluster;
  private static HdfsConfiguration conf;
  private static FileSystem fs;
  private static MiniKdc kdc;
  private static int pathCount = 0;
  private static Path path;

  @BeforeClass
  public static void init() throws Exception {
    File workDir = new File(System.getProperty("test.build.dir",
      "target/test-dir"));
    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, workDir);
    kdc.start();

    String userName = UserGroupInformation.getLoginUser().getShortUserName();
    File keytab = new File(workDir, userName + ".keytab");
    kdc.createPrincipal(keytab, userName + "/localhost", "HTTP/localhost");
    String hdfsPrincipal = userName + "/localhost@" + kdc.getRealm();
    String spnegoPrincipal = "HTTP/localhost@" + kdc.getRealm();

    conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab.getAbsolutePath());
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY,
      "authentication,integrity,privacy");

    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
  }

  @AfterClass
  public static void shutdown() {
    IOUtils.cleanup(null, fs);
    if (cluster != null) {
      cluster.shutdown();
    }
    if (kdc != null) {
      kdc.stop();
    }
  }

  @Before
  public void setUp() throws Exception {
    pathCount += 1;
    path = new Path("/p" + pathCount);
  }

  @Test
  public void testAuthentication() throws Exception {
    doTestForQop("authentication");
  }

  @Test
  public void testIntegrity() throws Exception {
    doTestForQop("integrity");
  }

  @Test
  public void testPrivacy() throws Exception {
    doTestForQop("privacy");
  }

  /**
   * Tests DataTransferProtocol with a specific QOP requested by the client.
   *
   * @param qop String QOP to test
   * @throws IOException if there is an I/O error
   */
  private static void doTestForQop(String qop) throws IOException {
    Configuration fsConf = new Configuration(conf);
    fsConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, qop);
    fs = FileSystem.get(cluster.getURI(), fsConf);
    createFile();
    String fileContent = DFSTestUtil.readFile(fs, path);
    assertEquals(TEST_FILE_CONTENT, fileContent);
    BlockLocation[] blockLocations = fs.getFileBlockLocations(path, 0,
      Long.MAX_VALUE);
    assertNotNull(blockLocations);
    assertEquals(1, blockLocations.length);
    assertNotNull(blockLocations[0].getHosts());
    assertEquals(3, blockLocations[0].getHosts().length);
  }

  /**
   * Creates a file at the testing path.
   *
   * @throws IOException if there is an I/O error
   */
  private static void createFile() throws IOException {
    OutputStream os = null;
    try {
      os = fs.create(path);
      os.write(TEST_FILE_CONTENT.getBytes("UTF-8"));
    } finally {
      IOUtils.cleanup(null, os);
    }
  }
}
