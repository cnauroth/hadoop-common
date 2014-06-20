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
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY;
import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSaslDataTransfer {

  private static final File BASEDIR = new File(
    System.getProperty("test.build.dir", "target/test-dir"),
    TestSaslDataTransfer.class.getSimpleName());
  private static final Path PATH  = new Path("/file1");
  private static final String TEST_FILE_CONTENT = "testing SASL";

  private static String hdfsPrincipal;
  private static MiniKdc kdc;
  private static String keytab;
  private static String spnegoPrincipal;

  private MiniDFSCluster cluster;
  private FileSystem fs;

  @BeforeClass
  public static void init() throws Exception {
    FileUtil.fullyDelete(BASEDIR);
    assertTrue(BASEDIR.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, BASEDIR);
    kdc.start();

    String userName = UserGroupInformation.getLoginUser().getShortUserName();
    File keytabFile = new File(BASEDIR, userName + ".keytab");
    keytab = keytabFile.getAbsolutePath();
    kdc.createPrincipal(keytabFile, userName + "/localhost", "HTTP/localhost");
    hdfsPrincipal = userName + "/localhost@" + kdc.getRealm();
    spnegoPrincipal = "HTTP/localhost@" + kdc.getRealm();
  }

  @AfterClass
  public static void shutdown() {
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(BASEDIR);
  }

  @After
  public void tearDown() {
    IOUtils.cleanup(null, fs);
    fs = null;
    if (cluster != null) {
      cluster.shutdown();
      cluster = null;
    }
  }

  @Test
  public void testAuthentication() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig(
      "authentication,integrity,privacy");
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    doTest(clientConf);
  }

  @Test
  public void testIntegrity() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig(
      "authentication,integrity,privacy");
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "integrity");
    doTest(clientConf);
  }

  @Test
  public void testPrivacy() throws Exception {
    HdfsConfiguration clusterConf = createSecureConfig(
      "authentication,integrity,privacy");
    startCluster(clusterConf);
    HdfsConfiguration clientConf = new HdfsConfiguration(clusterConf);
    clientConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "privacy");
    doTest(clientConf);
  }

  @Test
  public void testServerDoesNotSupportDesiredQop() throws Exception {
    // TODO
  }

  @Test
  public void testSecureClientInsecureCluster() throws Exception {
    // TODO
  }

  @Test
  public void testSecureClientInsecureClusterFallback() throws Exception {
    // TODO
  }

  /**
   * Creates a file at the testing path.
   *
   * @throws IOException if there is an I/O error
   */
  private void createFile() throws IOException {
    OutputStream os = null;
    try {
      os = fs.create(PATH);
      os.write(TEST_FILE_CONTENT.getBytes("UTF-8"));
    } finally {
      IOUtils.cleanup(null, os);
    }
  }

  /**
   * Tests DataTransferProtocol with a specific QOP requested by the client.
   *
   * @param conf client configuration
   * @throws IOException if there is an I/O error
   */
  private void doTest(HdfsConfiguration conf) throws IOException {
    fs = FileSystem.get(cluster.getURI(), conf);
    createFile();
    assertEquals(TEST_FILE_CONTENT, DFSTestUtil.readFile(fs, PATH));
    BlockLocation[] blockLocations = fs.getFileBlockLocations(PATH, 0,
      Long.MAX_VALUE);
    assertNotNull(blockLocations);
    assertEquals(1, blockLocations.length);
    assertNotNull(blockLocations[0].getHosts());
    assertEquals(3, blockLocations[0].getHosts().length);
    // TODO: multi-block
  }

  /**
   * Starts a cluster with the given configuration.
   *
   * @param conf cluster configuration
   * @throws IOException if there is an I/O error
   */
  private void startCluster(HdfsConfiguration conf) throws IOException {
    cluster = new MiniDFSCluster.Builder(conf).numDataNodes(3).build();
    cluster.waitActive();
  }

  /**
   * Creates configuration for starting a secure cluster.
   *
   * @param dataTransferProtection supported QOPs
   * @return configuration for starting a secure cluster
   * @throws Exception if there is any failure
   */
  private static HdfsConfiguration createSecureConfig(
      String dataTransferProtection) throws Exception {
    HdfsConfiguration conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(AuthenticationMethod.KERBEROS, conf);
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hdfsPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoPrincipal);
    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, dataTransferProtection);
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    String keystoresDir = BASEDIR.getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(
      TestSaslDataTransfer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
    return conf;
  }
}
