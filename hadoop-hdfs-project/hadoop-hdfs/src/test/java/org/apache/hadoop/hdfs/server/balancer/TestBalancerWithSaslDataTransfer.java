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
package org.apache.hadoop.hdfs.server.balancer;

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
import java.util.Properties;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestBalancerWithSaslDataTransfer {

  private static final File BASEDIR = new File(
    System.getProperty("test.build.dir", "target/test-dir"),
    TestBalancerWithSaslDataTransfer.class.getSimpleName());
  private static final TestBalancer TEST_BALANCER = new TestBalancer();

  private HdfsConfiguration conf;
  private MiniKdc kdc;

  @Before
  public void init() throws Exception {
    FileUtil.fullyDelete(BASEDIR);
    assertTrue(BASEDIR.mkdirs());

    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, BASEDIR);
    kdc.start();

    String userName = UserGroupInformation.getLoginUser().getShortUserName();
    File keytab = new File(BASEDIR, userName + ".keytab");
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
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    String keystoresDir = BASEDIR.getAbsolutePath();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(
      TestBalancerWithSaslDataTransfer.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, conf, false);
  }

  @After
  public void shutdown() {
    if (kdc != null) {
      kdc.stop();
    }
    FileUtil.fullyDelete(BASEDIR);
  }

  @Test
  public void testBalancer0Authentication() throws Exception {
    HdfsConfiguration testConf = new HdfsConfiguration(conf);
    testConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    TEST_BALANCER.testBalancer0Internal(testConf);
  }

  @Test
  public void testBalancer0Integrity() throws Exception {
    HdfsConfiguration testConf = new HdfsConfiguration(conf);
    testConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "integrity");
    TEST_BALANCER.testBalancer0Internal(testConf);
  }

  @Test
  public void testBalancer0Privacy() throws Exception {
    HdfsConfiguration testConf = new HdfsConfiguration(conf);
    testConf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "privacy");
    TEST_BALANCER.testBalancer0Internal(testConf);
  }
}
