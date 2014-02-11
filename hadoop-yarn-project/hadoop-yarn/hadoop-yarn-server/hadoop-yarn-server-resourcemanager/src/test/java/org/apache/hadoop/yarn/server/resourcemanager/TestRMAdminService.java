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

package org.apache.hadoop.yarn.server.resourcemanager;

import static org.junit.Assert.fail;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hadoop.security.authorize.ServiceAuthorizationManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshAdminAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshQueuesRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshServiceAclsRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RefreshSuperUserGroupsConfigurationRequest;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacitySchedulerConfiguration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class TestRMAdminService {

  private final Configuration configuration = new YarnConfiguration();
  private MockRM rm = null;
  private FileSystem fs;
  private Path workingPath;
  private Path tmpDir;

  @Before
  public void setup() throws IOException {
    Configuration.addDefaultResource(YarnConfiguration.CS_CONFIGURATION_FILE);
    fs = FileSystem.get(configuration);
    workingPath =
        new Path(new File("target", this.getClass().getSimpleName()
            + "-remoteDir").getAbsolutePath());
    configuration.set(YarnConfiguration.FS_BASED_RM_CONF_STORE,
        workingPath.toString());
    tmpDir = new Path(new File("target", this.getClass().getSimpleName()
        + "-tmpDir").getAbsolutePath());
    fs.delete(workingPath, true);
    fs.delete(tmpDir, true);
    fs.mkdirs(workingPath);
    fs.mkdirs(tmpDir);
  }

  @After
  public void tearDown() throws IOException {
    if (rm != null) {
      rm.stop();
    }
    fs.delete(workingPath, true);
    fs.delete(tmpDir, true);
  }

  @Test
  public void testAdminRefreshQueuesWithLocalConfigurationProvider()
      throws IOException, YarnException {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    int maxAppsBefore = cs.getConfiguration().getMaximumSystemApplications();

    try {
      rm.adminService.refreshQueues(RefreshQueuesRequest.newInstance());
      Assert.assertEquals(maxAppsBefore, cs.getConfiguration()
          .getMaximumSystemApplications());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void testAdminRefreshQueuesWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
      fail("Should throw an exception");
    } catch(Exception ex) {
      // Expect exception here
    }

    //upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    CapacityScheduler cs =
        (CapacityScheduler) rm.getRMContext().getScheduler();
    int maxAppsBefore = cs.getConfiguration().getMaximumSystemApplications();

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    csConf.set("yarn.scheduler.capacity.maximum-applications", "5000");
    uploadConfiguration(csConf, "capacity-scheduler.xml");

    rm.adminService.refreshQueues(RefreshQueuesRequest.newInstance());

    int maxAppsAfter = cs.getConfiguration().getMaximumSystemApplications();
    Assert.assertEquals(maxAppsAfter, 5000);
    Assert.assertTrue(maxAppsAfter != maxAppsBefore);
  }

  @Test
  public void testAdminAclsWithLocalConfigurationProvider() {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    try {
      rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void testAdminAclsWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
      fail("Should throw an exception");
    } catch(Exception ex) {
      // Expect exception here
    }

    //upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    String aclStringBefore =
        rm.adminService.getAccessControlList().getAclString().trim();

    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.YARN_ADMIN_ACL, "world:anyone:rwcda");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    rm.adminService.refreshAdminAcls(RefreshAdminAclsRequest.newInstance());

    String aclStringAfter =
        rm.adminService.getAccessControlList().getAclString().trim();

    Assert.assertTrue(!aclStringAfter.equals(aclStringBefore));
    Assert.assertEquals(aclStringAfter, "world:anyone:rwcda");
  }

  @Test
  public void testServiceAclsRefreshWithLocalConfigurationProvider() {
    configuration.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    ResourceManager resourceManager = null;

    try {
      resourceManager = new ResourceManager();
      resourceManager.init(configuration);
      resourceManager.start();
      resourceManager.adminService.refreshServiceAcls(RefreshServiceAclsRequest
          .newInstance());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    } finally {
      if (resourceManager != null) {
        resourceManager.stop();
      }
    }
  }

  @Test
  public void testServiceAclsRefreshWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.setBoolean(
        CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    ResourceManager resourceManager = null;
    try {
      try {
        resourceManager = new ResourceManager();
        resourceManager.init(configuration);
        resourceManager.start();
        fail("Should throw an exception");
      } catch (Exception ex) {
        // expect to get an exception here
      }

      //upload default configurations
      uploadDefaultConfiguration();
      Configuration conf = new Configuration();
      conf.setBoolean(
          CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, true);
      uploadConfiguration(conf, "core-site.xml");
      try {
        resourceManager = new ResourceManager();
        resourceManager.init(configuration);
        resourceManager.start();
      } catch (Exception ex) {
        fail("Should not get any exceptions");
      }

      String aclsString = "alice,bob users,wheel";
      Configuration newConf = new Configuration();
      newConf.set("security.applicationclient.protocol.acl", aclsString);
      uploadConfiguration(newConf, "hadoop-policy.xml");

      resourceManager.adminService.refreshServiceAcls(RefreshServiceAclsRequest
          .newInstance());

      // verify service Acls refresh for AdminService
      ServiceAuthorizationManager adminServiceServiceManager =
          resourceManager.adminService.getServer()
              .getServiceAuthorizationManager();
      verifyServiceACLsRefresh(adminServiceServiceManager,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs refresh for ClientRMService
      ServiceAuthorizationManager clientRMServiceServiceManager =
          resourceManager.getRMContext().getClientRMService().getServer()
              .getServiceAuthorizationManager();
      verifyServiceACLsRefresh(clientRMServiceServiceManager,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs refresh for ApplicationMasterService
      ServiceAuthorizationManager appMasterService =
          resourceManager.getRMContext().getApplicationMasterService()
              .getServer().getServiceAuthorizationManager();
      verifyServiceACLsRefresh(appMasterService,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);

      // verify service ACLs refresh for ResourceTrackerService
      ServiceAuthorizationManager RTService =
          resourceManager.getRMContext().getResourceTrackerService()
              .getServer().getServiceAuthorizationManager();
      verifyServiceACLsRefresh(RTService,
          org.apache.hadoop.yarn.api.ApplicationClientProtocolPB.class,
          aclsString);
    } finally {
      if (resourceManager != null) {
        resourceManager.stop();
      }
    }
  }

  private void verifyServiceACLsRefresh(ServiceAuthorizationManager manager,
      Class<?> protocol, String aclString) {
    for (Class<?> protocolClass : manager.getProtocolsWithAcls()) {
      AccessControlList accessList =
          manager.getProtocolsAcls(protocolClass);
      if (protocolClass == protocol) {
        Assert.assertEquals(accessList.getAclString(),
            aclString);
      } else {
        Assert.assertEquals(accessList.getAclString(), "*");
      }
    }
  }

  @Test
  public void
      testRefreshSuperUserGroupsWithLocalConfigurationProvider() {
    rm = new MockRM(configuration);
    rm.init(configuration);
    rm.start();

    try {
      rm.adminService.refreshSuperUserGroupsConfiguration(
          RefreshSuperUserGroupsConfigurationRequest.newInstance());
    } catch (Exception ex) {
      fail("Using localConfigurationProvider. Should not get any exception.");
    }
  }

  @Test
  public void
      testRefreshSuperUserGroupsWithFileSystemBasedConfigurationProvider()
      throws IOException, YarnException {
    configuration.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
      fail("Should throw an exception");
    } catch(Exception ex) {
      // Expect exception here
    }

    //upload default configurations
    uploadDefaultConfiguration();

    try {
      rm = new MockRM(configuration);
      rm.init(configuration);
      rm.start();
    } catch(Exception ex) {
      fail("Should not get any exceptions");
    }

    Configuration coreConf = new Configuration(false);
    coreConf.set("hadoop.proxyuser.test.groups", "test_groups");
    coreConf.set("hadoop.proxyuser.test.hosts", "test_hosts");
    uploadConfiguration(coreConf, "core-site.xml");

    rm.adminService.refreshSuperUserGroupsConfiguration(
        RefreshSuperUserGroupsConfigurationRequest.newInstance());
    Assert.assertTrue(ProxyUsers.getProxyGroups()
        .get("hadoop.proxyuser.test.groups").size() == 1);
    Assert.assertTrue(ProxyUsers.getProxyGroups()
        .get("hadoop.proxyuser.test.groups").contains("test_groups"));

    Assert.assertTrue(ProxyUsers.getProxyHosts()
        .get("hadoop.proxyuser.test.hosts").size() == 1);
    Assert.assertTrue(ProxyUsers.getProxyHosts()
        .get("hadoop.proxyuser.test.hosts").contains("test_hosts"));
  }

  private String writeConfigurationXML(Configuration conf, String confXMLName)
      throws IOException {
    DataOutputStream output = null;
    try {
      final File confFile = new File(tmpDir.toString(), confXMLName);
      if (confFile.exists()) {
        confFile.delete();
      }
      if (!confFile.createNewFile()) {
        Assert.fail("Can not create " + confXMLName);
      }
      output = new DataOutputStream(
          new FileOutputStream(confFile));
      conf.writeXml(output);
      return confFile.getAbsolutePath();
    } finally {
      if (output != null) {
        output.close();
      }
    }
  }

  private void uploadToRemoteFileSystem(Path filePath)
      throws IOException {
    fs.copyFromLocalFile(filePath, workingPath);
  }

  private void uploadConfiguration(Configuration conf, String confFileName)
      throws IOException {
    String csConfFile = writeConfigurationXML(conf, confFileName);
    // upload the file into Remote File System
    uploadToRemoteFileSystem(new Path(csConfFile));
  }

  private void uploadDefaultConfiguration() throws IOException {
    Configuration conf = new Configuration();
    uploadConfiguration(conf, "core-site.xml");

    YarnConfiguration yarnConf = new YarnConfiguration();
    yarnConf.set(YarnConfiguration.RM_CONFIGURATION_PROVIDER_CLASS,
        "org.apache.hadoop.yarn.FileSystemBasedConfigurationProvider");
    uploadConfiguration(yarnConf, "yarn-site.xml");

    CapacitySchedulerConfiguration csConf =
        new CapacitySchedulerConfiguration();
    uploadConfiguration(csConf, "capacity-scheduler.xml");

    Configuration hadoopPolicyConf = new Configuration(false);
    hadoopPolicyConf
        .addResource(YarnConfiguration.HADOOP_POLICY_CONFIGURATION_FILE);
    uploadConfiguration(hadoopPolicyConf, "hadoop-policy.xml");
  }
}
