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
package org.apache.hadoop.yarn.client.cli;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeHealthStatus;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.util.BuilderUtils;
import org.apache.hadoop.yarn.util.Records;
import org.junit.Before;
import org.junit.Test;

public class TestYarnCLI {

  private static final String LINE_SEPARATOR =
    System.getProperty("line.separator");

  private YarnClient client = mock(YarnClient.class);
  ByteArrayOutputStream sysOutStream;
  private PrintStream sysOut;
  ByteArrayOutputStream sysErrStream;
  private PrintStream sysErr;

  @Before
  public void setup() {
    sysOutStream = new ByteArrayOutputStream();
    sysOut = spy(new PrintStream(sysOutStream));
    sysErrStream = new ByteArrayOutputStream();
    sysErr = spy(new PrintStream(sysErrStream));
  }
  
  @Test
  public void testGetApplicationReport() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = BuilderUtils.newApplicationId(1234, 5);
    ApplicationReport newApplicationReport = BuilderUtils.newApplicationReport(
        applicationId, BuilderUtils.newApplicationAttemptId(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A");
    when(client.getApplicationReport(any(ApplicationId.class))).thenReturn(
        newApplicationReport);
    int result = cli.run(new String[] { "-status", applicationId.toString() });
    assertEquals(0, result);
    verify(client).getApplicationReport(applicationId);
    String appReportStr = StringUtils.join(LINE_SEPARATOR, Arrays.asList(
      "Application Report : ",
      "\tApplication-Id : application_1234_0005",
      "\tApplication-Name : appname",
      "\tUser : user",
      "\tQueue : queue",
      "\tStart-Time : 0",
      "\tFinish-Time : 0",
      "\tState : FINISHED",
      "\tFinal-State : SUCCEEDED",
      "\tTracking-URL : N/A",
      "\tDiagnostics : diagnostics",
      "")); // for final line separator
    Assert.assertEquals(appReportStr, sysOutStream.toString());
    verify(sysOut, times(1)).println(isA(String.class));
  }

  @Test
  public void testGetAllApplications() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = BuilderUtils.newApplicationId(1234, 5);
    ApplicationReport newApplicationReport = BuilderUtils.newApplicationReport(
        applicationId, BuilderUtils.newApplicationAttemptId(applicationId, 1),
        "user", "queue", "appname", "host", 124, null,
        YarnApplicationState.FINISHED, "diagnostics", "url", 0, 0,
        FinalApplicationStatus.SUCCEEDED, null, "N/A");
    List<ApplicationReport> applicationReports = new ArrayList<ApplicationReport>();
    applicationReports.add(newApplicationReport);
    when(client.getApplicationList()).thenReturn(applicationReports);
    int result = cli.run(new String[] { "-list" });
    assertEquals(0, result);
    verify(client).getApplicationList();

    String appsReport = StringUtils.join(LINE_SEPARATOR, Arrays.asList(
      "Total Applications:1",
      "                Application-Id\t    Application-Name"
      + "\t      User\t     Queue\t             State\t       "
      + "Final-State\t                       Tracking-URL",
      "         application_1234_0005\t             "
      + "appname\t      user\t     queue\t          FINISHED\t         "
      + "SUCCEEDED\t                                N/A",
      "")); // for final line separator
    Assert.assertEquals(appsReport.toString(), sysOutStream.toString());
    verify(sysOut, times(1)).write(any(byte[].class), anyInt(), anyInt());
  }

  @Test
  public void testKillApplication() throws Exception {
    ApplicationCLI cli = createAndGetAppCLI();
    ApplicationId applicationId = BuilderUtils.newApplicationId(1234, 5);
    int result = cli.run(new String[] { "-kill", applicationId.toString() });
    assertEquals(0, result);
    verify(client).killApplication(any(ApplicationId.class));
    verify(sysOut).println("Killing application application_1234_0005");
  }

  @Test
  public void testListClusterNodes() throws Exception {
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(getNodeReports(3));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    int result = cli.run(new String[] { "-list" });
    assertEquals(0, result);
    verify(client).getNodeReports();
    String nodesReport = StringUtils.join(LINE_SEPARATOR, Arrays.asList(
      "Total Nodes:3",
      "         Node-Id\tNode-State\tNode-Http-Address\t"
      + "Health-Status(isNodeHealthy)\tRunning-Containers",
      "         host0:0\t   RUNNING\t       host1:8888"
      + "\t                     false\t                 0",
      "         host1:0\t   RUNNING\t       host1:8888"
      + "\t                     false\t                 0",
      "         host2:0\t   RUNNING\t       host1:8888"
      + "\t                     false\t                 0",
      "")); // for final line separator
    Assert.assertEquals(nodesReport, sysOutStream.toString());
    verify(sysOut, times(1)).write(any(byte[].class), anyInt(), anyInt());
  }

  @Test
  public void testNodeStatus() throws Exception {
    NodeId nodeId = BuilderUtils.newNodeId("host0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(getNodeReports(3));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    int result = cli.run(new String[] { "-status", nodeId.toString() });
    assertEquals(0, result);
    verify(client).getNodeReports();
    String nodeStatus = StringUtils.join(LINE_SEPARATOR, Arrays.asList(
      "Node Report : ",
      "\tNode-Id : host0:0",
      "\tRack : rack1",
      "\tNode-State : RUNNING",
      "\tNode-Http-Address : host1:8888",
      "\tHealth-Status(isNodeHealthy) : false",
      "\tLast-Last-Health-Update : 0",
      "\tHealth-Report : null",
      "\tContainers : 0",
      "\tMemory-Used : 0M",
      "\tMemory-Capacity : 0",
      "")); // for final line separator
    verify(sysOut, times(1)).println(isA(String.class));
    verify(sysOut).println(nodeStatus);
  }

  @Test
  public void testAbsentNodeStatus() throws Exception {
    NodeId nodeId = BuilderUtils.newNodeId("Absenthost0", 0);
    NodeCLI cli = new NodeCLI();
    when(client.getNodeReports()).thenReturn(getNodeReports(0));
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    cli.setSysErrPrintStream(sysErr);
    int result = cli.run(new String[] { "-status", nodeId.toString() });
    assertEquals(0, result);
    verify(client).getNodeReports();
    verify(sysOut, times(1)).println(isA(String.class));
    verify(sysOut).println(
      "Could not find the node report for node id : " + nodeId.toString());
  }

  @Test
  public void testAppCLIUsageInfo() throws Exception {
    verifyUsageInfo(new ApplicationCLI());
  }

  @Test
  public void testNodeCLIUsageInfo() throws Exception {
    verifyUsageInfo(new NodeCLI());
  }

  private void verifyUsageInfo(YarnCLI cli) throws Exception {
    cli.setSysErrPrintStream(sysErr);
    cli.run(new String[0]);
    verify(sysErr).println("Invalid Command Usage : ");
  }

  private List<NodeReport> getNodeReports(int noOfNodes) {
    List<NodeReport> nodeReports = new ArrayList<NodeReport>();

    for (int i = 0; i < noOfNodes; i++) {
      NodeReport nodeReport = BuilderUtils.newNodeReport(BuilderUtils
          .newNodeId("host" + i, 0), NodeState.RUNNING, "host" + 1 + ":8888",
          "rack1", Records.newRecord(Resource.class), Records
              .newRecord(Resource.class), 0, Records
              .newRecord(NodeHealthStatus.class));
      nodeReports.add(nodeReport);
    }
    return nodeReports;
  }

  private ApplicationCLI createAndGetAppCLI() {
    ApplicationCLI cli = new ApplicationCLI();
    cli.setClient(client);
    cli.setSysOutPrintStream(sysOut);
    return cli;
  }

}
