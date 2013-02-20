/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hdfs.server.namenode.StartupProgress.Phase;
import org.znerd.xmlenc.XMLOutputter;

@SuppressWarnings("serial")
public class StartupProgressServlet extends DfsServlet {

  private static final String COUNT = "count";
  private static final String ELAPSED_TIME = "elapsedTime";
  private static final String NAME = "name";
  private static final String PERCENT_COMPLETE = "percentComplete";
  private static final String TOTAL = "total";

  public static final String PATH_SPEC = "/startupProgress";

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    StartupProgress prog = NameNode.getStartupProgress();
    XMLOutputter xml = new XMLOutputter(resp.getWriter(), "UTF-8");
    xml.declaration();

    try {
      resp.setContentType("application/xml; charset=UTF-8");
      xml.startTag("StartupProgress");

      for (Phase phase: StartupProgress.getVisiblePhases()) {
        xml.startTag("Phase");
        xml.attribute(NAME, phase.getName());
        attribute(xml, COUNT, prog.getCount(phase));
        attribute(xml, TOTAL, prog.getTotal(phase));
        attribute(xml, PERCENT_COMPLETE, prog.getPercentComplete(phase));
        attribute(xml, ELAPSED_TIME, prog.getElapsedTime(phase));

        for (String step: prog.getSteps(phase)) {
          xml.startTag("Step");
          xml.attribute(NAME, step);
          attribute(xml, COUNT, prog.getCount(phase, step));
          attribute(xml, TOTAL, prog.getTotal(phase, step));
          attribute(xml, PERCENT_COMPLETE, prog.getPercentComplete(phase, step));
          attribute(xml, ELAPSED_TIME, prog.getElapsedTime(phase, step));
          xml.endTag();
        }

        xml.endTag();
      }

      xml.endTag();
    } catch (IOException e) {
      writeXml(e, "/startupProgress", xml);
    }
  }

  private static void attribute(XMLOutputter xml, String name, Object value)
      throws IOException {
    xml.attribute(name, String.valueOf(value));
  }
}
