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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.hdfs.server.namenode.startupprogress.Phase;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgress;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StartupProgressView;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.Step;
import org.apache.hadoop.hdfs.server.namenode.startupprogress.StepType;
import org.mortbay.util.ajax.JSON;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Servlet that provides a JSON representation of the namenode's current startup
 * progress.
 */
@InterfaceAudience.Private
@SuppressWarnings("serial")
public class StartupProgressServlet extends DfsServlet {

  private static final String COUNT = "count";
  private static final String ELAPSED_TIME = "elapsedTime";
  private static final String FILE = "file";
  private static final String NAME = "name";
  private static final String PERCENT_COMPLETE = "percentComplete";
  private static final String PHASES = "phases";
  private static final String SIZE = "size";
  private static final String STATUS = "status";
  private static final String STEPS = "steps";
  private static final String TOTAL = "total";

  public static final String PATH_SPEC = "/startupProgress";

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp)
      throws IOException {
    resp.setContentType("application/json; charset=UTF-8");
    StartupProgress prog = NameNodeHttpServer.getStartupProgressFromContext(
      getServletContext());
    StartupProgressView view = prog.createView();
    List<Map<String, Object>> phases = new ArrayList<Map<String, Object>>();

    for (Phase phase: view.getPhases()) {
      Map<String, Object> phaseMap = new LinkedHashMap<String, Object>();
      phaseMap.put(NAME, phase.getName());
      phaseMap.put(STATUS, view.getStatus(phase));
      phaseMap.put(PERCENT_COMPLETE, view.getPercentComplete(phase));
      phaseMap.put(ELAPSED_TIME, view.getElapsedTime(phase));
      List<Map<String, Object>> steps = new ArrayList<Map<String, Object>>();

      for (Step step: view.getSteps(phase)) {
        Map<String, Object> stepMap = new LinkedHashMap<String, Object>();
        StepType type = step.getType();
        String name = type != null ? type.getName() : null;
        putIfNotNull(stepMap, NAME, name);
        stepMap.put(COUNT, view.getCount(phase, step));
        putIfNotNull(stepMap, FILE, step.getFile());
        putIfNotNull(stepMap, SIZE, step.getSize());
        stepMap.put(TOTAL, view.getTotal(phase, step));
        stepMap.put(PERCENT_COMPLETE, view.getPercentComplete(phase, step));
        stepMap.put(ELAPSED_TIME, view.getElapsedTime(phase, step));
        steps.add(stepMap);
      }

      phaseMap.put(STEPS, steps);
      phases.add(phaseMap);
    }

    Map<String, Object> respMap = new LinkedHashMap<String, Object>();
    respMap.put(ELAPSED_TIME, view.getElapsedTime());
    respMap.put(PERCENT_COMPLETE, view.getPercentComplete());
    respMap.put(PHASES, phases);
    resp.getWriter().println(JSON.toString(respMap));
  }

  /**
   * Puts an entry into a map only if the value is non-null.
   * 
   * @param map Map<String, Object> to put
   * @param key String key to put
   * @param value Object value to put
   */
  private static void putIfNotNull(Map<String, Object> map, String key,
      Object value) {
    if (value != null) {
      map.put(key, value);
    }
  }
}
