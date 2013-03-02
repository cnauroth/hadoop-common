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
package org.apache.hadoop.hdfs.server.namenode.startupprogress;

import java.util.EnumSet;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Indicates a particular phase of the namenode startup sequence.  The phases
 * are listed here in their execution order.
 */
@InterfaceAudience.Private
public enum Phase {
  /**
   * The namenode is loading the fsimage file into memory.
   */
  LOADING_FSIMAGE("LoadingFsImage", "Loading fsimage"),

  /**
   * The namenode is loading the edits file and applying its operations to the
   * in-memory metadata.
   */
  LOADING_EDITS("LoadingEdits", "Loading edits"),

  /**
   * The namenode is saving a new checkpoint.
   */
  SAVING_CHECKPOINT("SavingCheckpoint", "Saving checkpoint"),

  /**
   * The namenode has entered safemode, awaiting block reports from data nodes.
   */
  SAFEMODE("SafeMode", "Safe mode"),

  /**
   * The namenode has completed startup.  This is an artificial state used
   * internally to indicate that startup has finished, but it is never seen by
   * users.
   */
  STARTUP_COMPLETE("StartupComplete", "Startup complete");

  /**
   * Contains the set of all phases visible to users.
   */
  static EnumSet<Phase> VISIBLE_PHASES = EnumSet.range(LOADING_FSIMAGE,
    SAFEMODE);

  private final String name, description;

  /**
   * Returns phase description.
   * 
   * @return String description
   */
  public String getDescription() {
    return description;
  }

  /**
   * Returns phase name.
   * 
   * @return String phase name
   */
  public String getName() {
    return name;
  }

  /**
   * Private constructor of enum.
   * 
   * @param name String phase name
   * @param description String phase description
   */
  private Phase(String name, String description) {
    this.name = name;
    this.description = description;
  }
}
