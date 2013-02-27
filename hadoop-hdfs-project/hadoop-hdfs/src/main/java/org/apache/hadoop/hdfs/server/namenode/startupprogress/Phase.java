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

@InterfaceAudience.Private
public enum Phase {
  LOADING_FSIMAGE("LoadingFsImage", "Loading fsimage"),
  LOADING_EDITS("LoadingEdits", "Loading edits"),
  SAVING_CHECKPOINT("SavingCheckpoint", "Saving checkpoint"),
  SAFEMODE("SafeMode", "Safe mode"),
  COMPLETE("Complete", "Complete");

  static EnumSet<Phase> VISIBLE_PHASES = EnumSet.range(LOADING_FSIMAGE,
    SAFEMODE);

  private final String name, description;

  public String getDescription() {
    return description;
  }

  public String getName() {
    return name;
  }

  private Phase(String name, String description) {
    this.name = name;
    this.description = description;
  }
}
