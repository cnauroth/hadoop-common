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
package org.apache.hadoop.util;

import java.io.BufferedReader;
import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Shell command for determining if a process with a specified pid is alive.
 * This class encapsulates the platform-specific details of determining if a
 * process is alive.
 */
@InterfaceAudience.LimitedPrivate({"HDFS", "MapReduce"})
@InterfaceStability.Unstable
public class ProcessIsAlive extends Shell {

  private boolean isAlive;
  private final String pid;

  /**
   * Creates a new ProcessIsAlive command to check if the specified pid is alive.
   * 
   * @param pid String pid
   */
  public ProcessIsAlive(String pid) throws IOException {
    this.pid = pid;
    run();
  }

  /**
   * Returns true if the command determines that the process is alive.
   * 
   * @return boolean true if the command determines that the process is alive
   */
  public boolean isAlive() {
    return isAlive;
  }

  @Override
  protected String[] getExecString() {
    return WINDOWS ? new String[] { WINUTILS, "task", "isAlive", pid } :
      new String[] { "kill", "-0", isSetsidAvailable ? "-" + pid : pid };
  }

  @Override
  protected void parseExecResult(BufferedReader lines) throws IOException {
    // On Windows, if winutils found the requested process, then output will
    // contain "IsAlive".  On other platforms, output is not relevant.  (Only
    // exit code matters.)
    String line = lines.readLine();
    if (line != null) {
      isAlive = line.contains("IsAlive");
    }
  }

  @Override
  protected void run() throws IOException {
    try {
      super.run();
      if (!WINDOWS) {
        // On non-Windows, if the check returns a 0 exit code, then the requested
        // process is alive.
        isAlive = true;
      }
    } catch (ExitCodeException e) {
      if (WINDOWS) {
        // If winutils cannot find the requested process, then this is indicated
        // in the command output, not the exit code.  Therefore, any non-zero
        // exit code is an exception.
        throw e;
      } else {
        // On other platforms, a non-zero exit code indicates the requested
        // process is not found.
        isAlive = false;
      }
    }
  }
}
