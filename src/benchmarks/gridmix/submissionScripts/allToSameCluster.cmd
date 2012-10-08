@echo off
@rem Licensed to the Apache Software Foundation (ASF) under one or more
@rem contributor license agreements.  See the NOTICE file distributed with
@rem this work for additional information regarding copyright ownership.
@rem The ASF licenses this file to You under the Apache License, Version 2.0
@rem (the "License"); you may not use this file except in compliance with
@rem the License.  You may obtain a copy of the License at
@rem
@rem     http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing, software
@rem distributed under the License is distributed on an "AS IS" BASIS,
@rem WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
@rem See the License for the specific language governing permissions and
@rem limitations under the License.


SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

REM The original submits all the scripts concurrently then waits for them all
REM This windows-version currently submits stage-by-stage.  
REM This is due to WaitAllGridmix.cmd not being parameterized.
REM It can be remedied by using different window-titles per script and parameterizing the wait 

CALL %GRID_MIX_HOME%\submissionScripts\maxentToSameCluster 2>&1 > maxentToSameCluster.out 
CALL %GRID_MIX_HOME%\submissionScripts\textSortToSameCluster 2>&1 > textSortToSameCluster.out
CALL %GRID_MIX_HOME%\submissionScripts\monsterQueriesToSameCluster 2>&1 > monsterQueriesToSameCluster.out
CALL %GRID_MIX_HOME%\submissionScripts\webdataScanToSameCluster 2>&1 > webdataScanToSameCluster.out
CALL %GRID_MIX_HOME%\submissionScripts\webdataSortToSameCluster  2>&1 > webdataSortToSameCluster.out

CALL "%GRID_MIX_HOME%\submissionScripts\WaitAllGridmix.cmd"
