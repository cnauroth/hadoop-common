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

set INDIR="%VARINFLTEXT%/{part-000*0,part-000*1,part-000*2}"
FOR /F "delims=" %%a in ('%CYGWIN_HOME%\bin\date +%%F-%%H-%%M-%%S-%%N') DO (
 SET DATE=%%a
)

set OUTDIR=perf-out/sort-out-dir-medium_%Date%
call %HADOOP_HOME%/bin/hadoop dfs -rmr %OUTDIR%

%HADOOP_HOME%/bin/hadoop jar %EXAMPLE_JAR% sort -m 1 -r %NUM_OF_REDUCERS_FOR_MEDIUM_JOB% -inFormat org.apache.hadoop.mapred.KeyValueTextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text %INDIR% %OUTDIR%

