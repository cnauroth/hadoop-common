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
setlocal enabledelayedexpansion

@rem
@rem  Setup environment variables
@rem
if not defined HADOOP_INSTALL_ROOT ( 
  set HADOOP_INSTALL_ROOT=c:\hadoop
)
if not defined INSTALL_SCRIPT_ROOT ( 
  set INSTALL_SCRIPT_ROOT=%~dp0
)

set HADOOP_INSTALL_DIR=%HADOOP_INSTALL_ROOT%\@final.name@

echo Uninstalling Apache Hadoop @final.name@

@rem ensure running as admin.
reg query "HKEY_USERS\S-1-5-19\Environment" /v TEMP 2>&1 | findstr /I /C:"REG_EXPAND_SZ" 2>&1 > NUL
If %ERRORLEVEL% EQU 1 (
  echo FATAL ERROR: uninstall script must be run elevated
  goto :eof
)

@rem Root directory used for HDFS dn and nn files, and for mapred local dir
@rem TODO: Configs should be configurable from the outside and properly
@rem embedded into hdfs-site.xml/mapred-site.xml (same as on Unix)
set HDFS_DATA_DIR=c:\hdfs

@rem
@rem  Stop and delete services 
@rem
for %%i in (namenode datanode secondarynamenode jobtracker tasktracker) do (
  "%windir%\system32\net.exe" stop %%i
  "%windir%\system32\sc.exe" delete %%i
)

@rem Make sure we recursively delete only if the corresponding env variables are
@rem set, otherwise we could end up deleting some unwanted files.
if not defined HADOOP_INSTALL_DIR (
  echo FATAL ERROR: HADOOP_INSTALL_DIR is not set
  goto :eof
)
if not defined HDFS_DATA_DIR (
  echo FATAL ERROR: HDFS_DATA_DIR is not set
  goto :eof
)
if not defined HADOOP_INSTALL_ROOT (
  echo FATAL ERROR: HADOOP_INSTALL_ROOT is not set
  goto :eof
)

@rem
@rem  Delete Hadoop install dir
@rem
echo deleting %HADOOP_INSTALL_DIR%
rd /s /q "%HADOOP_INSTALL_DIR%"
rd /s /q "%HADOOP_INSTALL_ROOT%"

@rem
@rem  Delete Hadoop HDFS data dir
@rem
echo deleting %HDFS_DATA_DIR%
rd /s /q "%HDFS_DATA_DIR%"

@rem TODO: Workaround for HADOOP-64, explicitely taking the ownership and adding full permissions
@rem so that we can delete the below folders
takeown.exe /f "%HDFS_DATA_DIR%\*" /A /R /D Y
icacls "%HDFS_DATA_DIR%" /grant BUILTIN\Administrators:F /T /L
takeown.exe /f "%HADOOP_INSTALL_DIR%\*" /A /R /D Y
icacls "%HADOOP_INSTALL_DIR%" /grant BUILTIN\Administrators:F /T /L
rd /s /q "%HADOOP_INSTALL_DIR%"
rd /s /q "%HDFS_DATA_DIR%"
rd /s /q "%HADOOP_INSTALL_ROOT%"

endlocal