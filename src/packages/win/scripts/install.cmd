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

set HADOOP_INSTALL_ROOT=c:\hadoop

echo Installing Apache Hadoop @version@

@rem ensure running as admin.
reg query "HKEY_USERS\S-1-5-19\Environment" /v TEMP 2>&1 | findstr /I /C:"REG_EXPAND_SZ" 2>&1 > NUL
If %ERRORLEVEL% EQU 1 (
  echo "ERROR: install script must be run elevated"
  goto :eof
)

set INSTALL_PATH=%~dp0
set HADOOP_INSTALL_DIR=%HADOOP_INSTALL_ROOT%\@final.name@
set HADOOP_INSTALL_BIN=%HADOOP_INSTALL_DIR%\bin

@rem
@rem  Choose what services to install
@rem
set HDFS_ROLE_SERVICES=namenode datanode secondarynamenode 
set MAPRED_ROLE_SERVICES=jobtracker tasktracker 

if "%1" == "master" (
  set HDFS_ROLE_SERVICES=namenode datanode secondarynamenode 
  set MAPRED_ROLE_SERVICES=jobtracker tasktracker
)

if "%1" == "slave" ( 
  set HDFS_ROLE_SERVICES=datanode 
  set MAPRED_ROLE_SERVICES=tasktracker 
)

@rem
@rem  Lay down the bits 
@rem
echo Extracting @package.zip@ to %HADOOP_INSTALL_ROOT%
"%INSTALL_PATH%\..\resources\unzip.exe" "%INSTALL_PATH%\..\resources\@package.zip@" "%HADOOP_INSTALL_ROOT%"
xcopy /EIYQ "%INSTALL_PATH%\..\template" "%HADOOP_INSTALL_DIR%"

@rem
@rem  Create services 
@rem
for %%i in (namenode datanode secondarynamenode jobtracker tasktracker ) do (
echo Created %%i service
copy "%INSTALL_PATH%\..\resources\winsw.exe" "%HADOOP_INSTALL_DIR%\bin\%%i.exe" /y /d 
@rem TODO: make service run with obj= "NT Authority\Network Service"
"%windir%\system32\sc.exe" create %%i binPath= "%HADOOP_INSTALL_BIN%\%%i.exe" DisplayName= "Hadoop %%i Service" 
)

@rem
@rem  Setup HDFS service config
@rem
for %%j in (%HDFS_ROLE_SERVICES%) do (
cmd /c "%HADOOP_INSTALL_BIN%\hdfs.cmd" --service %%j > "%HADOOP_INSTALL_BIN%\%%j.xml"
)

@rem
@rem  Setup MapRed service config
@rem
for %%k in (%MAPRED_ROLE_SERVICES%) do (
cmd /c "%HADOOP_INSTALL_BIN%\mapred.cmd" --service %%k > "%HADOOP_INSTALL_BIN%\%%k.xml"
)
@rem TODO create event log for services

endlocal