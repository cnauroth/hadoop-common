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
@rem  Setup envrionment variables
@rem
if not defined HADOOP_INSTALL_ROOT ( 
  set HADOOP_INSTALL_ROOT=c:\hadoop
)
if not defined CORE_INSTALL_PATH ( 
  set CORE_INSTALL_PATH=%~dp0
)
if not defined WINPKG_LOG ( 
  set WINPKG_LOG=%CORE_INSTALL_PATH%\@final.name@-winpkg.log
)
set HADOOP_INSTALL_DIR=%HADOOP_INSTALL_ROOT%\@final.name@
set HADOOP_INSTALL_BIN=%HADOOP_INSTALL_DIR%\bin


@rem ensure running as admin.
reg query "HKEY_USERS\S-1-5-19\Environment" /v TEMP 2>&1 | findstr /I /C:"REG_EXPAND_SZ" 2>&1 > NUL
If %ERRORLEVEL% EQU 1 (
  echo FATAL ERROR: install script must be run elevated 
  endlocal
  goto :eof
)

@rem java needs to be installed 
if not exist %JAVA_HOME%\bin\java.exe (
  echo FATAL ERROR: %JAVA_HOME%\bin\java.exe does not exist JAVA_HOME not set properly  
  endlocal
  goto :eof
)

@rem
@rem  Begin install
@rem
echo Installing Apache Hadoop @final.name@ to %HADOOP_INSTALL_ROOT%
echo Installing Apache Hadoop @final.name@ to %HADOOP_INSTALL_ROOT% >> %WINPKG_LOG%

@rem
@rem  Choose what services to install
@rem    CORE_MASTER -> namenode secondarynamenode jobtracker tasktracker
@rem    CORE_SLAVE -> datanode tasktracker
@rem    CORE_ONEBOX || nothing -> namenode jobtracker tasktracker datanode
@rem
set HDFS_ROLE_SERVICES=namenode datanode secondarynamenode 
set MAPRED_ROLE_SERVICES=jobtracker tasktracker 

if "%1" == "CORE_MASTER" (
  set HDFS_ROLE_SERVICES=namenode datanode secondarynamenode 
  set MAPRED_ROLE_SERVICES=jobtracker tasktracker
)

if "%1" == "CORE_SLAVE" ( 
  set HDFS_ROLE_SERVICES=datanode 
  set MAPRED_ROLE_SERVICES=tasktracker 
)

@rem
@rem  Lay down the bits 
@rem
echo Extracting @package.zip@ to %HADOOP_INSTALL_ROOT% >> %WINPKG_LOG%
"%CORE_INSTALL_PATH%\..\resources\unzip.exe" "%CORE_INSTALL_PATH%\..\resources\@package.zip@" "%HADOOP_INSTALL_ROOT%" >> "%WINPKG_LOG%"
xcopy /EIYF "%CORE_INSTALL_PATH%\..\template" "%HADOOP_INSTALL_DIR%" >> "%WINPKG_LOG%"

@rem
@rem  Create services 
@rem
@rem TODO: make service run with obj= "NT Authority\Network Service"

for %%i in (%HDFS_ROLE_SERVICES% %MAPRED_ROLE_SERVICES%) do (
  echo Creating %%i service >> %WINPKG_LOG%
  copy "%CORE_INSTALL_PATH%\..\resources\servicehost.exe" "%HADOOP_INSTALL_DIR%\bin\%%i.exe" /y /d  >> "%WINPKG_LOG%"
  
  "%windir%\system32\sc.exe" create %%i binPath= "%HADOOP_INSTALL_BIN%\%%i.exe" DisplayName= "Hadoop %%i Service"  >> "%WINPKG_LOG%"
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