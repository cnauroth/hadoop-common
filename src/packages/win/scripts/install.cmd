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
set HADOOP_INSTALL_BIN=%HADOOP_INSTALL_DIR%\bin

@rem ensure running as admin.
reg query "HKEY_USERS\S-1-5-19\Environment" /v TEMP 2>&1 | findstr /I /C:"REG_EXPAND_SZ" 2>&1 > NUL
if %ERRORLEVEL% NEQ 0 (
  echo FATAL ERROR: install script must be run elevated
  goto :eof
)

if not exist %JAVA_HOME%\bin\java.exe (
  echo FATAL ERROR: JAVA_HOME not set properly, %JAVA_HOME%\bin\java.exe does not exist
  goto :eof
)

@rem Root directory used for HDFS dn and nn files, and for mapred local dir
@rem TODO: Configs should be configurable from the outside and properly
@rem embedded into hdfs-site.xml/mapred-site.xml (same as on Unix)
set HDFS_DATA_DIR=c:\hdfs

@rem
@rem  Begin install
@rem
echo Installing Apache Hadoop @final.name@ to %HADOOP_INSTALL_DIR%

@rem skip first two arguments
shift
shift

@rem
@rem  Parse command line
@rem
if not "%1"=="-username" (
  echo FATAL ERROR: Invalid command line '%*'
  call :usage
  goto :eof
)

set HADOOP_USERNAME=%2
if [%HADOOP_USERNAME%]==[] (
  echo FATAL ERROR: Username empty
  call :usage
  goto :eof
)

if not "%3"=="-password" (
  echo FATAL ERROR: Invalid command line '%*'
  call :usage
  goto :eof
)

set HADOOP_PASSWORD=%4
if [%HADOOP_PASSWORD%]==[] (
  echo FATAL ERROR: Password empty
  call :usage
  goto :eof
)

@rem
@rem  Setup default lists of services to install
@rem
if not defined MASTER_HDFS (
  set MASTER_HDFS=namenode datanode secondarynamenode
)

if not defined MASTER_MR (
  set MASTER_MR=jobtracker tasktracker
)

if not defined SLAVE_HDFS (
  set SLAVE_HDFS=datanode
)

if not defined SLAVE_MR (
  set SLAVE_MR=tasktracker
)

if not defined ONEBOX_HDFS (
  set ONEBOX_HDFS=namenode datanode
)

if not defined ONEBOX_MR (
  set ONEBOX_MR=jobtracker tasktracker
)

if not "%5"=="-hdfsrole" (
  echo FATAL ERROR: Invalid command line
  call :usage
  goto :eof
)

set HDFS_ROLE=%6
if [%HDFS_ROLE%]==[] (
  echo FATAL ERROR: hdfs role empty
  call :usage
  goto :eof
)

if not "%7"=="-mapredrole" (
  echo FATAL ERROR: Invalid command line
  call :usage
  goto :eof
)

set MR_ROLE=%8
if [%MR_ROLE%]==[] (
  echo FATAL ERROR: mapred list empty
  call :usage
  goto :eof
)

set HDFS_ROLE_SERVICES=!%HDFS_ROLE%!
set MAPRED_ROLE_SERVICES=!%MR_ROLE%!

echo Node HDFS services: %HDFS_ROLE_SERVICES%
echo Node MapReduce services: %MAPRED_ROLE_SERVICES%

@rem Set HADOOP_HOME as a global environment variable
@rem TODO: This should not be needed after HADOOP-60
setx /m HADOOP_HOME %HADOOP_INSTALL_DIR%

@rem
@rem  Extract Hadoop distribution from compressed tarballs
@rem
echo Extracting Hadoop to %HADOOP_INSTALL_ROOT%
"%INSTALL_SCRIPT_ROOT%\..\resources\unzip.exe" "%INSTALL_SCRIPT_ROOT%\..\resources\@final.name@.zip" "%HADOOP_INSTALL_ROOT%"
xcopy /EIYF "%INSTALL_SCRIPT_ROOT%\..\template" "%HADOOP_INSTALL_DIR%"

@rem
@rem  Grant Hadoop user access to HADOOP_INSTALL_DIR and HDFS root
@rem
icacls "%HADOOP_INSTALL_DIR%" /grant %HADOOP_USERNAME%:(OI)(CI)F
if not exist "%HDFS_DATA_DIR%" mkdir "%HDFS_DATA_DIR%"
icacls "%HDFS_DATA_DIR%" /grant %HADOOP_USERNAME%:(OI)(CI)F

@rem
@rem  Create Hadoop Windows services and grant users ACLS to start/stop
@rem
set LogonString=obj= .\%HADOOP_USERNAME% password= %HADOOP_PASSWORD%

for %%i in (%HDFS_ROLE_SERVICES% %MAPRED_ROLE_SERVICES%) do (
  echo Creating %%i service
  copy "%INSTALL_SCRIPT_ROOT%\..\resources\servicehost.exe" "%HADOOP_INSTALL_DIR%\bin\%%i.exe" /y /d

  "%windir%\system32\sc.exe" create %%i binPath= "%HADOOP_INSTALL_BIN%\%%i.exe" DisplayName= "Hadoop %%i Service" %LogonString%
  "%windir%\system32\sc.exe" failure %%i reset= 30 actions= restart/5000
  call :let_auth_users_control_services %%i
  "%windir%\system32\sc.exe" config %%i start= auto
)

@rem
@rem  Setup HDFS service config
@rem
for %%j in (%HDFS_ROLE_SERVICES%) do (
  echo Creating service config "%HADOOP_INSTALL_BIN%\%%j.xml"
  cmd /c "%HADOOP_INSTALL_BIN%\hdfs.cmd" --service %%j > "%HADOOP_INSTALL_BIN%\%%j.xml" 
)

@rem
@rem  Setup MapRed service config
@rem
for %%k in (%MAPRED_ROLE_SERVICES%) do (
  echo Creating service config "%HADOOP_INSTALL_BIN%\%%k.xml"
  cmd /c "%HADOOP_INSTALL_BIN%\mapred.cmd" --service %%k > "%HADOOP_INSTALL_BIN%\%%k.xml"
)

@rem
@rem  Format the namenode
@rem
@call %HADOOP_INSTALL_BIN%\hadoop.cmd namenode -format

goto :eof

@rem
@rem ------------------------------ Usage -------------------------------------
@rem
:usage
  echo Usage: install.cmd -username [USERNAME] -password [PASSWORD] -hdfsrole HdfsOption -mapredrole MapRedOption
  echo HdfsOption could be: MASTER_HDFS or SLAVE_HDFS or ONEBOX_HDFS
  echo MapRedOption could be: MASTER_MR or SLAVE_MR or ONEBOX_MR
  goto :eof

:let_auth_users_control_services
  @rem Add service control permissions to authenticated users.
  @rem Reference:
  @rem   http://stackoverflow.com/questions/4436558/start-stop-a-windows-service-from-a-non-administrator-user-account 
  @rem   http://msmvps.com/blogs/erikr/archive/2007/09/26/set-permissions-on-a-specific-service-windows.aspx
  for /f "delims=" %%a in ('sc sdshow %1 ^| findstr /v "linux"') do @set _CurrentSD=%%a
  set _SearchStr=S:(
  set _ReplaceStr=(A;;RPWPCR;;;AU)S:(
  echo Original %1 service Security Descriptor: %_CurrentSD%
  set _NewSD=!_CurrentSD:%_SearchStr%=%_ReplaceStr%!
  echo New %1 service Security Descriptor: %_NewSD%
  sc sdset %1 %_NewSD%
  goto :eof
