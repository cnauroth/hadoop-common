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
if not defined CORE_UNINSTALL_PATH ( 
  set CORE_INSTALL_PATH=%~dp0
)
if not defined WINPKG_LOG ( 
  set WINPKG_LOG=%CORE_UNINSTALL_PATH%\@final.name@-winpkg.log
)
set HADOOP_INSTALL_DIR=%HADOOP_INSTALL_ROOT%\@final.name@
set HADOOP_INSTALL_BIN=%HADOOP_INSTALL_DIR%\bin

echo Uninstalling Apache Hadoop @version@

@rem ensure running as admin.
reg query "HKEY_USERS\S-1-5-19\Environment" /v TEMP 2>&1 | findstr /I /C:"REG_EXPAND_SZ" 2>&1 > NUL
If %ERRORLEVEL% EQU 1 (
  echo FATAL ERROR: uninstall script must be run elevated
  goto :eof
)

@rem
@rem  Stop and delete services 
@rem
for %%i in (namenode datanode secondarynamenode jobtracker tasktracker ) do (
  net stop %%i >> "%WINPKG_LOG%" 
  "%windir%\system32\sc.exe" delete %%i  >> "%WINPKG_LOG%" 
)

@rem
@rem  Delete install dir for @final.name@
@rem
echo deleting %HADOOP_INSTALL_DIR% >> "%WINPKG_LOG%"
rd /s /q "%HADOOP_INSTALL_DIR%" >> "%WINPKG_LOG%"

@rem TODO create event log for services

endlocal