SETLOCAL ENABLEDELAYEDEXPANSION 
SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

set I=1
:loop1
  echo Iteration: !I!
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\webdatascan\webdata_scan.medium.cmd"  ^> webdata_scan.medium.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_MEDIUM_JOBS_PER_CLASS%  goto loop1
    
set I=1
:loop2
  echo Iteration: !I!
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\webdatascan\webdata_scan.small.cmd"  ^> webdata_scan.small.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_SMALL_JOBS_PER_CLASS%  goto loop2
    
set I=1
:loop3
  echo Iteration: !I!
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\webdatascan\webdata_scan.large.cmd"  ^> webdata_scan.large.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_SMALL_JOBS_PER_CLASS%  goto loop3

CALL "%GRID_MIX_HOME%\submissionScripts\WaitAllGridmix.cmd"