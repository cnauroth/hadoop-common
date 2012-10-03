SETLOCAL ENABLEDELAYEDEXPANSION 
SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

REM @@TODO: main query scripts should get called in background then wait-all
REM         batch-script doesn't support this.. will need to rewrite in another scripting language.


set I=1
:loop1
  echo !I!
  CALL "%GRID_MIX_HOME%\webdatascan\webdata_scan.medium.cmd"  2>&1 > webdata_scan.medium.!I!.out
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_MEDIUM_JOBS_PER_CLASS%  goto loop1
    
set I=1
:loop2
  echo !I!
  CALL "%GRID_MIX_HOME%\webdatascan\webdata_scan.small.cmd"  2>&1 > webdata_scan.small.!I!.out
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_SMALL_JOBS_PER_CLASS%  goto loop2
    
set I=1
:loop3
  echo !I!
  CALL "%GRID_MIX_HOME%\webdatascan\webdata_scan.large.cmd"  2>&1 > webdata_scan.large.!I!.out
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_SMALL_JOBS_PER_CLASS%  goto loop3

