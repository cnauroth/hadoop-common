SETLOCAL ENABLEDELAYEDEXPANSION 
SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

REM NOTE: pipe-sort does not run on windows.
REM @@TODO: main query scripts should get called in background then wait-all
REM         batch-script doesn't support this.. will need to rewrite in another scripting language.

set I=1
:loop1
  echo !I!
  REM no pipe-sort.
  CALL "%GRID_MIX_HOME%\streamsort\text-sort.small.cmd"  2>&1 > streamsort.small.!I!.out
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  CALL "%GRID_MIX_HOME%\streamsort\text-sort.small.cmd"  2>&1 > javasort.small.!I!.out & 
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_SMALL_JOBS_PER_CLASS%  goto loop1


set I=1
:loop2
  echo !I!
  REM no pipe-sort.
  CALL "%GRID_MIX_HOME%\streamsort\text-sort.medium.cmd"  2>&1 > streamsort.medium.!I!.out
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  CALL "%GRID_MIX_HOME%\streamsort\text-sort.medium.cmd"  2>&1 > javasort.medium.!I!.out & 
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_MEDIUM_JOBS_PER_CLASS%  goto loop2

set I=1
:loop3
  echo !I!
  REM no pipe-sort.
  CALL "%GRID_MIX_HOME%\streamsort\text-sort.large.cmd"  2>&1 > streamsort.large.!I!.out
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  CALL "%GRID_MIX_HOME%\streamsort\text-sort.large.cmd"  2>&1 > javasort.large.!I!.out & 
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_LARGE_JOBS_PER_CLASS%  goto loop3
    
