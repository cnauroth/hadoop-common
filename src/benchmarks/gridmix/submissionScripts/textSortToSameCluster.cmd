SETLOCAL ENABLEDELAYEDEXPANSION 
SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

REM NOTE: pipe-sort does not run on windows.

set I=1
:loop1
  echo Iteration: !I!
  REM no pipe-sort.
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\streamsort\text-sort.small.cmd"  ^> streamsort.small.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\streamsort\text-sort.small.cmd"  ^> javasort.small.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_SMALL_JOBS_PER_CLASS%  goto loop1


set I=1
:loop2
  echo Iteration: !I!
  REM no pipe-sort.
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\streamsort\text-sort.medium.cmd"  ^> streamsort.medium.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\streamsort\text-sort.medium.cmd"  ^> javasort.medium.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_MEDIUM_JOBS_PER_CLASS%  goto loop2

set I=1
:loop3
  echo Iteration: !I!
  REM no pipe-sort.
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\streamsort\text-sort.large.cmd"  ^> streamsort.large.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\streamsort\text-sort.large.cmd"  ^> javasort.large.!I!.out 2^>^&1
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_LARGE_JOBS_PER_CLASS%  goto loop3

    
CALL "%GRID_MIX_HOME%\submissionScripts\WaitAllGridmix.cmd"