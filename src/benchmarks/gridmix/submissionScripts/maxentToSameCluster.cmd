SETLOCAL ENABLEDELAYEDEXPANSION 
SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

set I=1
:loop
  echo !I!
  START "__GRIDMIX_CMD" cmd /c "%GRID_MIX_HOME%\maxent\maxent.large.cmd"  ^> maxent.large.!I!.out 2^>^&1
  CALL %GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd
  set /a I=!I!+1
if !I! LEQ %NUM_OF_LARGE_JOBS_FOR_ENTROPY_CLASS%  goto loop

CALL "%GRID_MIX_HOME%\submissionScripts\WaitAllGridmix.cmd"
