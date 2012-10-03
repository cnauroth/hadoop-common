SETLOCAL ENABLEDELAYEDEXPANSION 
SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

REM @@TODO: main query scripts should get called in background then wait-all
REM         batch-script doesn't support this.. will need to rewrite in another scripting language.



set I=1
:loop1
  echo !I!
  CALL "%GRID_MIX_HOME%\webdatasort\webdata_sort.large.cmd"  2>&1 > webdata_sort.large.!I!.out
  CALL "%GRID_MIX_HOME%\submissionScripts\sleep_if_too_busy.cmd"
  set /a I=!I!+1
if !I! LEQ %NUM_OF_LARGE_JOBS_PER_CLASS%  goto loop1
