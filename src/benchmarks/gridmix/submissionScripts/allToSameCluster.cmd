
SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

REM The original submits all the scripts concurrently then waits for them all
REM This windows-version currently submits stage-by-stage.  
REM This is due to WaitAllGridmix.cmd not being parameterized.
REM It can be remedied by using different window-titles per script and parameterizing the wait 

CALL %GRID_MIX_HOME%\submissionScripts\maxentToSameCluster 2>&1 > maxentToSameCluster.out 
CALL %GRID_MIX_HOME%\submissionScripts\textSortToSameCluster 2>&1 > textSortToSameCluster.out
CALL %GRID_MIX_HOME%\submissionScripts\monsterQueriesToSameCluster 2>&1 > monsterQueriesToSameCluster.out
CALL %GRID_MIX_HOME%\submissionScripts\webdataScanToSameCluster 2>&1 > webdataScanToSameCluster.out
CALL %GRID_MIX_HOME%\submissionScripts\webdataSortToSameCluster  2>&1 > webdataSortToSameCluster.out

CALL "%GRID_MIX_HOME%\submissionScripts\WaitAllGridmix.cmd"
