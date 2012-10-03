SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

set NUM_OF_REDUCERS=%NUM_OF_REDUCERS_FOR_MEDIUM_JOB%
set INDIR="%VARINFLTEXT%/{part-000*0,part-000*1,part-000*2}"
FOR /F "delims=" %%a in ('%GRID_DIR%\..\WinDateTime.exe') DO (
 SET DATE=%%a
)


set OUTDIR=perf-out/stream-out-dir-medium_%Date%
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%


CALL %HADOOP_HOME%\bin\hadoop jar %STREAM_JAR% -input %INDIR% -output %OUTDIR% -mapper cat -reducer cat -numReduceTasks %NUM_OF_REDUCERS%

