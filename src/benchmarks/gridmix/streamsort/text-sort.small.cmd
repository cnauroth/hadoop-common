SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd


set NUM_OF_REDUCERS=%NUM_OF_REDUCERS_FOR_SMALL_JOB%
set INDIR="%VARINFLTEXT%/{part-00000,part-00001,part-00002}"
FOR /F "delims=" %%a in ('%CYGWIN_HOME%\bin\date +%%F-%%H-%%M-%%S-%%N') DO (
 SET DATE=%%a
)

set OUTDIR=perf-out/stream-out-dir-small_%Date%
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%


CALL %HADOOP_HOME%\bin\hadoop jar %STREAM_JAR% -input %INDIR% -output %OUTDIR% -mapper cat -reducer cat -numReduceTasks %NUM_OF_REDUCERS%

