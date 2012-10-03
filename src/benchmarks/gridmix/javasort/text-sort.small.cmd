SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

set INDIR="%VARINFLTEXT%/{part-00000,part-00001,part-00002}"

FOR /F "delims=" %%a in ('%GRID_DIR%\..\WinDateTime.exe') DO (
 SET DATE=%%a
)

set OUTDIR=perf-out/sort-out-dir-small_%Date%
call %HADOOP_HOME%/bin/hadoop dfs -rmr %OUTDIR%
%HADOOP_HOME%\bin\hadoop jar %EXAMPLE_JAR% sort -m 1 -r %NUM_OF_REDUCERS_FOR_SMALL_JOB% -inFormat org.apache.hadoop.mapred.KeyValueTextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text %INDIR% %OUTDIR%

