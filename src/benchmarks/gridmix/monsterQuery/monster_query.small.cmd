SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

set NUM_OF_REDUCERS=%NUM_OF_REDUCERS_FOR_SMALL_JOB%
set INDIR="%FIXCOMPSEQ%/{part-00000,part-00001,part-00002}"

FOR /F "delims=" %%a in ('%CYGWIN_HOME%\bin\date +%%F-%%H-%%M-%%S-%%N') DO (
 SET DATE=%%a
)


set OUTDIR=perf-out/mq-out-dir-small_%DATE%.1
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%

CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 10 -keepred 40 -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text -indir %INDIR% -outdir %OUTDIR% -r %NUM_OF_REDUCERS%

set INDIR=%OUTDIR%
set OUTDIR=perf-out/mq-out-dir-small_%DATE%.2
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%

CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 100 -keepred 77 -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text -indir %INDIR% -outdir %OUTDIR% -r %NUM_OF_REDUCERS%

set INDIR=%OUTDIR%
set OUTDIR=perf-out/mq-out-dir-small_%DATE%.3
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%

CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 116 -keepred 91 -inFormat org.apache.hadoop.mapred.SequenceFileInputFormat -outFormat org.apache.hadoop.mapred.SequenceFileOutputFormat -outKey org.apache.hadoop.io.Text -outValue org.apache.hadoop.io.Text -indir %INDIR% -outdir %OUTDIR% -r %NUM_OF_REDUCERS%

