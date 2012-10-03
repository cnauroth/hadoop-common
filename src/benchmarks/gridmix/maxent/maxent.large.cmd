SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

set NUM_OF_REDUCERS=100
set INDIR=%FIXCOMPTEXT%

FOR /F "delims=" %%a in ('%GRID_DIR%\..\WinDateTime.exe') DO (
 SET DATE=%%a
)

set OUTDIR=perf-out/maxent-out-dir-large_%Date%
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%

CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -outdir %OUTDIR%.1 -r %NUM_OF_REDUCERS%


CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -indir %OUTDIR%.1 -outdir %OUTDIR%.2 -r %NUM_OF_REDUCERS%
IF ERRORLEVEL 1 exit /b ERRORLEVEL
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.1
CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -indir %OUTDIR%.2 -outdir %OUTDIR%.3 -r %NUM_OF_REDUCERS%
IF ERRORLEVEL 1 exit /b ERRORLEVEL
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.2
CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -indir %OUTDIR%.3 -outdir %OUTDIR%.4 -r %NUM_OF_REDUCERS%
IF ERRORLEVEL 1 exit /b ERRORLEVEL
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.3
CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -indir %OUTDIR%.4 -outdir %OUTDIR%.5 -r %NUM_OF_REDUCERS%
IF ERRORLEVEL 1 exit /b ERRORLEVEL
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.4
CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -indir %OUTDIR%.5 -outdir %OUTDIR%.6 -r %NUM_OF_REDUCERS%
IF ERRORLEVEL 1 exit /b ERRORLEVEL
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.5
CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -indir %OUTDIR%.6 -outdir %OUTDIR%.7 -r %NUM_OF_REDUCERS%
IF ERRORLEVEL 1 exit /b ERRORLEVEL
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.6
CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -indir %OUTDIR%.7 -outdir %OUTDIR%.8 -r %NUM_OF_REDUCERS%
IF ERRORLEVEL 1 exit /b ERRORLEVEL
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.7
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.8
