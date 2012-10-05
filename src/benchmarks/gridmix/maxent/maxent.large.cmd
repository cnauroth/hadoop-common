@echo off
SETLOCAL ENABLEDELAYEDEXPANSION 

SET GRID_DIR=%~dp0
CD %GRID_DIR%
CALL %GRID_DIR%\..\gridmix-env.cmd

set NUM_OF_REDUCERS=2
set INDIR=%FIXCOMPTEXT%

FOR /F "delims=" %%a in ('%CYGWIN_HOME%\bin\date +%%F-%%H-%%M-%%S-%%N') DO (
 SET DATE=%%a
)

set OUTDIR=perf-out/maxent-out-dir-large_%Date%
CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%

CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -outdir %OUTDIR%.1 -r %NUM_OF_REDUCERS%

FOR /L %%C in (1,1,7) DO (
  SET /A A=%%C
  SET /A B=%%C+1
  CALL %HADOOP_HOME%\bin\hadoop jar %APP_JAR% loadgen -keepmap 50 -keepred 100 -inFormatIndirect org.apache.hadoop.mapred.TextInputFormat -outFormat org.apache.hadoop.mapred.TextOutputFormat -outKey org.apache.hadoop.io.LongWritable -outValue org.apache.hadoop.io.Text -indir %INDIR% -indir %OUTDIR%.!A! -outdir %OUTDIR%.!B! -r %NUM_OF_REDUCERS%
  IF ERRORLEVEL 1 exit /b ERRORLEVEL
  CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.1
) 

CALL %HADOOP_HOME%\bin\hadoop dfs -rmr %OUTDIR%.8
