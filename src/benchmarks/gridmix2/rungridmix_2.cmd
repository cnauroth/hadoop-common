@ECHO OFF

REM Environment configuration
SET GRID_MIX=%~dp0
CD %GRID_MIX%
CALL "%GRID_MIX%gridmix-env-2.cmd"

FOR /F "delims=" %%a in ('%CYGWIN_HOME%\bin\date +%%F-%%H-%%M-%%S-%%N') DO (
 SET DATE=%%a
)

ECHO %Date% > %1_start.out


SET HADOOP_CLASSPATH=%APP_JAR%:%EXAMPLE_JAR%:%STREAMING_JAR%
SET LIBJARS=%APP_JAR%,%EXAMPLE_JAR%,%STREAMING_JAR%

CALL %HADOOP_HOME%\bin\hadoop jar gridmix.jar org.apache.hadoop.mapred.GridMixRunner -libjars %LIBJARS%

FOR /F "delims=" %%a in ('%GRID_MIX%\WinDateTime.exe') DO (
 SET DATE=%%a
)

ECHO %Date% > %1_end.out

	