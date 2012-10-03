@echo off
SETLOCAL ENABLEDELAYEDEXPANSION 

:loop

Set /a JavaProcCount=0
For /F "skip=1" %%A in ('tasklist /nh /fi "IMAGENAME eq java.exe"') Do Set /a JavaProcCount=!JavaProcCount!+1
echo JavaProcCount=%JavaProcCount%

IF /I "!JavaProcCount!" GTR "70" (
 sleep 1 
 GOTO loop
)
