@echo OFF
tasklist /NH /V | %CYGWIN_HOME%\bin\grep.exe "__GRIDMIX_CMD"