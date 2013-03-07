@for /F "usebackq tokens=* delims=" %%A in (`findstr .`) do @echo %%A

