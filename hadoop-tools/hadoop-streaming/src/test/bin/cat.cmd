@for /F "usebackq tokens=* delims=" %%A in (`findstr .`) do @echo %%A
@rem lines have been copied from stdin to stdout
