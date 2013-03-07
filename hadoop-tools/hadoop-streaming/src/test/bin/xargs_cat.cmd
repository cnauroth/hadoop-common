@for /F "usebackq tokens=* delims=" %%A in (`findstr .`) do @type %%A
@rem files named on stdin have been copied to stdout
