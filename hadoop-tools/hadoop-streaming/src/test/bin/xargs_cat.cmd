@for /F "usebackq tokens=* delims=" %%A in (`findstr .`) do @type %%A

