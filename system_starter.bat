@echo off

:init
@echo Started: %date% %time%
echo init starts

cd C:\pythonproject\lkwstock_kiwoom

if exist call activate py37_32 (
call activate py37_32
) else (
call activate st39_32
)

GOTO starter

:loop
timeout 1 > NUL
set /a loopa+=1
tasklist | find "python.exe" > NUL
if Not ERRORLEVEL 1 (
echo %loopa%
goto loop
) else (
echo restart!!
goto init
)

:starter
echo first_starter!!
start python main.py
timeout 30 > NUL
set loopa = 0
goto loop
