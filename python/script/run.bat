@echo off

cd %~dp0
cd ..\..
echo Current directory: %cd%

call .\.venv\Scripts\activate
echo Change venv: .\.venv\Scripts\activate

start cmd /k "python python\script\producer.py"
@REM start cmd /k "python python\script\consumer_v1.py"
start cmd /k "python python\script\consumer_v2.py"
@REM start cmd /k "python python\script\consumer_v3.py"
@REM start cmd /k "python python\script\consumer_v4.py"
echo All Services [2] Are Running...

pause