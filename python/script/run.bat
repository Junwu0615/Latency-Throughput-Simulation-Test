@echo off

cd %~dp0
cd ..\..
echo Current directory: %cd%

call .\.venv\Scripts\activate
echo Change venv: .\.venv\Scripts\activate

start cmd /k "python script\producer.py"
start cmd /k "python script\consumer.py"
echo All Services [2] Are Running...

pause