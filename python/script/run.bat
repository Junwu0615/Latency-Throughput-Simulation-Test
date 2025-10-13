@echo off

cd %~dp0
cd ..\..
echo Current directory: %cd%

call .\.venv\Scripts\activate
echo Change venv: .\.venv\Scripts\activate

start cmd /k "python python\script\producer.py"

REM 吞吐量 v1: 用 batch 方式塞資料 但還是需要依序等待 I/O
@REM start cmd /k "python python\script\consumer_v1.py"

REM 吞吐量 v2: 導入 ThreadPoolExecutor # 多執行緒
start cmd /k "python python\script\consumer_v2.py"

REM 吞吐量 v3: 異步 I/O (Asyncio)
@REM start cmd /k "python python\script\consumer_v3.py"

REM 吞吐量 v4: 水平擴展 [2]
@REM start cmd /k "python python\script\consumer_v4.py"
@REM start cmd /k "python python\script\consumer_v4.py"

REM 吞吐量 v4: 水平擴展 [4]
@REM start cmd /k "python python\script\consumer_v4.py"
@REM start cmd /k "python python\script\consumer_v4.py"
@REM start cmd /k "python python\script\consumer_v4.py"
@REM start cmd /k "python python\script\consumer_v4.py"


echo All Services [2] Are Running...

pause