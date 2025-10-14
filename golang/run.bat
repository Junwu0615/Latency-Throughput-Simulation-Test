@echo off

cd %~dp0
echo Current directory: %cd%

call ..\.venv\Scripts\activate
echo Change venv: .\.venv\Scripts\activate

start cmd /k "python ..\python\script\producer.py"


REM GO: 無數據庫
echo 正在編譯 consumer.go ...
go build consumer.go

REM 檢查編譯是否成功
if errorlevel 1 (
    echo Go 編譯失敗！請檢查 consumer.go 檔案中的錯誤 ...
    pause
    exit /b 1
)

echo 編譯成功... 新視窗啟動 consumer.exe ...
start cmd /k "consumer.exe"


echo All Services [2] Are Running...

pause