@echo off

cd %~dp0
echo Current directory: %cd%

call ..\.venv\Scripts\activate
echo Change venv: .\.venv\Scripts\activate

start cmd /k "python ..\python\script\producer.py"


REM GO: 無數據庫
echo 正在編譯 Go 語言 Consumer 程式 (consumer.go)...
go build consumer.go

REM 檢查編譯是否成功
if errorlevel 1 (
    echo 🔴 Go 編譯失敗！請檢查 consumer.go 檔案中的錯誤。
    pause
    exit /b 1
)

echo 編譯成功！正在新視窗啟動 consumer.exe ...

REM ==============================================================
REM 2. 使用 start cmd /k 在新視窗中執行已編譯的程式
REM    - /k: 執行完 consumer.exe 後保持視窗開啟，以便查看結果。
REM ==============================================================
start cmd /k "consumer.exe"

echo All Services [2] Are Running...

pause