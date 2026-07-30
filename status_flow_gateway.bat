@echo off
setlocal EnableDelayedExpansion
cd /d D:\Codex\projects\flow_gateway_poc\flowkit

set PYTHON=D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe
set PID_FILE=runtime\gateway.pid

echo Flow Gateway status
if exist "%PID_FILE%" (
  set /p PID=<"%PID_FILE%"
) else (
  echo pid_file: missing
)
if not "!PID!"=="" echo pid_file: !PID!

for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8200" ^| findstr "LISTENING"') do set PORT_PID=%%p
if "%PORT_PID%"=="" (
  echo port_8200: not listening
  exit /b 1
)
echo port_8200: listening pid %PORT_PID%

"%PYTHON%" -c "import json,urllib.request; urls=['http://127.0.0.1:8200/health','http://127.0.0.1:8200/api/v1/system/status','http://127.0.0.1:8200/api/v1/accounts']; [print(u, urllib.request.urlopen(u,timeout=5).read().decode('utf-8')[:2000]) for u in urls]"
exit /b %errorlevel%
