@echo off
setlocal
cd /d D:\Codex\projects\flow_gateway_poc\flowkit

set PID_FILE=runtime\gateway.pid
set PID=

if exist "%PID_FILE%" (
  set /p PID=<"%PID_FILE%"
)

if "%PID%"=="" (
  for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8200" ^| findstr "LISTENING"') do set PID=%%p
)

if "%PID%"=="" (
  echo Gateway is not listening on 8200.
  exit /b 0
)

echo Stopping Gateway PID %PID% only. Workers and Chrome are not touched.
powershell -NoProfile -ExecutionPolicy Bypass -Command "Stop-Process -Id %PID% -ErrorAction Stop; $deadline=(Get-Date).AddSeconds(10); while((Get-Date) -lt $deadline){ if(-not (Get-Process -Id %PID% -ErrorAction SilentlyContinue)){ exit 0 }; Start-Sleep -Milliseconds 250 }; exit 1"
if errorlevel 1 (
  echo Gateway stop timed out or failed.
  exit /b 1
)

if exist "%PID_FILE%" del "%PID_FILE%" >nul 2>nul
echo Gateway stopped.
exit /b 0
