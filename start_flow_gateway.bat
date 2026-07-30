@echo off
setlocal EnableDelayedExpansion
cd /d D:\Codex\projects\flow_gateway_poc\flowkit

set PYTHON=D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe
set GATEWAY_API_HOST=127.0.0.1
set GATEWAY_API_PORT=8200
set POOL_DRY_RUN=false
set POOL_MAX_CONCURRENCY=10
set FLOWKIT_GATEWAY_WORKER_SOURCE=runtime_registry
set GATEWAY_DB_PATH=D:\Codex\projects\flow_gateway_poc\flowkit\data\gateway.db
set GATEWAY_LEASE_SWEEPER_INTERVAL_SECONDS=30
set PID_FILE=runtime\gateway.pid
set STDOUT_LOG=logs\gateway.log
set STDERR_LOG=logs\gateway-startup-error.log

if not exist logs mkdir logs
if not exist runtime mkdir runtime

if not exist "%PYTHON%" (
  echo Missing Python venv: %PYTHON%
  exit /b 1
)

if not exist gateway\workers.json (
  echo Missing gateway\workers.json
  exit /b 1
)

"%PYTHON%" -c "import socket,sys; s=socket.socket(); s.settimeout(2); rc=s.connect_ex(('127.0.0.1',8200)); s.close(); sys.exit(0 if rc == 0 else 1)"
if %errorlevel%==0 (
  for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8200" ^| findstr "LISTENING"') do set RUNNING_PID=%%p
  if not "!RUNNING_PID!"=="" echo !RUNNING_PID!>"%PID_FILE%"
  echo Gateway already running: http://127.0.0.1:8200/
  if not "!RUNNING_PID!"=="" echo PID: !RUNNING_PID!
  exit /b 0
)

echo Starting Flow Gateway Task Center...
powershell -NoProfile -ExecutionPolicy Bypass -Command "Start-Process -WindowStyle Hidden -FilePath '%PYTHON%' -ArgumentList '-m','gateway.main' -WorkingDirectory '%CD%' -RedirectStandardOutput '%CD%\%STDOUT_LOG%' -RedirectStandardError '%CD%\%STDERR_LOG%'"
if errorlevel 1 (
  echo Failed to launch Gateway. See %STDERR_LOG%
  exit /b 1
)

"%PYTHON%" scripts\wait_gateway_health.py http://127.0.0.1:8200/health 30 >nul 2>nul
if errorlevel 1 (
  echo Gateway did not become healthy within 30 seconds.
  echo stdout: %STDOUT_LOG%
  echo stderr: %STDERR_LOG%
  exit /b 1
)

:ready
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8200" ^| findstr "LISTENING"') do set RUNNING_PID=%%p
if not "!RUNNING_PID!"=="" echo !RUNNING_PID!>"%PID_FILE%"
echo Gateway ready: http://127.0.0.1:8200/
if not "!RUNNING_PID!"=="" echo PID: !RUNNING_PID!
exit /b 0
