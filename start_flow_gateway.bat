@echo off
setlocal
cd /d D:\Codex\projects\flow_gateway_poc\flowkit
set PYTHON=D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe
set GATEWAY_API_HOST=127.0.0.1
set GATEWAY_API_PORT=8200
set POOL_DRY_RUN=false
set POOL_MAX_CONCURRENCY=2
set FLOWKIT_GATEWAY_WORKER_SOURCE=static_json
set GATEWAY_DB_PATH=D:\Codex\projects\flow_gateway_poc\flowkit\data\gateway.db
set GATEWAY_LEASE_SWEEPER_INTERVAL_SECONDS=30
if not exist logs mkdir logs
%PYTHON% -c "import socket,sys; s=socket.socket(); rc=s.connect_ex(('127.0.0.1',8200)); s.close(); sys.exit(0 if rc == 0 else 1)"
if %errorlevel%==0 (
  echo Gateway already running: http://127.0.0.1:8200/
  exit /b 0
)
if not exist gateway\workers.json (
  echo Missing gateway\workers.json
  exit /b 1
)
echo Starting Flow Gateway Task Center...
echo UI: http://127.0.0.1:8200/
start "Flow Gateway" /min cmd /c ""%PYTHON%" -m gateway.main > logs\gateway.log 2> logs\gateway-startup-error.log"
exit /b 0
