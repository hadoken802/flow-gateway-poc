@echo off
setlocal
cd /d D:\Codex\projects\flow_gateway_poc\flowkit

set GATEWAY_API_HOST=127.0.0.1
set GATEWAY_API_PORT=8200
set POOL_MAX_CONCURRENCY=1
set POOL_DRY_RUN=false
set CANARY_ONLY=true
set CANARY_LIMIT=1
set OMNI_10S_CREDIT_COST=15
set REAL_SUBMIT_MAX_ATTEMPTS=1
set GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS=300
set FLOWKIT_GATEWAY_WORKER_SOURCE=runtime_registry

for /f "tokens=1-3 delims=/ " %%a in ("%date%") do set _date=%%c%%a%%b
for /f "tokens=1-3 delims=:." %%a in ("%time%") do set _time=%%a%%b%%c
set _time=%_time: =0%
set CANARY_RUN_DIR=D:\FlowKitRuntimeDiag\flow024-real-canary-%_date%-%_time%
set GATEWAY_DB_PATH=%CANARY_RUN_DIR%\gateway.db

if not exist "%CANARY_RUN_DIR%" mkdir "%CANARY_RUN_DIR%"

echo Starting Flow Gateway real canary
echo API: %GATEWAY_API_HOST%:%GATEWAY_API_PORT%
echo CANARY_ONLY=%CANARY_ONLY% CANARY_LIMIT=%CANARY_LIMIT%
echo POOL_MAX_CONCURRENCY=%POOL_MAX_CONCURRENCY%
echo REAL_SUBMIT_MAX_ATTEMPTS=%REAL_SUBMIT_MAX_ATTEMPTS%
echo GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS=%GATEWAY_WORKER_SUBMIT_TIMEOUT_SECONDS%
echo FLOWKIT_GATEWAY_WORKER_SOURCE=%FLOWKIT_GATEWAY_WORKER_SOURCE%
echo CANARY_RUN_DIR=%CANARY_RUN_DIR%
echo GATEWAY_DB_PATH=%GATEWAY_DB_PATH%

D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe -m gateway.main
if errorlevel 1 (
  echo Gateway failed to start.
  pause
)
