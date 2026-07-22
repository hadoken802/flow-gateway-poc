@echo off
chcp 65001 >nul
cd /d D:\Codex\projects\flow_gateway_poc\flowkit

set GATEWAY_API_HOST=127.0.0.1
set GATEWAY_API_PORT=8200
set POOL_MAX_CONCURRENCY=2
set POOL_DRY_RUN=true
set OMNI_10S_CREDIT_COST=15
set GATEWAY_DB_PATH=D:\Codex\projects\flow_gateway_poc\data\gateway.db

echo Starting Flow Gateway Dry Run
echo API: http://%GATEWAY_API_HOST%:%GATEWAY_API_PORT%
D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe -m gateway.main
if errorlevel 1 (
  echo Gateway failed with exit code %errorlevel%.
  pause
)
