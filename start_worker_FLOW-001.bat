@echo off
chcp 65001 >nul
cd /d D:\Codex\projects\flow_gateway_poc\flowkit

set FLOW_ACCOUNT_ID=FLOW-001
set AGENT_API_HOST=127.0.0.1
set AGENT_API_PORT=8100
set EXTENSION_WS_HOST=127.0.0.1
set EXTENSION_WS_PORT=9222
set FLOW_DB_PATH=D:\Codex\projects\flow_gateway_poc\data\FLOW-001.db
set OUTPUT_DIR=D:\Codex\projects\flow_gateway_poc\outputs\FLOW-001

echo Starting FlowKit Worker %FLOW_ACCOUNT_ID%
echo API: http://%AGENT_API_HOST%:%AGENT_API_PORT%
echo WS:  ws://%EXTENSION_WS_HOST%:%EXTENSION_WS_PORT%
D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe -m agent.main
if %errorlevel%==3 exit /b 0
if errorlevel 1 (
  echo Worker failed with exit code %errorlevel%.
  pause
)
