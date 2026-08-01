@echo off
setlocal
set "FLOWKIT_HOME=%~dp0"
set "FLOWKIT_HOME=%FLOWKIT_HOME:~0,-1%"
if exist "%FLOWKIT_HOME%\.env" (
  for /f "usebackq tokens=1,* delims==" %%A in ("%FLOWKIT_HOME%\.env") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
  )
)
if "%GATEWAY_DB_PATH%"=="" set "GATEWAY_DB_PATH=%FLOWKIT_HOME%\data\embedded_gateway.db"
if "%GATEWAY_WORKERS_PATH%"=="" set "GATEWAY_WORKERS_PATH=%FLOWKIT_HOME%\runtime\embedded_workers.json"
if "%FLOW_GATEWAY_OUTPUT_DIR%"=="" set "FLOW_GATEWAY_OUTPUT_DIR=%FLOWKIT_HOME%\outputs"
if "%FLOWKIT_GATEWAY_WORKER_SOURCE%"=="" set "FLOWKIT_GATEWAY_WORKER_SOURCE=static_json"
if not exist "%FLOWKIT_HOME%\runtime\embedded_workers.json" echo [] > "%FLOWKIT_HOME%\runtime\embedded_workers.json"
set "GATEWAY_LAUNCHER_PID=%PROCESSID%"
start "Flow Gateway Embedded" /min cmd /c ""%FLOWKIT_HOME%\.venv\Scripts\python.exe" -m gateway.main > "%FLOWKIT_HOME%\logs\embedded_gateway.log" 2>&1"
echo Flow Gateway Embedded starting on http://%GATEWAY_API_HOST%:%GATEWAY_API_PORT%
endlocal
