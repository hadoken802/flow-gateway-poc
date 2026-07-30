@echo off
setlocal
cd /d D:\Codex\projects\flow_gateway_poc\flowkit
set PYTHON=D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe
if not exist "%PYTHON%" (
  echo Missing Python venv: %PYTHON%
  exit /b 1
)
"%PYTHON%" -m runtime.cli start-all-workers
exit /b %errorlevel%
