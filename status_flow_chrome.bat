@echo off
setlocal
cd /d D:\Codex\projects\flow_gateway_poc\flowkit
set PYTHON=D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe
if "%~1"=="" (
  echo Usage: status_flow_chrome.bat FLOW-004
  exit /b 2
)
if not exist "%PYTHON%" (
  echo Missing Python venv: %PYTHON%
  exit /b 1
)
"%PYTHON%" -m runtime.cli status %~1
exit /b %errorlevel%
