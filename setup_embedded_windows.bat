@echo off
setlocal
set "FLOWKIT_HOME=%~dp0"
set "FLOWKIT_HOME=%FLOWKIT_HOME:~0,-1%"

if not exist "%FLOWKIT_HOME%\data" mkdir "%FLOWKIT_HOME%\data"
if not exist "%FLOWKIT_HOME%\profiles" mkdir "%FLOWKIT_HOME%\profiles"
if not exist "%FLOWKIT_HOME%\outputs" mkdir "%FLOWKIT_HOME%\outputs"
if not exist "%FLOWKIT_HOME%\logs" mkdir "%FLOWKIT_HOME%\logs"
if not exist "%FLOWKIT_HOME%\runtime" mkdir "%FLOWKIT_HOME%\runtime"

if not exist "%FLOWKIT_HOME%\runtime\embedded_workers.json" (
  > "%FLOWKIT_HOME%\runtime\embedded_workers.json" echo []
)

if not exist "%FLOWKIT_HOME%\.env" (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "(Get-Content -LiteralPath '%FLOWKIT_HOME%\.env.example') -replace '%%FLOWKIT_HOME%%', '%FLOWKIT_HOME:\=\\%' | Set-Content -LiteralPath '%FLOWKIT_HOME%\.env' -Encoding UTF8"
)

if not exist "%FLOWKIT_HOME%\.venv\Scripts\python.exe" (
  py -3 -m venv "%FLOWKIT_HOME%\.venv"
)

"%FLOWKIT_HOME%\.venv\Scripts\python.exe" -m pip install -r "%FLOWKIT_HOME%\requirements.txt"

echo Embedded Flow Gateway setup complete.
echo Project: %FLOWKIT_HOME%
echo Initial worker accounts: 0
endlocal
