@echo off
setlocal
set "FLOWKIT_HOME=%~dp0"
set "FLOWKIT_HOME=%FLOWKIT_HOME:~0,-1%"
for /f "tokens=2 delims=," %%P in ('wmic process where "CommandLine like '%%gateway.main%%' and CommandLine like '%%%FLOWKIT_HOME:\=\\%%%'" get ProcessId /format:csv ^| findstr /r "[0-9]"') do (
  taskkill /PID %%P /F >nul 2>nul
)
echo Flow Gateway Embedded stop requested.
endlocal
