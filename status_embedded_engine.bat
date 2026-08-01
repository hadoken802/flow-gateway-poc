@echo off
setlocal
set "FLOWKIT_HOME=%~dp0"
set "FLOWKIT_HOME=%FLOWKIT_HOME:~0,-1%"
if exist "%FLOWKIT_HOME%\.env" (
  for /f "usebackq tokens=1,* delims==" %%A in ("%FLOWKIT_HOME%\.env") do (
    if not "%%A"=="" if not "%%A:~0,1%"=="#" set "%%A=%%B"
  )
)
if "%GATEWAY_API_HOST%"=="" set "GATEWAY_API_HOST=127.0.0.1"
if "%GATEWAY_API_PORT%"=="" set "GATEWAY_API_PORT=8200"
powershell -NoProfile -ExecutionPolicy Bypass -Command "$h=@{}; if ('%FLOW_GATEWAY_CLIENT_API_KEY%') { $h['X-API-Key']='%FLOW_GATEWAY_CLIENT_API_KEY%' }; try { Invoke-WebRequest -UseBasicParsing -TimeoutSec 5 -Headers $h -Uri 'http://%GATEWAY_API_HOST%:%GATEWAY_API_PORT%/api/v1/client/system/ready' | Select-Object StatusCode,Content } catch { Write-Output $_.Exception.Message; exit 1 }"
endlocal
