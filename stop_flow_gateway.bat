@echo off
setlocal
for /f "tokens=5" %%p in ('netstat -ano ^| findstr ":8200" ^| findstr "LISTENING"') do set PID=%%p
if "%PID%"=="" (
  echo Gateway is not listening on 8200.
  exit /b 0
)
echo Stopping Gateway PID %PID% only. Workers and Chrome are not touched.
powershell -NoProfile -Command "Stop-Process -Id %PID% -ErrorAction Stop"
exit /b %errorlevel%
