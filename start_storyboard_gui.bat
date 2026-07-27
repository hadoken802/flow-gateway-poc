@echo off
setlocal
cd /d "%~dp0"

set "PYTHON=%~dp0..\.venv\Scripts\python.exe"
set "PYTHONPATH=%~dp0"
set "LOG_DIR=%~dp0logs"
set "LOG_FILE=%LOG_DIR%\storyboard-gui-startup-error.log"

if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

if not exist "%PYTHON%" (
    echo Python not found:
    echo %PYTHON%
    echo [%date% %time%] Python not found: %PYTHON%>>"%LOG_FILE%"
    pause
    exit /b 1
)

echo Starting Flow Storyboard Video Maker...
"%PYTHON%" -m gateway.storyboard_gui

set "EXIT_CODE=%ERRORLEVEL%"
if not "%EXIT_CODE%"=="0" (
    echo.
    echo GUI failed. Exit code: %EXIT_CODE%
    echo [%date% %time%] GUI failed. Exit code: %EXIT_CODE%>>"%LOG_FILE%"
    pause
)

exit /b %EXIT_CODE%
