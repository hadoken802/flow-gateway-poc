@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"

set "PYTHON=D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe"
set "PROJECT_DIR=%~dp0"
set "LOG_DIR=%PROJECT_DIR%logs"
set "ERROR_LOG=%LOG_DIR%\storyboard-gui-startup-error.log"

if not exist "%LOG_DIR%" (
    mkdir "%LOG_DIR%"
)

if not exist "%PYTHON%" (
    echo.
    echo [启动失败] 找不到 Python：
    echo %PYTHON%
    echo.
    echo 请确认虚拟环境是否存在。
    >>"%ERROR_LOG%" echo [%date% %time%] Python 不存在：%PYTHON%
    pause
    exit /b 1
)

set "PYTHONPATH=%PROJECT_DIR%"

echo 正在启动 Flow 多账号分镜制作工具...
echo.

"%PYTHON%" -m gateway.storyboard_gui 2>>"%ERROR_LOG%"

if errorlevel 1 (
    echo.
    echo [启动失败] GUI 进程异常退出。
    echo 请查看：
    echo %ERROR_LOG%
    echo.
    >>"%ERROR_LOG%" echo [%date% %time%] GUI 启动失败，命令："%PYTHON%" -m gateway.storyboard_gui
    pause
    exit /b 1
)

endlocal
