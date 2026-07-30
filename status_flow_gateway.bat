@echo off
setlocal
cd /d D:\Codex\projects\flow_gateway_poc\flowkit
D:\Codex\projects\flow_gateway_poc\.venv\Scripts\python.exe -c "import json,urllib.request; urls=['http://127.0.0.1:8200/health','http://127.0.0.1:8200/api/v1/system/status','http://127.0.0.1:8200/api/v1/accounts']; [print(u, urllib.request.urlopen(u,timeout=5).read().decode('utf-8')) for u in urls]"
exit /b %errorlevel%
