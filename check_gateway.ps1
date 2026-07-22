$ErrorActionPreference = "Stop"

$base = "http://127.0.0.1:8200"
Write-Host "Gateway health"
Invoke-RestMethod "$base/health" | ConvertTo-Json -Depth 5

Write-Host "Pool status"
Invoke-RestMethod "$base/api/pool/status" | ConvertTo-Json -Depth 5

Write-Host "Accounts"
Invoke-RestMethod "$base/api/pool/accounts" | ConvertTo-Json -Depth 5

Write-Host "Tasks"
Invoke-RestMethod "$base/api/pool/tasks" | ConvertTo-Json -Depth 5
