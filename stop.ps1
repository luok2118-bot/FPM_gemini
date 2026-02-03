# stop.ps1 - stop background uvicorn started by start.ps1
# It will:
# 1) stop process from pidfile if exists
# 2) fallback: stop process listening on the configured port

[CmdletBinding()]
param(
    [int]$AppPort = 8000
)

$ErrorActionPreference = "Stop"
$ProjectRootPath = $PSScriptRoot
$PidFilePath = Join-Path $ProjectRootPath "run\uvicorn.pid"

function Get-ListeningProcessIdByPort([int]$Port) {
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($conn) { return $conn.OwningProcess }
    return $null
}

function Stop-ProcessTreeById([int]$ProcessId) {
    # Try to stop children first (best-effort), then parent
    try {
        $childList = Get-CimInstance Win32_Process -Filter "ParentProcessId=$ProcessId" -ErrorAction SilentlyContinue
        foreach ($c in $childList) {
            try {
                Stop-Process -Id $c.ProcessId -Force -ErrorAction SilentlyContinue
                Write-Host "Stopped child PID=$($c.ProcessId) (PPID=$ProcessId)" -ForegroundColor DarkGreen
            } catch {}
        }
    } catch {}

    try {
        Stop-Process -Id $ProcessId -Force -ErrorAction Stop
        Write-Host "Stopped PID=$ProcessId" -ForegroundColor Green
        return $true
    } catch {
        Write-Host "PID=$ProcessId not running (or no permission)." -ForegroundColor Yellow
        return $false
    }
}

# ---- 1) stop by pidfile ----
$stoppedSomething = $false

if (Test-Path $PidFilePath) {
    $pidTextValue = (Get-Content $PidFilePath -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($pidTextValue -match '^\d+$') {
        $processIdFromFile = [int]$pidTextValue
        $ok = Stop-ProcessTreeById $processIdFromFile
        if ($ok) { $stoppedSomething = $true }
    } else {
        Write-Host "Invalid pid file content: $PidFilePath" -ForegroundColor Yellow
    }

    Remove-Item $PidFilePath -Force -ErrorAction SilentlyContinue
}

# ---- 2) fallback: stop by port ----
$portOwnerProcessId = Get-ListeningProcessIdByPort $AppPort
if ($portOwnerProcessId) {
    Write-Host "Port $AppPort is still in use by PID=$portOwnerProcessId. Stopping it..." -ForegroundColor Yellow
    $ok2 = Stop-ProcessTreeById $portOwnerProcessId
    if ($ok2) { $stoppedSomething = $true }
} else {
    if (-not $stoppedSomething) {
        Write-Host "Nothing to stop: no pidfile and port $AppPort not listening." -ForegroundColor Yellow
    } else {
        Write-Host "Port $AppPort is free now." -ForegroundColor Green
    }
}
