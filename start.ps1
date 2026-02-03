# start.ps1 - Activate conda env and start FastAPI (uvicorn)
# Usage:
#   .\start.ps1                # default: background
#   .\start.ps1 -Foreground    # run in current window
#   .\start.ps1 -Port 8001
# If script cannot run:
#   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

[CmdletBinding()]
param(
    [int]$AppPort = 8000,
    [string]$CondaEnvName = "env-py312",
    [string]$AsgiApp = "main:app",
    [string]$BindAddress = "0.0.0.0",   # avoid $Host
    [int]$WorkerCount = 1,
    [switch]$Foreground
)

$ErrorActionPreference = "Stop"

# Root paths
$ProjectRootPath = $PSScriptRoot

# Output paths
$LogDirPath = Join-Path $ProjectRootPath "logs"
$RunDirPath = Join-Path $ProjectRootPath "run"
$StdoutLogPath = Join-Path $LogDirPath "uvicorn.out.log"
$StderrLogPath = Join-Path $LogDirPath "uvicorn.err.log"
$PidFilePath = Join-Path $RunDirPath "uvicorn.pid"

New-Item -ItemType Directory -Force -Path $LogDirPath | Out-Null
New-Item -ItemType Directory -Force -Path $RunDirPath | Out-Null

function Get-CondaExePath {
    # prefer CONDA_EXE if exists
    if ($env:CONDA_EXE -and (Test-Path $env:CONDA_EXE)) { return $env:CONDA_EXE }

    $condaCmd = Get-Command conda -ErrorAction SilentlyContinue
    if ($condaCmd -and $condaCmd.Source) { return $condaCmd.Source }

    $commonPaths = @(
        "$env:USERPROFILE\miniconda3\Scripts\conda.exe",
        "$env:USERPROFILE\anaconda3\Scripts\conda.exe",
        "$env:ProgramData\miniconda3\Scripts\conda.exe",
        "$env:ProgramData\anaconda3\Scripts\conda.exe"
    )
    foreach ($p in $commonPaths) {
        if (Test-Path $p) { return $p }
    }
    return $null
}

function Get-ListeningProcessIdByPort([int]$Port) {
    $conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($conn) { return $conn.OwningProcess }
    return $null
}

function Test-ProcessAlive([int]$ProcessId) {
    try { Get-Process -Id $ProcessId -ErrorAction Stop | Out-Null; return $true } catch { return $false }
}

# ---- Prevent double start if pidfile points to a live process ----
if (Test-Path $PidFilePath) {
    $oldPidText = (Get-Content $PidFilePath -ErrorAction SilentlyContinue | Select-Object -First 1)
    if ($oldPidText -match '^\d+$') {
        $oldProcessId = [int]$oldPidText
        if (Test-ProcessAlive $oldProcessId) {
            Write-Host "Already running (PID=$oldProcessId). If you want to restart, run .\stop.ps1 first." -ForegroundColor Yellow
            exit 0
        } else {
            Remove-Item $PidFilePath -Force -ErrorAction SilentlyContinue
        }
    } else {
        Remove-Item $PidFilePath -Force -ErrorAction SilentlyContinue
    }
}

# ---- Port check (avoid $PID / $pid naming) ----
$portOwnerProcessId = Get-ListeningProcessIdByPort $AppPort
if ($portOwnerProcessId) {
    Write-Host "Port $AppPort is in use by PID=$portOwnerProcessId. Run .\stop.ps1 or stop that process." -ForegroundColor Yellow
    exit 1
}

$condaExePath = Get-CondaExePath
if (-not $condaExePath) {
    Write-Host "Conda not found. Install Miniconda/Anaconda or set CONDA_EXE." -ForegroundColor Red
    exit 1
}

Set-Location $ProjectRootPath

# Compose uvicorn args (run via python -m uvicorn is more robust than relying on uvicorn.exe)
$uvicornArgList = @(
    "-m", "uvicorn", $AsgiApp,
    "--host", $BindAddress,
    "--port", "$AppPort",
    "--workers", "$WorkerCount"
)

# ---- Foreground mode ----
if ($Foreground) {
    Write-Host "Working dir: $ProjectRootPath" -ForegroundColor Cyan
    Write-Host "Starting uvicorn in FOREGROUND (port $AppPort). Ctrl+C to stop." -ForegroundColor Cyan

    & $condaExePath shell.powershell hook | Out-String | Invoke-Expression
    conda activate $CondaEnvName

    python @uvicornArgList
    exit 0
}

# ---- Background mode: Start a new PowerShell process ----
# NOTE:
# - Child process must run conda hook + activate again (env var not inherited as 'activated')
# - Use single-quoted values inside child script to avoid most escaping issues
$childCommand = @"
`$ErrorActionPreference = 'Stop'
Set-Location '$ProjectRootPath'

& '$condaExePath' shell.powershell hook | Out-String | Invoke-Expression
conda activate '$CondaEnvName'

python $($uvicornArgList -join ' ')
"@

# Truncate logs on each start (comment out if you prefer append)
"" | Set-Content -Path $StdoutLogPath -Encoding UTF8
"" | Set-Content -Path $StderrLogPath -Encoding UTF8

# Use powershell.exe (Windows PowerShell) for maximum compatibility
$bgProcess = Start-Process -FilePath "powershell.exe" `
    -ArgumentList @("-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", $childCommand) `
    -WorkingDirectory $ProjectRootPath `
    -WindowStyle Hidden `
    -RedirectStandardOutput $StdoutLogPath `
    -RedirectStandardError $StderrLogPath `
    -PassThru

# Save PID
$bgProcess.Id | Set-Content -Path $PidFilePath -Encoding ASCII

Write-Host "Started uvicorn in BACKGROUND." -ForegroundColor Green
Write-Host "PID: $($bgProcess.Id)" -ForegroundColor Green
Write-Host "Out log: $StdoutLogPath" -ForegroundColor Cyan
Write-Host "Err log: $StderrLogPath" -ForegroundColor Cyan
Write-Host "Stop with: .\stop.ps1" -ForegroundColor Cyan
