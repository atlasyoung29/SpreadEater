Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$script:MonitorUrl = "http://127.0.0.1:8080"
$script:DatabaseUrl = "postgres://postgres:postgres@127.0.0.1:54329/spreadeater_monitor"
$script:ComposeFile = Join-Path $script:RepoRoot "docker-compose.monitor.yml"
$script:RuntimeDir = Join-Path $script:RepoRoot ".monitor-runtime"
$script:StdOutLog = Join-Path $script:RuntimeDir "monitor.stdout.log"
$script:StdErrLog = Join-Path $script:RuntimeDir "monitor.stderr.log"
$script:MonitorPort = 8080
$script:PostgresPort = 54329

# Set CARGO_TARGET_DIR to avoid spaces-in-path issue with dlltool on Windows
$env:CARGO_TARGET_DIR = "C:\rust-build\spreadeater"
# Add MSYS2 MinGW to PATH
if (Test-Path "C:\msys64\mingw64\bin") {
    $env:PATH = "C:\msys64\mingw64\bin;$env:PATH"
}

function Get-CargoExecutable {
    $cargo = Get-Command cargo -ErrorAction SilentlyContinue
    if ($cargo) {
        return $cargo.Source
    }

    $fallback = Join-Path $env:USERPROFILE ".cargo\bin\cargo.exe"
    if (Test-Path $fallback) {
        return $fallback
    }

    throw "cargo.exe was not found. Install Rust or add cargo to PATH."
}

function Ensure-RuntimeDirectory {
    if (-not (Test-Path $script:RuntimeDir)) {
        New-Item -ItemType Directory -Path $script:RuntimeDir | Out-Null
    }
}

function Test-TcpPort {
    param(
        [Parameter(Mandatory = $true)]
        [string]$HostName,
        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $async = $client.BeginConnect($HostName, $Port, $null, $null)
        if (-not $async.AsyncWaitHandle.WaitOne(500)) {
            return $false
        }

        $client.EndConnect($async)
        return $true
    } catch {
        return $false
    } finally {
        $client.Dispose()
    }
}

function Wait-TcpPort {
    param(
        [Parameter(Mandatory = $true)]
        [string]$HostName,
        [Parameter(Mandatory = $true)]
        [int]$Port,
        [int]$TimeoutSeconds = 60,
        [string]$Label = "service"
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-TcpPort -HostName $HostName -Port $Port) {
            return $true
        }

        Start-Sleep -Seconds 1
    }

    throw "$Label did not become ready within $TimeoutSeconds seconds."
}

function Test-MonitorReady {
    try {
        $response = Invoke-WebRequest -Uri $script:MonitorUrl -UseBasicParsing -TimeoutSec 2
        return $response.StatusCode -ge 200 -and $response.StatusCode -lt 500
    } catch {
        return $false
    }
}

function Wait-MonitorReady {
    param([int]$TimeoutSeconds = 60)

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        if (Test-MonitorReady) {
            return $true
        }

        Start-Sleep -Seconds 1
    }

    return $false
}

function Get-MonitorProcesses {
    $processes = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
        $_.CommandLine -and (
            (($_.Name -ieq "spreadeater-monitor.exe") -and ($_.CommandLine -match "\bserve\b")) -or
            (($_.Name -ieq "cargo.exe") -and ($_.CommandLine -match "spreadeater-monitor") -and ($_.CommandLine -match "\bserve\b"))
        )
    }

    @($processes)
}

function Stop-MonitorServer {
    $processes = Get-MonitorProcesses
    if (@($processes).Count -eq 0) {
        Write-Host "Monitor server is not running."
        return
    }

    $processIds = $processes | Select-Object -ExpandProperty ProcessId -Unique
    foreach ($processId in $processIds) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }

    Start-Sleep -Seconds 1
    Write-Host "Monitor server stopped."
}

function Ensure-DockerMonitor {
    $docker = Get-Command docker -ErrorAction SilentlyContinue
    if (-not $docker) {
        throw "docker.exe was not found. Install Docker Desktop or add docker to PATH."
    }

    & $docker.Source compose -f $script:ComposeFile up -d
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to start Postgres with docker compose."
    }

    $null = Wait-TcpPort -HostName "127.0.0.1" -Port $script:PostgresPort -TimeoutSeconds 60 -Label "Postgres"
}

function Start-MonitorServer {
    param(
        [switch]$EnsureDocker
    )

    if ($EnsureDocker) {
        Ensure-DockerMonitor
    }

    if (Test-MonitorReady) {
        return
    }

    $existingProcesses = Get-MonitorProcesses
    if (@($existingProcesses).Count -gt 0) {
        if (Wait-MonitorReady -TimeoutSeconds 10) {
            return
        }

        Stop-MonitorServer
    }

    Ensure-RuntimeDirectory
    Remove-Item $script:StdOutLog, $script:StdErrLog -ErrorAction SilentlyContinue

    $cargo = Get-CargoExecutable
    $process = Start-Process `
        -FilePath $cargo `
        -ArgumentList @("run", "-p", "spreadeater-monitor", "--", "serve", "--database-url", $script:DatabaseUrl) `
        -WorkingDirectory $script:RepoRoot `
        -RedirectStandardOutput $script:StdOutLog `
        -RedirectStandardError $script:StdErrLog `
        -PassThru

    if (Wait-MonitorReady -TimeoutSeconds 180) {
        return
    }

    $runningProcess = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if ($runningProcess) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
    }

    throw "Monitor server failed to become ready. See $script:StdOutLog and $script:StdErrLog."
}

function Stop-MonitorStack {
    Stop-MonitorServer

    $docker = Get-Command docker -ErrorAction SilentlyContinue
    if ($docker) {
        $dockerProcess = Start-Process `
            -FilePath $docker.Source `
            -ArgumentList @("compose", "-f", $script:ComposeFile, "down") `
            -WorkingDirectory $script:RepoRoot `
            -WindowStyle Hidden `
            -PassThru

        if (-not $dockerProcess.WaitForExit(15000)) {
            Stop-Process -Id $dockerProcess.Id -Force -ErrorAction SilentlyContinue
            Write-Warning "Docker shutdown did not finish within 15 seconds. If the Postgres container is still running, stop it from Docker Desktop."
        } elseif ($dockerProcess.ExitCode -ne 0) {
            throw "Failed to stop docker compose monitor services."
        }
    }

    Write-Host "Monitor stack stopped."
}

function Write-MonitorReadyMessage {
    Write-Host "Monitor ready at $script:MonitorUrl"
    Write-Host "Logs: $script:StdOutLog"
}
