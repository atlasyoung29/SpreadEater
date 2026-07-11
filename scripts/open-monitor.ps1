. "$PSScriptRoot\monitor-common.ps1"

if (Test-MonitorReady) {
    Write-MonitorReadyMessage
    exit 0
}

Ensure-DockerMonitor
Write-MonitorReadyMessage

$cargo = Get-CargoExecutable
& $cargo run -p spreadeater-monitor -- serve --database-url $script:DatabaseUrl
