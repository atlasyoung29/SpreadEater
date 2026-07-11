. "$PSScriptRoot\monitor-common.ps1"

Ensure-DockerMonitor
Write-MonitorReadyMessage

$cargo = Get-CargoExecutable
& $cargo run -p spreadeater-monitor -- serve --database-url $script:DatabaseUrl
