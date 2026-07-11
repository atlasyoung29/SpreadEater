Stop-Process -Name spreadeater -Force -ErrorAction SilentlyContinue
if ($?) { Write-Host "SpreadEater stopped." } else { Write-Host "SpreadEater is not running." }
