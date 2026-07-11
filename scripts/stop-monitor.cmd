@echo off
setlocal
cd /d "%~dp0\.."

taskkill /f /im spreadeater-monitor.exe >nul 2>nul
docker compose -f .\docker-compose.monitor.yml down
if errorlevel 1 exit /b 1

echo Monitor stack stopped.
