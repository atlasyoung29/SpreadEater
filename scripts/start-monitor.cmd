@echo off
setlocal
cd /d "%~dp0\.."

set "MONITOR_URL=http://127.0.0.1:8080"
set "DATABASE_URL=postgres://postgres:postgres@127.0.0.1:54329/spreadeater_monitor"
set "CARGO_EXE="

rem Add MSYS2 MinGW to PATH (provides dlltool.exe required by GNU toolchain)
if exist "C:\msys64\mingw64\bin" set "PATH=C:\msys64\mingw64\bin;%PATH%"
set "CARGO_TARGET_DIR=C:\rust-build\spreadeater"

call :resolve_cargo || exit /b 1

for /f "tokens=5" %%P in ('netstat -ano ^| findstr /r /c:":8080 .*LISTENING"') do (
  tasklist /fi "PID eq %%P" /fo csv /nh | find /i "spreadeater-monitor.exe" >nul
  if not errorlevel 1 (
    echo Monitor already running at %MONITOR_URL%
    exit /b 0
  )
)

docker compose -f .\docker-compose.monitor.yml up -d
if errorlevel 1 exit /b 1

echo Monitor ready at %MONITOR_URL%
"%CARGO_EXE%" run -p spreadeater-monitor -- serve --database-url %DATABASE_URL%
exit /b %errorlevel%

:resolve_cargo
where cargo >nul 2>nul
if not errorlevel 1 (
  for /f "delims=" %%I in ('where cargo') do (
    set "CARGO_EXE=%%I"
    exit /b 0
  )
)

if exist "%USERPROFILE%\.cargo\bin\cargo.exe" (
  set "CARGO_EXE=%USERPROFILE%\.cargo\bin\cargo.exe"
  exit /b 0
)

echo cargo.exe was not found. Install Rust or add cargo to PATH.
exit /b 1
