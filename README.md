# SpreadEater

## Monitor Commands

Run these from the repository root (`<repo-root>`).

You can either:
- type them in PowerShell or Command Prompt
- or double-click the matching `.cmd` file in File Explorer

### Start Everything

Starts Docker Postgres, starts the monitor server, and prints the dashboard URL.
This command keeps running in that terminal while the monitor server is up.

```powershell
.\scripts\start-monitor.cmd
```

### Open The Monitor

If the monitor is already running, this just prints the dashboard URL.
If it is not running, it starts it first and then prints the URL.

```powershell
.\scripts\open-monitor.cmd
```

### Restart The Monitor

Restarts the monitor server, keeps Docker up, and prints the dashboard URL again.
This command also keeps running in that terminal while the monitor server is up.

```powershell
.\scripts\restart-monitor.cmd
```

### Stop Everything

Stops the monitor server and shuts down the monitor Docker Postgres container.

```powershell
.\scripts\stop-monitor.cmd
```

### Dashboard URL

The commands print this URL:

```text
http://127.0.0.1:8080
```

### Monitor Tabs

The monitor now has dedicated tabs for:
- `Overview`
- `Open Orders`
- `Inventory`
- `History`
- `Errors`
- `Watchlist`
- `Config`

## Bot Commands

### Safe Full-Pipeline Dry Run

This is the normal monitored dry-run command for the bot.

```powershell
cargo run -- live --dry-run
```

If you want the monitor `Errors` tab to capture bot runtime errors, run the bot with error-only logging redirected to the standard monitor log file:

```powershell
$env:RUST_LOG="error"
New-Item -ItemType Directory -Force .\data\logs | Out-Null
cargo run -- live --dry-run *>> .\data\logs\spreadeater-bot.log
```

### Real Trading

This places real orders.

```powershell
cargo run -- live
```

Real trading with redirected error logging:

```powershell
$env:RUST_LOG="error"
New-Item -ItemType Directory -Force .\data\logs | Out-Null
cargo run -- live *>> .\data\logs\spreadeater-bot.log
```

### Decision-Only Loop

This runs the older decision loop. It now emits decision/watchlist monitor telemetry only, not order/fill/hedge events.

```powershell
cargo run -- dry-run-loop
```

Decision-only loop with redirected error logging:

```powershell
$env:RUST_LOG="error"
New-Item -ItemType Directory -Force .\data\logs | Out-Null
cargo run -- dry-run-loop *>> .\data\logs\spreadeater-bot.log
```
