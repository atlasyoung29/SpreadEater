param(
    [Parameter(Mandatory = $true)]
    [string]$Scenario
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$fixtureDir = Join-Path $repoRoot "fixtures\hedge_live_probe_scenarios"
$envFile = Join-Path $repoRoot ".env"

function Import-DotEnvFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    if (-not (Test-Path $Path)) {
        return
    }

    foreach ($line in Get-Content $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith("#")) {
            continue
        }

        $parts = $trimmed -split '=', 2
        if ($parts.Count -ne 2) {
            continue
        }

        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        Set-Item -Path ("Env:" + $name) -Value $value
    }
}

function Resolve-ScenarioPath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InputPath,
        [Parameter(Mandatory = $true)]
        [string]$FixtureDirectory
    )

    if (Test-Path $InputPath) {
        return (Resolve-Path $InputPath).Path
    }

    $fixturePath = Join-Path $FixtureDirectory $InputPath
    if (Test-Path $fixturePath) {
        return (Resolve-Path $fixturePath).Path
    }

    if (-not $InputPath.EndsWith(".json")) {
        $jsonFixturePath = Join-Path $FixtureDirectory ($InputPath + ".json")
        if (Test-Path $jsonFixturePath) {
            return (Resolve-Path $jsonFixturePath).Path
        }
    }

    throw "Scenario file not found: $InputPath"
}

$scenarioPath = Resolve-ScenarioPath -InputPath $Scenario -FixtureDirectory $fixtureDir

Import-DotEnvFile -Path $envFile
$env:SPREADEATER_HEDGE_LIVE_PROBE_SCENARIO = $scenarioPath

Write-Host "Running connect-only user stream smoke test with scenario: $scenarioPath"
& cargo test --bin spreadeater live_probe_user_stream_smoke_connects_without_orders -- --ignored --nocapture
