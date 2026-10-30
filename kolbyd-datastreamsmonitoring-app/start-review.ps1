[CmdletBinding()]
param([switch]$Build, [switch]$RunTests, [switch]$Stop, [string]$ProjectName)

$ErrorActionPreference = 'Stop'
$reviewRoot = $PSScriptRoot
$composeArguments = @()
if ($ProjectName) { $composeArguments = @('-p', $ProjectName) }
$grafanaPort = if ($env:GRAFANA_HTTP_PORT) { $env:GRAFANA_HTTP_PORT } else { '3001' }
$reviewUrl = "http://127.0.0.1:$grafanaPort"
$previousAdminPassword = $env:GRAFANA_ADMIN_PASSWORD
$previousFlowKey = $env:FLOW_SERVICE_API_KEY
$previousGrafanaUrl = $env:GRAFANA_URL

function Read-ReviewSecret([string]$Service, [string]$SecretName) {
    $containerId = & docker compose @composeArguments ps --quiet $Service 2>$null
    if ($LASTEXITCODE -ne 0 -or -not $containerId) { return $null }
    $secretValue = & docker compose @composeArguments exec -T $Service cat "/run/secrets/$SecretName" 2>$null
    if ($LASTEXITCODE -eq 0 -and $secretValue) { return ($secretValue -join '').Trim() }
    return $null
}

function New-ReviewSecret {
    return [Convert]::ToHexString([Security.Cryptography.RandomNumberGenerator]::GetBytes(32)).ToLowerInvariant()
}

Push-Location $reviewRoot
try {
    if ($Build) { & "$reviewRoot/build-review.ps1"; if ($LASTEXITCODE -ne 0) { throw 'Plugin build failed.' } }
    if (-not $Stop -and (-not (Test-Path 'dist/module.js') -or -not (Test-Path 'dist/gpx_data_streams_monitoring_linux_amd64'))) {
        throw 'Build the plugin first with ./build-review.ps1, or run this script with -Build.'
    }
    if (-not $env:GRAFANA_ADMIN_PASSWORD) {
        $env:GRAFANA_ADMIN_PASSWORD = Read-ReviewSecret 'grafana' 'grafana_admin_password'
        if (-not $env:GRAFANA_ADMIN_PASSWORD) { $env:GRAFANA_ADMIN_PASSWORD = New-ReviewSecret }
    }
    if (-not $env:FLOW_SERVICE_API_KEY) {
        $env:FLOW_SERVICE_API_KEY = Read-ReviewSecret 'grafana' 'flow_service_api_key'
        if (-not $env:FLOW_SERVICE_API_KEY) { $env:FLOW_SERVICE_API_KEY = Read-ReviewSecret 'flow-service' 'flow_service_api_key' }
        if (-not $env:FLOW_SERVICE_API_KEY) { $env:FLOW_SERVICE_API_KEY = New-ReviewSecret }
    }
    if ($Stop) {
        & docker compose @composeArguments down
        if ($LASTEXITCODE -ne 0) { throw 'Could not stop the isolated review project.' }
        Write-Host 'Stopped the selected isolated review project.'
        exit 0
    }
    & docker compose @composeArguments up --detach --build --wait --wait-timeout 180
    if ($LASTEXITCODE -ne 0) { throw 'Review services did not become healthy. Inspect Docker Compose logs for the selected project.' }
    Write-Host "Review: $reviewUrl/a/eageraspen2909-datastreamsmonitoring-app/explore"
    Write-Host 'Anonymous access is Viewer only. Secrets were supplied to Docker without writing an env file.'
    if ($RunTests) {
        $env:GRAFANA_URL = $reviewUrl
        & npm run e2e
        if ($LASTEXITCODE -ne 0) { throw 'Plugin browser tests failed.' }
    }
} finally {
    $env:GRAFANA_ADMIN_PASSWORD = $previousAdminPassword
    $env:FLOW_SERVICE_API_KEY = $previousFlowKey
    $env:GRAFANA_URL = $previousGrafanaUrl
    Pop-Location
}
