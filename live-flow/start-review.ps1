param([string]$Url = 'http://127.0.0.1:4318')
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    Write-Host ('Review viewer: ' + $Url)
    Write-Host 'Choose Start demo in the viewer, or send OTLP/HTTP traces to /v1/traces.'
    dotnet run --project src/LiveFlow --no-launch-profile -- --urls $Url
    if ($LASTEXITCODE -ne 0) { throw 'The service did not start successfully.' }
} finally { Pop-Location }
