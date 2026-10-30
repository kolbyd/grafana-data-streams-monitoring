param([switch]$StopKafka, [switch]$ResetData)
$ErrorActionPreference = 'Stop'
$stateRoot = Join-Path $PSScriptRoot '.lab-state'
New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null
Set-Content -LiteralPath (Join-Path $stateRoot 'stop.request') -Value 'stop'
$activeFile = Join-Path $stateRoot 'active.json'
if (Test-Path -LiteralPath $activeFile) {
    $active = Get-Content -LiteralPath $activeFile -Raw | ConvertFrom-Json
    $process = Get-Process -Id $active.ProcessId -ErrorAction SilentlyContinue
    if ($process) {
        try { Wait-Process -Id $process.Id -Timeout 30 -ErrorAction Stop }
        catch { Write-Warning 'The process has not exited yet. The stop request remains in place; no unrelated process was killed.' }
    }
}
if ($StopKafka -or $ResetData) {
    $arguments = @('compose', '-p', 'dsm-dotnet-lab', '-f', (Join-Path $PSScriptRoot 'compose.yaml'), 'down')
    if ($ResetData) { $arguments += '--volumes' }
    & docker @arguments
    if ($LASTEXITCODE -ne 0) { throw 'Could not stop the isolated lab Compose project.' }
}
Write-Host 'Stop requested. Kafka data is retained unless -ResetData was explicitly supplied.'
