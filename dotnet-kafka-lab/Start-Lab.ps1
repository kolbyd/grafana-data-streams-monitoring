param(
    [ValidateRange(10,3600)][int]$DurationSeconds = 180,
    [ValidateRange(0.1,20)][double]$Rate = 1,
    [ValidateRange(0,100000)][int]$Count = 0,
    [ValidatePattern('^[a-zA-Z0-9-]{1,64}$')][string]$RunId = (Get-Date -Format 'yyyyMMdd-HHmmss'),
    [switch]$Background,
    [switch]$NoExport,
    [switch]$ExportMetrics,
    [switch]$SkipBuild,
    [switch]$SkipKafka
)
$ErrorActionPreference = 'Stop'
$labRoot = $PSScriptRoot
$stateRoot = Join-Path $labRoot '.lab-state'
New-Item -ItemType Directory -Path $stateRoot -Force | Out-Null
$activeFile = Join-Path $stateRoot 'active.json'
if (Test-Path -LiteralPath $activeFile) {
    $previous = Get-Content -LiteralPath $activeFile -Raw | ConvertFrom-Json
    if (Get-Process -Id $previous.ProcessId -ErrorAction SilentlyContinue) { throw 'A previous lab process is still present. Run Stop-Lab.ps1 first.' }
}
$stopFile = Join-Path $stateRoot 'stop.request'
if (Test-Path -LiteralPath $stopFile) { Remove-Item -LiteralPath $stopFile }
Push-Location $labRoot
try {
    if (!$SkipKafka) {
        docker compose -p dsm-dotnet-lab -f (Join-Path $labRoot 'compose.yaml') up -d --wait --wait-timeout 120 --pull never
        if ($LASTEXITCODE -ne 0) { throw 'Isolated Kafka did not become ready.' }
    }
    if (!$SkipBuild) {
        dotnet restore DotNetKafkaLab.slnx --locked-mode -m:1 -nr:false
        if ($LASTEXITCODE -ne 0) { throw 'Restore failed.' }
        dotnet build DotNetKafkaLab.slnx -c Release --no-restore -m:1 -nr:false -p:UseSharedCompilation=false
        if ($LASTEXITCODE -ne 0) { throw 'Build failed.' }
    }
    if (!$env:OTEL_EXPORTER_OTLP_ENDPOINT -and !$env:OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) { $env:OTEL_EXPORTER_OTLP_ENDPOINT = 'http://127.0.0.1:4318' }
    if (!$env:OTEL_EXPORTER_OTLP_PROTOCOL -and !$env:OTEL_EXPORTER_OTLP_TRACES_PROTOCOL) { $env:OTEL_EXPORTER_OTLP_PROTOCOL = 'http/protobuf' }
    $application = Join-Path $labRoot 'src/DotNetKafkaLab/bin/Release/net8.0/DotNetKafkaLab.dll'
    $arguments = @($application, 'run', '--run-id', $RunId, '--duration', $DurationSeconds.ToString(), '--rate', $Rate.ToString([Globalization.CultureInfo]::InvariantCulture), '--count', $Count.ToString(), '--state-dir', $stateRoot, '--stop-file', $stopFile)
    if ($NoExport) { $arguments += '--no-export' }
    if ($ExportMetrics) { $arguments += '--export-metrics' }
    if ($Background) {
        # Start-Process requires quoted argument strings for paths containing spaces. No secret is an argument.
        $quoted = $arguments | ForEach-Object { '"' + $_.Replace('"','\"') + '"' }
        $parameters = @{
            FilePath = 'dotnet'; ArgumentList = $quoted; WorkingDirectory = $labRoot; PassThru = $true
            RedirectStandardOutput = (Join-Path $stateRoot "$RunId.stdout.log")
            RedirectStandardError = (Join-Path $stateRoot "$RunId.stderr.log")
        }
        if ($IsWindows -or $PSVersionTable.PSEdition -eq 'Desktop') { $parameters.WindowStyle = 'Hidden' }
        $process = Start-Process @parameters
        @{ ProcessId = $process.Id; RunId = $RunId; StartedAt = (Get-Date).ToUniversalTime().ToString('o') } | ConvertTo-Json | Set-Content -LiteralPath $activeFile
        Write-Host "Lab process $($process.Id) started; run $RunId. Stop with .\Stop-Lab.ps1."
    }
    else {
        & dotnet @arguments
        if ($LASTEXITCODE -ne 0) { throw 'Lab run failed; inspect .lab-state counters/logs.' }
    }
}
finally { Pop-Location }
