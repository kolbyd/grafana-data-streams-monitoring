param([switch]$SkipKafka, [switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
Push-Location $PSScriptRoot
try {
    if (!$SkipKafka) {
        docker compose -p dsm-dotnet-lab -f compose.yaml up -d --wait --wait-timeout 120 --pull never
        if ($LASTEXITCODE -ne 0) { throw 'Isolated Kafka did not become ready.' }
    }
    if (!$SkipBuild) {
        dotnet restore DotNetKafkaLab.slnx --locked-mode -m:1 -nr:false
        if ($LASTEXITCODE -ne 0) { throw 'Restore failed.' }
        dotnet test DotNetKafkaLab.slnx -c Release --no-restore -m:1 -nr:false -p:UseSharedCompilation=false
        if ($LASTEXITCODE -ne 0) { throw 'Unit tests failed.' }
    }
    $runId = 'smoke-' + (Get-Date -Format 'yyyyMMdd-HHmmss')
    dotnet src/DotNetKafkaLab/bin/Release/net8.0/DotNetKafkaLab.dll smoke --count 24 --rate 6 --duration 75 --run-id $runId --no-export
    if ($LASTEXITCODE -ne 0) { throw 'Kafka smoke test failed.' }
}
finally { Pop-Location }
