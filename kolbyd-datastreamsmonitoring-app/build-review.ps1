[CmdletBinding()]
param([switch]$SkipTests)

$ErrorActionPreference = 'Stop'
$reviewRoot = $PSScriptRoot
$taskWork = Join-Path (Split-Path (Split-Path $reviewRoot -Parent) -Parent) 'work'
$npmCache = Join-Path $taskWork 'npm-cache'
$localGo = Join-Path $taskWork 'go-toolchain/go/bin'
$localMage = Join-Path $taskWork 'go-cache/bin/mage.exe'
$previousPath = $env:PATH
$previousGoPath = $env:GOPATH
$previousGoCache = $env:GOCACHE
$previousGoMaxProcs = $env:GOMAXPROCS
$previousGoFlags = $env:GOFLAGS

Push-Location $reviewRoot
try {
    if (Test-Path (Join-Path $localGo 'go.exe')) {
        $env:PATH = "$localGo;$env:PATH"
        $env:GOPATH = Join-Path $taskWork 'go-cache'
        $env:GOCACHE = Join-Path $taskWork 'go-build-cache'
    }
    $env:GOMAXPROCS = '4'
    $env:GOFLAGS = '-p=4'
    $mageCommand = if (Test-Path $localMage) { $localMage } else { (Get-Command mage -ErrorAction SilentlyContinue).Source }
    if (-not (Get-Command go -ErrorAction SilentlyContinue) -or -not $mageCommand) {
        throw 'Go and Mage are required for the backend. Install Go, then run: go install github.com/magefile/mage@v1.17.2'
    }
    New-Item -ItemType Directory -Force $npmCache | Out-Null
    $frontendCommand = if ($SkipTests) {
        'npm ci --ignore-scripts --no-audit --no-fund && npm run typecheck && npm run build'
    } else {
        'npm ci --ignore-scripts --no-audit --no-fund && npm run typecheck && npm run test:ci && npm run build'
    }
    & docker run --rm --mount "type=bind,source=$reviewRoot,target=/workspace" --mount 'type=volume,source=liveflow-plugin-node-modules,target=/workspace/node_modules' --mount "type=bind,source=$npmCache,target=/root/.npm" --workdir /workspace node:24.15.0-bookworm-slim sh -ec $frontendCommand
    if ($LASTEXITCODE -ne 0) { throw 'The official Webpack frontend build or checks failed.' }
    if (-not $SkipTests) {
        & go test ./pkg/...
        if ($LASTEXITCODE -ne 0) { throw 'Backend tests failed.' }
    }
    & $mageCommand -v build:linux
    if ($LASTEXITCODE -ne 0) { throw 'The Mage backend build failed.' }
    & docker run --rm --mount "type=bind,source=$reviewRoot,target=/workspace" --workdir /workspace node:24.15.0-bookworm-slim sh -ec 'chmod 0755 dist/gpx_*'
    if ($LASTEXITCODE -ne 0) { throw 'Could not set backend executable permissions.' }
    $noticeFile = Join-Path $reviewRoot 'THIRD_PARTY_NOTICES.txt'
    if (Test-Path -LiteralPath $noticeFile) {
        Copy-Item -LiteralPath $noticeFile -Destination (Join-Path $reviewRoot 'dist/THIRD_PARTY_NOTICES.txt')
    }
    Write-Host 'Plugin built into dist/ using the create-plugin Webpack configuration and SDK Mage targets.'
} finally {
    $env:PATH = $previousPath
    $env:GOPATH = $previousGoPath
    $env:GOCACHE = $previousGoCache
    $env:GOMAXPROCS = $previousGoMaxProcs
    $env:GOFLAGS = $previousGoFlags
    Pop-Location
}
