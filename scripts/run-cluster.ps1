# Start all nodes defined in config/cluster.local.toml (local 3-node by default).
# Usage: .\scripts\run-cluster.ps1
#        .\scripts\run-cluster.ps1 -Config config/cluster.docker.toml  # won't work locally (docker IPs)

param(
    [string]$Config = "config/cluster.local.toml"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $Root

$env:CLUSTER_CONFIG = $Config
$ids = & cargo run --quiet --bin raft-node -- --list-nodes 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "failed to read node ids from $Config"
}

Write-Host "cluster: $Config"
Write-Host "nodes: $($ids -join ', ')"
Write-Host "Press Ctrl+C to stop all nodes."

$jobs = @()
foreach ($id in $ids) {
    $jobs += Start-Job -ScriptBlock {
        param($Root, $Config, $NodeId)
        Set-Location $Root
        $env:CLUSTER_CONFIG = $Config
        $env:NODE_ID = $NodeId
        $env:RUST_LOG = "info"
        cargo run --bin raft-node 2>&1
    } -ArgumentList $Root, $Config, $id
    Start-Sleep -Milliseconds 300
}

try {
    while ($true) {
        Receive-Job -Job $jobs -Keep | ForEach-Object { Write-Host $_ }
        Start-Sleep -Milliseconds 200
    }
} finally {
    $jobs | Stop-Job -ErrorAction SilentlyContinue
    $jobs | Remove-Job -Force -ErrorAction SilentlyContinue
}
