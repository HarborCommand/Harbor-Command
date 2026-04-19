param(
  [int]$Port = 8787
)

$ErrorActionPreference = "Stop"

$serverScript = Join-Path $PSScriptRoot "server.mjs"
$nodeCandidates = @(
  "C:\Program Files\nodejs\node.exe",
  "C:\Program Files (x86)\nodejs\node.exe"
)

try {
  $nodeCommand = Get-Command node -ErrorAction Stop
  if ($nodeCommand.Source) {
    $nodeCandidates = @($nodeCommand.Source) + $nodeCandidates
  }
} catch {
}

$nodePath = $nodeCandidates | Where-Object { $_ -and (Test-Path $_) } | Select-Object -First 1

if (-not $nodePath) {
  throw "Node.js was not found. Install Node.js, then run start-harbor-command.cmd again."
}

& $nodePath $serverScript "--port=$Port"
