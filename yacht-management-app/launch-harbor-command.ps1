param(
  [int]$Port = 8787
)

$ErrorActionPreference = "Stop"

$projectRoot = $PSScriptRoot
$serverScript = Join-Path $projectRoot "server.mjs"
$dataDirectory = Join-Path $projectRoot "data"
$stdoutLog = Join-Path $dataDirectory "harbor-command-server.stdout.log"
$stderrLog = Join-Path $dataDirectory "harbor-command-server.stderr.log"

if (-not (Test-Path $dataDirectory)) {
  New-Item -ItemType Directory -Path $dataDirectory | Out-Null
}

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

$healthUrl = "http://localhost:$Port/api/health"
$appUrl = "http://localhost:$Port/?launch=$([DateTimeOffset]::Now.ToUnixTimeMilliseconds())"

function Test-HarborCommandHealth {
  try {
    $response = Invoke-WebRequest -UseBasicParsing $healthUrl -TimeoutSec 2
    return $response.StatusCode -eq 200 -and $response.Content -match '"ok":true'
  } catch {
    return $false
  }
}

function Get-ListeningConnection {
  try {
    return Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction Stop | Select-Object -First 1
  } catch {
    return $null
  }
}

if (Test-HarborCommandHealth) {
  Start-Process $appUrl | Out-Null
  Write-Output "Harbor Command already running."
  exit 0
}

$listener = Get-ListeningConnection
if ($listener) {
  throw "Port $Port is already in use, but Harbor Command did not respond to the health check."
}

$startInfo = New-Object System.Diagnostics.ProcessStartInfo
$startInfo.FileName = $nodePath
$startInfo.Arguments = ('"{0}" "--port={1}"' -f $serverScript, $Port)
$startInfo.WorkingDirectory = $projectRoot
$startInfo.UseShellExecute = $false
$startInfo.RedirectStandardOutput = $true
$startInfo.RedirectStandardError = $true
$startInfo.CreateNoWindow = $true

$serverProcess = New-Object System.Diagnostics.Process
$serverProcess.StartInfo = $startInfo
$serverProcess.EnableRaisingEvents = $true

$stdoutWriter = [System.IO.StreamWriter]::new($stdoutLog, $false)
$stderrWriter = [System.IO.StreamWriter]::new($stderrLog, $false)
$serverProcess.add_OutputDataReceived({
  param($sender, $eventArgs)
  if ($null -ne $eventArgs.Data) {
    $stdoutWriter.WriteLine($eventArgs.Data)
    $stdoutWriter.Flush()
  }
})
$serverProcess.add_ErrorDataReceived({
  param($sender, $eventArgs)
  if ($null -ne $eventArgs.Data) {
    $stderrWriter.WriteLine($eventArgs.Data)
    $stderrWriter.Flush()
  }
})

if (-not $serverProcess.Start()) {
  throw "Harbor Command could not launch the local Node.js server process."
}

$serverProcess.BeginOutputReadLine()
$serverProcess.BeginErrorReadLine()

for ($attempt = 0; $attempt -lt 20; $attempt++) {
  Start-Sleep -Milliseconds 500

  if (Test-HarborCommandHealth) {
    Start-Process $appUrl | Out-Null
    Write-Output "Harbor Command started successfully."
    exit 0
  }

  if ($serverProcess.HasExited) {
    break
  }
}

$errorTail = ""
if (Test-Path $stderrLog) {
  $errorTail = (Get-Content $stderrLog -Tail 20 -ErrorAction SilentlyContinue) -join [Environment]::NewLine
}

if (-not $errorTail) {
  $errorTail = "No stderr output was captured. Check the server log files in the data folder."
}

throw "Harbor Command could not confirm that the local service started.`n`n$errorTail"
