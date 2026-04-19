param(
  [int]$Port = 8787
)

$projectPath = Split-Path -Parent $PSCommandPath
$appUrl = "http://127.0.0.1:$Port/"
$healthUrl = "${appUrl}api/health"
$launcherScript = Join-Path $projectPath 'launch-harbor-command-server.ps1'

function Test-HarborHealth {
  param([string]$Url)

  try {
    $response = Invoke-WebRequest -UseBasicParsing $Url -TimeoutSec 2
    return $response.StatusCode -eq 200
  } catch {
    return $false
  }
}

function Open-HarborCommand {
  param([string]$Url)

  $launchUrl = '{0}?launch={1}' -f $Url.TrimEnd('/'), ([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())
  Start-Process $launchUrl | Out-Null
}

function Get-HarborListener {
  param([int]$Port)

  $listener = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
  if ($listener) {
    return $listener
  }

  $netstatLine = netstat -ano | Select-String -Pattern (":{0}\s+.*LISTENING\s+" -f $Port) | Select-Object -First 1
  if (-not $netstatLine) {
    return $null
  }

  $parts = ($netstatLine.Line -replace '^\s+', '') -split '\s+'
  if ($parts.Length -lt 5) {
    return $null
  }

  return [PSCustomObject]@{
    LocalAddress = ($parts[1] -split ':')[0]
    LocalPort = $Port
    State = $parts[3]
    OwningProcess = [int]$parts[4]
  }
}

if (Test-HarborHealth -Url $healthUrl) {
  Open-HarborCommand -Url $appUrl
  exit 0
}

$listener = Get-HarborListener -Port $Port
if ($listener) {
  $listenerProcess = Get-Process -Id $listener.OwningProcess -ErrorAction SilentlyContinue
  if ($listenerProcess -and $listenerProcess.ProcessName -ieq 'node') {
    Stop-Process -Id $listener.OwningProcess -Force -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 1
  } else {
    Write-Host ''
    Write-Host "Port $Port is already in use by another process."
    Write-Host 'Close that app or change Harbor Command to a different port, then try again.'
    exit 1
  }
}

Write-Host 'Starting Harbor Command local service...'
& powershell -NoProfile -ExecutionPolicy Bypass -File $launcherScript -ProjectPath $projectPath -Port $Port | Out-Null

for ($attempt = 0; $attempt -lt 30; $attempt += 1) {
  if (Test-HarborHealth -Url $healthUrl) {
    Open-HarborCommand -Url $appUrl
    exit 0
  }

  Start-Sleep -Seconds 1
}

Write-Host ''
Write-Host 'Harbor Command could not confirm that the local service started.'
Write-Host 'Try running server.mjs manually from:'
Write-Host "  $projectPath"
exit 1
