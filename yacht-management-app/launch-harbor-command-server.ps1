param(
  [string]$ProjectPath = (Split-Path -Parent $PSCommandPath),
  [int]$Port = 8787
)

$runnerPath = Join-Path $ProjectPath 'run-harbor-command-server.cmd'
$argumentList = @('/k', ('"{0}" {1}' -f $runnerPath, $Port))

Start-Process -FilePath 'cmd.exe' -ArgumentList $argumentList -WorkingDirectory $ProjectPath -WindowStyle Minimized
