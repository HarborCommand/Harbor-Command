param(
  [string]$ShortcutPath = ""
)

$ErrorActionPreference = "Stop"

$projectRoot = $PSScriptRoot
$launcherPath = Join-Path $projectRoot "start-harbor-command.cmd"
$logoPngPath = Join-Path $projectRoot "public\\assets\\harbor-command-logo.png"
$iconPath = Join-Path $projectRoot "public\\assets\\harbor-command-launcher.ico"

if (-not (Test-Path $launcherPath)) {
  throw "Could not find start-harbor-command.cmd in $projectRoot."
}

if (Test-Path $logoPngPath) {
  Add-Type -AssemblyName System.Drawing

  $sourceImage = [System.Drawing.Image]::FromFile($logoPngPath)
  $iconCanvas = New-Object System.Drawing.Bitmap(256, 256)
  $graphics = [System.Drawing.Graphics]::FromImage($iconCanvas)
  $graphics.Clear([System.Drawing.Color]::Transparent)
  $graphics.InterpolationMode = [System.Drawing.Drawing2D.InterpolationMode]::HighQualityBicubic
  $graphics.SmoothingMode = [System.Drawing.Drawing2D.SmoothingMode]::HighQuality
  $graphics.PixelOffsetMode = [System.Drawing.Drawing2D.PixelOffsetMode]::HighQuality

  $scale = [Math]::Min(256 / $sourceImage.Width, 256 / $sourceImage.Height)
  $targetWidth = [int][Math]::Round($sourceImage.Width * $scale)
  $targetHeight = [int][Math]::Round($sourceImage.Height * $scale)
  $targetX = [int][Math]::Floor((256 - $targetWidth) / 2)
  $targetY = [int][Math]::Floor((256 - $targetHeight) / 2)
  $targetRect = New-Object System.Drawing.Rectangle($targetX, $targetY, $targetWidth, $targetHeight)
  $graphics.DrawImage($sourceImage, $targetRect)

  $pngStream = New-Object System.IO.MemoryStream
  $iconCanvas.Save($pngStream, [System.Drawing.Imaging.ImageFormat]::Png)
  $pngBytes = $pngStream.ToArray()

  $graphics.Dispose()
  $iconCanvas.Dispose()
  $sourceImage.Dispose()
  $pngStream.Dispose()

  $stream = New-Object System.IO.MemoryStream
  $writer = New-Object System.IO.BinaryWriter($stream)

  $writer.Write([UInt16]0)
  $writer.Write([UInt16]1)
  $writer.Write([UInt16]1)
  $writer.Write([Byte]0)
  $writer.Write([Byte]0)
  $writer.Write([Byte]0)
  $writer.Write([Byte]0)
  $writer.Write([UInt16]1)
  $writer.Write([UInt16]32)
  $writer.Write([UInt32]$pngBytes.Length)
  $writer.Write([UInt32]22)
  $writer.Write($pngBytes)
  $writer.Flush()

  [System.IO.File]::WriteAllBytes($iconPath, $stream.ToArray())
  $writer.Dispose()
  $stream.Dispose()
}

if (-not $ShortcutPath) {
  $desktopPath = [Environment]::GetFolderPath("Desktop")
  $ShortcutPath = Join-Path $desktopPath "Harbor Command.lnk"
}

$shell = New-Object -ComObject WScript.Shell
$shortcut = $shell.CreateShortcut($ShortcutPath)
$shortcut.TargetPath = $launcherPath
$shortcut.WorkingDirectory = $projectRoot
$shortcut.Description = "Launch Harbor Command and open the local app."
$shortcut.WindowStyle = 1
if (Test-Path $iconPath) {
  $shortcut.IconLocation = $iconPath
}
$shortcut.Save()

Write-Output "Created shortcut: $ShortcutPath"
