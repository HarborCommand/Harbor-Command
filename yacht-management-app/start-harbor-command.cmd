@echo off
setlocal

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start-harbor-command.ps1"
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
  echo.
  echo Harbor Command could not start correctly.
  echo Try launching it manually from:
  echo   %~dp0server.mjs
  pause
)

exit /b %EXIT_CODE%
