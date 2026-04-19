@echo off
setlocal

set "PORT=%~1"
if "%PORT%"=="" set "PORT=8787"

set "NODE_EXE=%ProgramFiles%\nodejs\node.exe"
if not exist "%NODE_EXE%" set "NODE_EXE=node"

cd /d "%~dp0"
"%NODE_EXE%" "server.mjs" --port=%PORT%
