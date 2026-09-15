$ErrorActionPreference = 'Stop'
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
python (Join-Path $ProjectDir 'momentum_monitor.py') --scan
