$ErrorActionPreference = 'Stop'
$ProjectDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$PythonExe = (Get-Command python).Source
$ScriptPath = Join-Path $ProjectDir 'momentum_monitor.py'
$Action = New-ScheduledTaskAction -Execute $PythonExe -Argument ('"{0}" --scan --respect-session' -f $ScriptPath) -WorkingDirectory $ProjectDir
$Trigger = New-ScheduledTaskTrigger -Once -At (Get-Date).Date.AddMinutes(1) -RepetitionInterval (New-TimeSpan -Minutes 5)
$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 3)
Register-ScheduledTask -TaskName 'MetalsMomentum5m' -Description 'Metals five-minute momentum research scan' -Action $Action -Trigger $Trigger -Settings $Settings -Force
Write-Host 'Task MetalsMomentum5m installed. Off-session runs will be skipped.'
