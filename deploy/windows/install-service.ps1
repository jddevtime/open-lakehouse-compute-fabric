# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# Registers (or removes) a per-user Scheduled Task that runs the OLCF control plane or a worker at logon.  No administrator rights are needed
# and nothing is installed system-wide.  A true Windows service would require a service host; Task Scheduler gives restart-on-failure and
# start-at-logon without one, which is what a developer machine needs.  For an unattended server, run OLCF in Docker or on Linux.
#
#   .\install-service.ps1 -Role server                       # control plane, data in %USERPROFILE%\.olcf
#   .\install-service.ps1 -Role worker -StateDir D:\olcf-worker
#   .\install-service.ps1 -Role server -Uninstall
#
# NOTE: not executed in the environment it was written in (no PowerShell there); reviewed by eye, structure checked by tests/tooling.

[CmdletBinding(SupportsShouldProcess = $true)]
param(
    [ValidateSet('server', 'worker')][string]$Role = 'server',
    [string]$Prefix = $(if ($env:OLCF_PREFIX) { $env:OLCF_PREFIX } else { Join-Path $env:LOCALAPPDATA 'olcf' }),
    [string]$DataDir = $(if ($env:OLCF_DATA_DIR) { $env:OLCF_DATA_DIR } else { Join-Path $env:USERPROFILE '.olcf' }),
    [string]$StateDir = (Join-Path $env:LOCALAPPDATA 'olcf-worker'),
    [int]$Port = 8470,
    [switch]$Uninstall
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$taskName = if ($Role -eq 'server') { 'OLCF-Server' } else { 'OLCF-Worker' }

if ($Uninstall) {
    if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) {
        if ($PSCmdlet.ShouldProcess($taskName, 'Unregister scheduled task')) {
            Stop-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue
            Unregister-ScheduledTask -TaskName $taskName -Confirm:$false
            Write-Host "removed task $taskName"
        }
    } else { Write-Host "task $taskName is not registered" }
    return
}

$exe = if ($Role -eq 'server') { Join-Path $Prefix 'current\Scripts\olcf.exe' } else { Join-Path $Prefix 'current\Scripts\olcf-worker.exe' }
if (-not (Test-Path -LiteralPath $exe)) { throw "cannot find $exe - install OLCF first (scripts\install.ps1) or pass -Prefix" }

if ($Role -eq 'server') {
    $arguments = ('--data-dir "{0}" serve --port {1}' -f $DataDir, $Port)
    $workDir = $DataDir
} else {
    if (-not (Test-Path -LiteralPath (Join-Path $StateDir 'identity.json'))) {
        throw "this machine is not enrolled yet: run  olcf-worker run --enroll-only --url <controller> --state-dir `"$StateDir`"  with OLCF_WORKER_TOKEN set (token from 'olcf worker enroll-token')"
    }
    $arguments = ('run --state-dir "{0}"' -f $StateDir)
    $workDir = $StateDir
}
New-Item -ItemType Directory -Force -Path $workDir | Out-Null

$action = New-ScheduledTaskAction -Execute $exe -Argument $arguments -WorkingDirectory $workDir
$trigger = New-ScheduledTaskTrigger -AtLogOn -User "$env:USERDOMAIN\$env:USERNAME"
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType Interactive -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable `
    -RestartCount 10 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -Hidden

if ($PSCmdlet.ShouldProcess($taskName, "Register scheduled task: $exe $arguments")) {
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $principal -Settings $settings `
        -Description "Open Lakehouse Compute Fabric $Role" -Force | Out-Null
    Write-Host "registered task $taskName (starts at your next logon); start it now with:  Start-ScheduledTask -TaskName $taskName"
}
