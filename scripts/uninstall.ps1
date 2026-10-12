# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# OLCF uninstaller for Windows (per user, no administrator rights).  Mirrors scripts/uninstall.sh:
# by default only the PROGRAM is removed (versions, bundled Python, uv, PATH entry, logon task); your data is kept.  Data categories are removed
# independently and only on request:
#   -RemoveMetadata   database, config.toml, jobs, artifacts
#   -RemoveSecrets    master and signing keys, bootstrap password, built-in CA   (irreversible: backups and encrypted secrets become unreadable)
#   -RemoveLogs       logs
#   -RemoveStorage    lakehouse files and tables stored in the data directory   (YOUR DATA)
#   -RemoveRuntimes   Spark runtimes, downloaded JDK, wheelhouse, uv cache
#   -RemoveBackups    backups
#   -RemoveCliConfig  saved CLI logins
#   -Purge            everything above and the data directory itself
# Deleting data needs -Yes (or an interactive "yes"); -DryRun only lists what would be removed.
#
# NOTE: not executed in the environment it was written in (no PowerShell there); reviewed by eye, structure checked by tests/tooling/test_installer.py.

[CmdletBinding()]
param(
    [string]$Prefix = $env:OLCF_PREFIX,
    [string]$DataDir = $env:OLCF_DATA_DIR,
    [switch]$KeepProgram,
    [switch]$RemoveMetadata,
    [switch]$RemoveSecrets,
    [switch]$RemoveLogs,
    [switch]$RemoveStorage,
    [switch]$RemoveRuntimes,
    [switch]$RemoveBackups,
    [switch]$RemoveCliConfig,
    [switch]$Purge,
    [switch]$Yes,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Say([string]$Message) { [Console]::Error.WriteLine($Message) }
function Step([string]$Message) { Say "==> $Message" }
function Fail([string]$Message) { throw "olcf uninstaller: $Message" }

if ($Purge) { $RemoveMetadata = $true; $RemoveSecrets = $true; $RemoveLogs = $true; $RemoveStorage = $true; $RemoveRuntimes = $true; $RemoveBackups = $true; $RemoveCliConfig = $true }
if (-not $Prefix) { $Prefix = Join-Path $env:LOCALAPPDATA 'olcf' }
$Prefix = [IO.Path]::GetFullPath($Prefix)
if (-not $DataDir -and (Test-Path -LiteralPath (Join-Path $Prefix 'install.json'))) {
    try { $DataDir = (Get-Content -LiteralPath (Join-Path $Prefix 'install.json') -Raw | ConvertFrom-Json).data_dir } catch { $DataDir = $null }
}
if (-not $DataDir) { $DataDir = Join-Path $env:USERPROFILE '.olcf' }
$DataDir = [IO.Path]::GetFullPath($DataDir)

# only delete what is recognisably ours
foreach ($p in @($Prefix, $DataDir)) {
    $root = [IO.Path]::GetPathRoot($p)
    if ($p.TrimEnd('\') -eq $root.TrimEnd('\') -or $p.TrimEnd('\') -eq $env:USERPROFILE.TrimEnd('\')) { Fail "refusing to treat '$p' as an OLCF directory" }
}
$isPrefix = (Test-Path -LiteralPath (Join-Path $Prefix 'install.json')) -and (Test-Path -LiteralPath (Join-Path $Prefix 'versions'))
$isData = (Test-Path -LiteralPath (Join-Path $DataDir 'config.toml')) -or (Test-Path -LiteralPath (Join-Path $DataDir 'olcf.db')) -or (Test-Path -LiteralPath (Join-Path $DataDir 'secrets'))

function Remove-Item-Logged([string]$Path) {
    if ($DryRun) { Say "    would remove $Path"; return }
    Remove-Item -LiteralPath $Path -Recurse -Force
    Say "    removed: $Path"
}

$dataSelected = $RemoveMetadata -or $RemoveSecrets -or $RemoveLogs -or $RemoveStorage -or $RemoveRuntimes -or $RemoveBackups -or $RemoveCliConfig
$mode = ''
if ($DryRun) { $mode = ' (dry run)' }
Step "OLCF uninstaller: prefix $Prefix, data $DataDir$mode"
if ($dataSelected -and -not $Yes -and -not $DryRun) {
    if ([Environment]::UserInteractive -and -not [Console]::IsInputRedirected) {
        $answer = Read-Host "This permanently deletes the selected data under $DataDir. Type 'yes' to continue"
        if ($answer -ne 'yes') { Fail 'aborted; nothing was deleted' }
    } else { Fail 'refusing to delete data in a non-interactive session without -Yes' }
}

$olcf = Join-Path $Prefix 'bin\olcf.cmd'
if (-not $KeepProgram -and (Test-Path -LiteralPath $olcf)) {
    if (Test-Path -LiteralPath (Join-Path $DataDir 'olcf.pid')) {
        Step 'stopping the running server'
        if ($DryRun) { Say '    would run: olcf stop' } else { & $olcf --data-dir $DataDir -q stop }
    }
    Step 'removing the logon task (if any)'
    if ($DryRun) { Say '    would run: olcf service uninstall' } else { & $olcf --data-dir $DataDir -q service uninstall }
}

if (-not $KeepProgram) {
    Step 'removing the program'
    if ($isPrefix) {
        $bin = Join-Path $Prefix 'bin'
        $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
        if ($userPath -and (($userPath -split ';') -contains $bin)) {
            if ($DryRun) { Say "    would remove $bin from your user PATH" }
            else {
                [Environment]::SetEnvironmentVariable('Path', ((($userPath -split ';') | Where-Object { $_ -and $_ -ne $bin }) -join ';'), 'User')
                Say "    removed: $bin from your user PATH"
            }
        }
        $current = Join-Path $Prefix 'current'
        if ((Test-Path -LiteralPath $current) -and -not $DryRun) { cmd /c rmdir "`"$current`"" | Out-Null }       # drop the junction first: never recurse through it
        Remove-Item-Logged $Prefix
    } elseif (Test-Path -LiteralPath $Prefix) {
        Write-Warning "$Prefix does not look like an OLCF installation (no install.json or versions); leaving it alone"
    } else { Say "    nothing to remove at $Prefix" }
}

$dataTargets = @()
if ($RemoveMetadata) { $dataTargets += @('olcf.db', 'olcf.db-wal', 'olcf.db-shm', 'olcf.db-journal', 'config.toml', 'olcf.pid', 'artifacts', 'jobs') }
if ($RemoveSecrets) { $dataTargets += @('secrets', 'pki') }
if ($RemoveLogs) { $dataTargets += @('logs') }
if ($RemoveStorage) { $dataTargets += @('storage') }
if ($RemoveRuntimes) { $dataTargets += @('runtimes', 'wheelhouse', 'workers', 'tmp') }
if ($RemoveBackups) { $dataTargets += @('backups') }
if ($dataTargets.Count -gt 0) {
    Step "removing selected data from $DataDir"
    if (-not $isData) { Write-Warning "$DataDir does not look like an OLCF data directory; skipping data removal" }
    else {
        foreach ($name in $dataTargets) {
            $path = Join-Path $DataDir $name
            if (Test-Path -LiteralPath $path) { Remove-Item-Logged $path }
        }
        if ($RemoveRuntimes -and $KeepProgram -and (Test-Path -LiteralPath (Join-Path $Prefix 'cache'))) { Remove-Item-Logged (Join-Path $Prefix 'cache') }
        if ($Purge -and -not $DryRun) {
            if (@(Get-ChildItem -LiteralPath $DataDir -Force).Count -eq 0) { Remove-Item -LiteralPath $DataDir -Force; Say "    removed: $DataDir" }
            else { Write-Warning "$DataDir still contains other files; left in place" }
        }
    }
}

if ($RemoveCliConfig) {
    Step 'removing saved CLI logins'
    $cfg = $env:OLCF_CONFIG_DIR
    if (-not $cfg) { $cfg = Join-Path $env:APPDATA 'olcf' }
    if (Test-Path -LiteralPath (Join-Path $cfg 'contexts.json')) { Remove-Item-Logged $cfg } else { Say "    nothing at $cfg" }
}

Say ''
if ($DryRun) { Say 'Dry run: nothing was changed.' }
Say 'Done.'
