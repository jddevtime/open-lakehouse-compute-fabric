# SPDX-License-Identifier: Apache-2.0
# Copyright (c) 2026 Justin Dodrill
#
# OLCF installer for Windows 10/11 (Windows PowerShell 5.1 or PowerShell 7).  Per-user: no administrator rights, nothing outside your profile.
#
#   (no -Wheel? this script then needs a package index; to install from the GitHub source archive use uv: see deploy/README.md section 1)
#   powershell -ExecutionPolicy Bypass -File scripts\install.ps1 -Wheel dist\olcf-0.1.0-py3-none-any.whl                   (from a local build)
#   Get-Help .\scripts\install.ps1 -Detailed
#
# Like install.sh this needs no compiler and no pre-installed Python or Java.  It bootstraps uv (SHA-256 verified), lets uv provide a
# managed Python, builds one virtual environment per OLCF version from wheels only, and optionally installs the Spark runtime including a
# checksum-verified Eclipse Temurin JDK.  Layout (same as `olcf upgrade` expects, see src/olcf/cli/upgrade.py):
#   <Prefix>\install.json  <Prefix>\bin\{uv.exe,olcf.cmd}  <Prefix>\python\  <Prefix>\cache\  <Prefix>\versions\<v>\venv\  <Prefix>\current (junction)
#
# NOTE: this script could not be executed in the environment it was written in (no PowerShell there); it was reviewed by eye and its
# structure is checked by tests/tooling/test_installer.py.  Please report problems with the output of  .\install.ps1 -Verbose.

<#
.SYNOPSIS
Installs the Open Lakehouse Compute Fabric (OLCF) for the current Windows user.
.PARAMETER Version
OLCF version from the package index (default: latest).
.PARAMETER Wheel
Install this olcf-*.whl file (or https URL) instead of using the package index.
.PARAMETER Prefix
Program directory (default %LOCALAPPDATA%\olcf).
.PARAMETER DataDir
Data directory used by `olcf init` (default %USERPROFILE%\.olcf).
.PARAMETER Extras
Optional extras, for example postgres,s3,azure or all.
.PARAMETER NoRuntime
Do not install the Spark runtime (SQL on DuckDB, lakehouses and the UI work without it).
.PARAMETER NoJdk
Never download a JDK.
.PARAMETER WithService
Start OLCF at logon (Task Scheduler, no administrator rights needed).
.PARAMETER OfflineBundle
Directory with uv.exe, wheels\ and optionally python\ and runtime\ for an air-gapped install.
.PARAMETER Docker
Print the Docker instructions and exit.
.PARAMETER DryRun
Show what would happen without changing anything.
#>
[CmdletBinding()]
param(
    [string]$Version = $env:OLCF_VERSION,
    [string]$Prefix = $env:OLCF_PREFIX,
    [string]$DataDir = $env:OLCF_DATA_DIR,
    [string]$Wheel = $env:OLCF_WHEEL,
    [string]$IndexUrl = $env:OLCF_INDEX_URL,
    [string]$FindLinks = $env:OLCF_FIND_LINKS,
    [string]$Extras = $env:OLCF_EXTRAS,
    [string]$Python = '3.13',
    [string]$UvVersion = '0.11.32',
    [string]$UvBin = $env:OLCF_UV_BIN,
    [string]$UvBaseUrl = 'https://github.com/astral-sh/uv/releases/download',
    [string]$Runtime = '',
    [string]$OfflineBundle = '',
    [int]$Port = 0,
    [int]$Keep = 2,
    [switch]$NoRuntime,
    [switch]$NoJdk,
    [switch]$NoInit,
    [switch]$WithService,
    [switch]$NoModifyPath,
    [switch]$Reinstall,
    [switch]$Docker,
    [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'     # the progress bar makes Invoke-WebRequest very slow on Windows PowerShell 5.1
try { [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12 -bor [Net.ServicePointManager]::SecurityProtocol } catch { }

function Say([string]$Message) { [Console]::Error.WriteLine($Message) }
function Step([string]$Message) { Say "==> $Message" }
function Ok([string]$Message) { Say " ok $Message" }
function Warn([string]$Message) { Write-Warning $Message }
function Fail([string]$Message) { throw "olcf installer: $Message" }

if ($Docker) {
    Say @'
Docker route (no installation on the host at all):

  docker run -d --name olcf -p 127.0.0.1:8470:8470 -v olcf-data:/data ghcr.io/jddevtime/olcf:latest
  docker logs olcf                 # shows the first administrator password once
  start http://127.0.0.1:8470

or with the compose file from this repository:

  copy deploy\.env.example deploy\.env
  docker compose -f deploy\docker-compose.yml --profile allinone up -d

The image name is a placeholder until the project is published; build it yourself with
  docker build -f deploy\Dockerfile -t olcf:local .
See deploy\README.md.
'@
    return
}

# --------------------------------------------------------------------------------------------- platform and paths
if ($env:OS -ne 'Windows_NT') { Fail 'this script is for Windows. Use scripts/install.sh on macOS and Linux.' }
$arch = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE }
switch ($arch) {
    'AMD64' { $UvTriple = 'x86_64-pc-windows-msvc' }
    'ARM64' { $UvTriple = 'aarch64-pc-windows-msvc' }
    default { Fail "unsupported CPU architecture '$arch' (supported: AMD64, ARM64)" }
}
if (-not $Prefix) { $Prefix = Join-Path $env:LOCALAPPDATA 'olcf' }
if (-not $DataDir) { $DataDir = Join-Path $env:USERPROFILE '.olcf' }
$Prefix = [IO.Path]::GetFullPath($Prefix)
$DataDir = [IO.Path]::GetFullPath($DataDir)
if ($Python -notmatch '^\d+\.\d+$') { Fail '-Python must look like 3.13' }
if ($Wheel -and $Version) { Fail 'use either -Wheel or -Version, not both' }
if ($OfflineBundle) {
    if (-not (Test-Path -LiteralPath $OfflineBundle -PathType Container)) { Fail "-OfflineBundle '$OfflineBundle' is not a directory" }
    $OfflineBundle = (Resolve-Path -LiteralPath $OfflineBundle).Path
}
$note = ''
if ($DryRun) { $note = ' (dry run: nothing is changed)' }
Step "OLCF installer: Windows/$arch -> prefix $Prefix, data $DataDir$note"

# --------------------------------------------------------------------------------------------- helpers
function Get-FileSha256([string]$Path) { (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant() }

function Download([string]$Url, [string]$Dest) {
    if ($OfflineBundle) { Fail "internal error: network access requested in offline mode ($Url)" }
    if ($Url -notmatch '^https://') { Fail "refusing to download from a non-HTTPS URL: $Url" }
    Invoke-WebRequest -UseBasicParsing -Uri $Url -OutFile $Dest
}

function Invoke-Native([string]$What, [string]$Exe, [string[]]$Arguments) {
    # run a native program, fail with a clear message on a non-zero exit code
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) { Fail "$What failed (exit code $LASTEXITCODE)" }
}

function Test-Uv([string]$Path) {
    if (-not $Path -or -not (Test-Path -LiteralPath $Path -PathType Leaf)) { return $false }
    try {
        $out = & $Path --version 2>$null
        if ($LASTEXITCODE -ne 0 -or $out -notmatch '^uv (\d+)\.(\d+)') { return $false }
        return ([int]$Matches[1] -gt 0) -or ([int]$Matches[2] -ge 5)
    } catch { return $false }
}

# uv keeps all of its state under the prefix and may only use Pythons it manages itself.
$env:UV_PYTHON_INSTALL_DIR = Join-Path $Prefix 'python'
$env:UV_CACHE_DIR = Join-Path $Prefix 'cache'
$env:UV_PYTHON_PREFERENCE = 'only-managed'
$env:UV_NO_PROGRESS = '1'
$env:UV_NO_CONFIG = '1'

$tmp = $null
if (-not $DryRun) {
    foreach ($d in @('bin', 'versions', 'python', 'cache')) { New-Item -ItemType Directory -Force -Path (Join-Path $Prefix $d) | Out-Null }
    $tmp = Join-Path ([IO.Path]::GetTempPath()) ("olcf-install-" + [Guid]::NewGuid().ToString('N').Substring(0, 8))
    New-Item -ItemType Directory -Force -Path $tmp | Out-Null
}
$partial = $null
try {
    # ----------------------------------------------------------------------------------------- uv
    $uv = $null
    if ($UvBin) {
        if (-not (Test-Uv $UvBin)) { Fail "-UvBin '$UvBin' is not a working uv (>= 0.5)" }
        $uv = $UvBin
    } elseif (Test-Uv (Join-Path $Prefix 'bin\uv.exe')) {
        $uv = Join-Path $Prefix 'bin\uv.exe'
    } elseif ($OfflineBundle) {
        foreach ($cand in @((Join-Path $OfflineBundle 'uv.exe'), (Join-Path $OfflineBundle 'bin\uv.exe'))) {
            if (Test-Uv $cand) { if (-not $DryRun) { Copy-Item -LiteralPath $cand -Destination (Join-Path $Prefix 'bin\uv.exe') -Force }; $uv = Join-Path $Prefix 'bin\uv.exe'; break }
        }
        if (-not $uv) { Fail "the offline bundle has no uv.exe for $UvTriple" }
    } else {
        $onPath = Get-Command uv.exe -ErrorAction SilentlyContinue
        if ($onPath -and (Test-Uv $onPath.Source)) {
            $uv = $onPath.Source
            Say "    using the uv found on PATH: $uv"
        } else {
            $name = "uv-$UvTriple.zip"
            $url = "$UvBaseUrl/$UvVersion/$name"
            Step "downloading uv $UvVersion ($UvTriple)"
            if ($DryRun) {
                Say "    would download $url and verify its SHA-256"
                $uv = Join-Path $Prefix 'bin\uv.exe'
            } else {
                Download $url (Join-Path $tmp $name)
                Download "$url.sha256" (Join-Path $tmp "$name.sha256")
                $want = ((Get-Content -LiteralPath (Join-Path $tmp "$name.sha256") -TotalCount 1) -split '\s+')[0].ToLowerInvariant()
                $got = Get-FileSha256 (Join-Path $tmp $name)
                if (-not $want -or $want -ne $got) { Fail "SHA-256 mismatch for $name (expected '$want', got '$got'); refusing to run it" }
                Expand-Archive -LiteralPath (Join-Path $tmp $name) -DestinationPath (Join-Path $tmp 'uv') -Force
                $found = Get-ChildItem -LiteralPath (Join-Path $tmp 'uv') -Recurse -Filter uv.exe | Select-Object -First 1
                if (-not $found) { Fail "$name did not contain uv.exe" }
                Copy-Item -LiteralPath $found.FullName -Destination (Join-Path $Prefix 'bin\uv.exe') -Force
                $uv = Join-Path $Prefix 'bin\uv.exe'
                if (-not (Test-Uv $uv)) { Fail 'the downloaded uv does not run on this machine' }
                Ok "uv $UvVersion verified and installed to $uv"
            }
        }
    }

    # ----------------------------------------------------------------------------------------- what to install
    $extraSpec = ''
    if ($Extras) { $extraSpec = "[$Extras]" }
    $targetVersion = $null
    $targetSpec = $null
    if ($Wheel) {
        if ($Wheel -match '^https://') {
            $base = ($Wheel -split '/')[-1]
            $targetSpec = "olcf$extraSpec @ $Wheel"
        } else {
            if (-not (Test-Path -LiteralPath $Wheel -PathType Leaf)) { Fail "wheel not found: $Wheel" }
            $full = (Resolve-Path -LiteralPath $Wheel).Path
            $base = Split-Path -Leaf $full
            $targetSpec = "olcf$extraSpec @ " + ([Uri]$full).AbsoluteUri
        }
        if ($base -notmatch '^olcf-([^-]+)-.*\.whl$') { Fail "'$base' is not an olcf wheel (expected olcf-<version>-py3-none-any.whl)" }
        $targetVersion = $Matches[1]
    } elseif ($OfflineBundle) {
        $wheels = @(Get-ChildItem -LiteralPath (Join-Path $OfflineBundle 'wheels') -Filter 'olcf-*.whl' -ErrorAction SilentlyContinue)
        if ($wheels.Count -ne 1) { Fail 'the offline bundle must contain exactly one wheels\olcf-*.whl' }
        $null = $wheels[0].Name -match '^olcf-([^-]+)-'
        $targetVersion = $Matches[1]
        $targetSpec = "olcf$extraSpec==$targetVersion"
    } elseif ($Version) {
        $targetVersion = $Version
        $targetSpec = "olcf$extraSpec==$Version"
    } else {
        Step 'looking up the newest olcf release'
        if ($DryRun) { $targetVersion = 'latest'; $targetSpec = "olcf$extraSpec" }
        else {
            $reqFile = Join-Path $tmp 'req.in'
            Set-Content -LiteralPath $reqFile -Value "olcf$extraSpec" -Encoding ascii
            $compileArgs = @('pip', 'compile', $reqFile, '--python-version', $Python, '--only-binary', ':all:', '--no-header', '--no-annotate')
            if ($IndexUrl) { $compileArgs += @('--index-url', $IndexUrl) }
            $lines = & $uv @compileArgs 2>$null
            $hit = @($lines | Where-Object { $_ -match '^olcf==' } | Select-Object -First 1)
            if ($hit.Count -eq 0) { Fail 'could not find olcf in the package index. If it is not published yet, install from a build: -Wheel dist\olcf-<version>-py3-none-any.whl' }
            $targetVersion = (($hit[0] -replace '^olcf==', '') -split '[\s;]')[0]
            $targetSpec = "olcf$extraSpec==$targetVersion"
        }
    }

    # ----------------------------------------------------------------------------------------- build the version
    $dest = Join-Path $Prefix "versions\$targetVersion"
    $venvPython = Join-Path $dest 'venv\Scripts\python.exe'
    $needBuild = $true
    if ((Test-Path -LiteralPath $venvPython) -and -not $Reinstall) {
        & $venvPython -c 'import olcf' 2>$null
        if ($LASTEXITCODE -eq 0) { $needBuild = $false; Ok "olcf $targetVersion is already installed (use -Reinstall to rebuild it)" }
    }
    if ($needBuild) {
        Step "installing olcf $targetVersion"
        if ($DryRun) {
            Say "    would: uv python install $Python; uv venv $dest\venv; uv pip install --only-binary :all: '$targetSpec'"
        } else {
            $partial = $dest
            if (Test-Path -LiteralPath $dest) { Remove-Item -LiteralPath $dest -Recurse -Force }
            New-Item -ItemType Directory -Force -Path $dest | Out-Null
            if ($OfflineBundle -and (Test-Path -LiteralPath (Join-Path $OfflineBundle 'python'))) {
                $env:UV_PYTHON_INSTALL_MIRROR = ([Uri](Join-Path $OfflineBundle 'python')).AbsoluteUri
            }
            Say "    Python $Python (uv-managed, under $Prefix\python)"
            $pyArgs = @('python', 'install', $Python)
            if ($OfflineBundle) { $pyArgs += '--offline' }
            Invoke-Native "installing Python $Python with uv (behind a proxy? set HTTPS_PROXY; offline? use -OfflineBundle)" $uv $pyArgs
            Invoke-Native 'creating the virtual environment' $uv @('venv', '--python', $Python, '--python-preference', 'only-managed', (Join-Path $dest 'venv'))
            $pipArgs = @('pip', 'install', '--python', $venvPython, '--only-binary', ':all:', '--no-config')
            if ($IndexUrl) { $pipArgs += @('--index-url', $IndexUrl) }
            if ($FindLinks) { $pipArgs += @('--find-links', $FindLinks) }
            if ($OfflineBundle) { $pipArgs += @('--offline', '--no-index', '--find-links', (Join-Path $OfflineBundle 'wheels')) }
            Say '    installing wheels (nothing is compiled; --only-binary :all:)'
            Invoke-Native "installing '$targetSpec' (a package without a Windows wheel cannot be installed without compilers)" $uv ($pipArgs + @($targetSpec))
            $env:OLCF_DATA_DIR = $DataDir
            $reported = (& $venvPython -m olcf.cli.main --json version 2>$null | Out-String | ConvertFrom-Json).olcf
            if ($reported -ne $targetVersion) { Fail "smoke test failed: the new environment reports version '$reported', expected '$targetVersion'" }
            $partial = $null
            Ok "olcf $targetVersion installed in $dest\venv"
        }
    }

    # ----------------------------------------------------------------------------------------- switch (backup + migrate when upgrading)
    $current = Join-Path $Prefix 'current'
    $previous = ''
    if (Test-Path -LiteralPath $current) {
        $previous = Split-Path -Leaf ((Get-Item -LiteralPath $current).Target | Select-Object -First 1)
    }
    if ($DryRun) {
        Say "    would switch $current -> versions\$targetVersion (from '$previous')"
    } else {
        $env:OLCF_DATA_DIR = $DataDir
        $oldPython = Join-Path $Prefix "versions\$previous\venv\Scripts\python.exe"
        if ($previous -and $previous -ne $targetVersion -and (Test-Path -LiteralPath $oldPython)) {
            Step "upgrading $previous -> $targetVersion"
            if (Test-Path -LiteralPath (Join-Path $DataDir 'olcf.pid')) {
                Say '    stopping the running server'
                & $oldPython -m olcf.cli.main -q stop --grace 45
                if ($LASTEXITCODE -ne 0) { Warn 'could not stop the running server cleanly; continuing' }
            }
            if (Test-Path -LiteralPath (Join-Path $DataDir 'olcf.db')) {
                $stamp = Get-Date -Format 'yyyyMMddTHHmmss'
                $bk = Join-Path $DataDir "backups\pre-upgrade-$previous-to-$targetVersion-$stamp.olcfbak"
                New-Item -ItemType Directory -Force -Path (Join-Path $DataDir 'backups') | Out-Null
                Say "    backing up the database -> $bk"
                & $oldPython -m olcf.cli.main -q backup create -o $bk
                if ($LASTEXITCODE -ne 0) { Fail 'the pre-upgrade backup failed; the old version stays active. Nothing was changed.' }
                Say "    migrating the database with $targetVersion"
                & $venvPython -m olcf.cli.main -q db migrate
                if ($LASTEXITCODE -ne 0) { Fail "the database migration failed; the old version ($previous) stays active (backup: $bk)" }
            }
        }
        # junction swap (no administrator rights needed, unlike symbolic links)
        if (Test-Path -LiteralPath $current) { cmd /c rmdir "`"$current`"" | Out-Null }
        New-Item -ItemType Junction -Path $current -Target (Join-Path $Prefix "versions\$targetVersion") | Out-Null
        $shim = Join-Path $Prefix 'bin\olcf.cmd'
        Set-Content -LiteralPath $shim -Encoding ascii -Value "@echo off`r`n`"%~dp0..\current\venv\Scripts\olcf.exe`" %*"
        Set-Content -LiteralPath (Join-Path $Prefix 'bin\olcf-worker.cmd') -Encoding ascii -Value "@echo off`r`n`"%~dp0..\current\venv\Scripts\olcf-worker.exe`" %*"

        $method = 'index'
        if ($Wheel) { $method = 'wheel' }
        if ($OfflineBundle) { $method = 'offline-bundle' }
        $helper = Join-Path $tmp 'write_install_json.py'
        Set-Content -LiteralPath $helper -Encoding ascii -Value @'
import json, os, sys, time
prefix, version, previous, pyver, data_dir, method, uvbin = sys.argv[1:8]
path = os.path.join(prefix, "install.json")
try:
    info = json.load(open(path, encoding="utf-8"))
except (OSError, ValueError):
    info = {}
info.update(schema=1, prefix=prefix, version=version, python=pyver, data_dir=data_dir, method=method, uv=uvbin,
            installed_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
if previous not in ("", "-") and previous != version:  # "-" stands for none: Windows PowerShell 5.1 silently drops an empty argument
    info["previous"] = previous
tmp = path + ".tmp"
with open(tmp, "w", encoding="utf-8") as fh:
    json.dump(info, fh, indent=2, sort_keys=True)
    fh.write("\n")
os.replace(tmp, path)
'@
        $previousArg = if ($previous) { $previous } else { '-' }  # an empty string would vanish from the argument list
        Invoke-Native 'writing install.json' $venvPython @('-I', $helper, $Prefix, $targetVersion, $previousArg, $Python, $DataDir, $method, $uv)
        # keep the newest $Keep versions (never the current or the previous one)
        if ($Keep -gt 0) {
            $dirs = @(Get-ChildItem -LiteralPath (Join-Path $Prefix 'versions') -Directory | Sort-Object LastWriteTime -Descending)
            for ($i = $Keep; $i -lt $dirs.Count; $i++) {
                if ($dirs[$i].Name -ne $targetVersion -and $dirs[$i].Name -ne $previous) { Say "    removing old version $($dirs[$i].Name)"; Remove-Item -LiteralPath $dirs[$i].FullName -Recurse -Force }
            }
        }
        Ok "current -> versions\$targetVersion"
    }

    # ----------------------------------------------------------------------------------------- PATH (user scope)
    $bin = Join-Path $Prefix 'bin'
    if (-not $NoModifyPath) {
        if ($DryRun) { Say "    would add $bin to your user PATH" }
        else {
            $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
            $entries = @()
            if ($userPath) { $entries = $userPath -split ';' | Where-Object { $_ } }
            if ($entries -notcontains $bin) {
                [Environment]::SetEnvironmentVariable('Path', (($entries + $bin) -join ';'), 'User')
                Ok "added $bin to your user PATH (open a new terminal)"
            }
            if (($env:Path -split ';') -notcontains $bin) { $env:Path = "$bin;$env:Path" }
        }
    }

    $olcf = Join-Path $bin 'olcf.cmd'
    # `olcf init` runs BEFORE the runtime install: it writes config.toml with environment = "development", which makes this installation trust the development key that signs
    # the bundled Spark runtime manifests; without a config.toml the data directory counts as production and `olcf runtime install` refuses them.
    if (-not $NoInit) {
        if ($DryRun) { Step "initialising $DataDir"; Say '    would run: olcf init' }
        else {
            $initArgs = @('--data-dir', $DataDir, 'init')
            if ($Port -gt 0) { $initArgs += @('--port', "$Port") }
            & $olcf @initArgs
            if ($LASTEXITCODE -ne 0) { Fail 'olcf init failed' }
        }
    }

    $runtimeNote = ''
    if (-not $NoRuntime) {
        Step 'installing the Spark runtime (several minutes the first time: Python wheels, PySpark, JARs and - if no Java 17/21 exists - a verified Temurin JDK)'
        if ($DryRun) { Say '    would run: olcf runtime install' }
        else {
            $rtArgs = @('--data-dir', $DataDir, 'runtime', 'install')
            if ($Runtime) { $rtArgs += $Runtime }
            if ($NoJdk) { $rtArgs += '--no-jdk' } else { $rtArgs += '--with-jdk' }
            if ($IndexUrl) { $rtArgs += @('--index-url', $IndexUrl) }
            if ($FindLinks) { $rtArgs += @('--find-links', $FindLinks) }
            if ($OfflineBundle) {
                $bundle = Get-ChildItem -LiteralPath (Join-Path $OfflineBundle 'runtime') -Filter '*.olcfrt' -ErrorAction SilentlyContinue | Select-Object -First 1
                if ($bundle) { $rtArgs += @('--bundle', $bundle.FullName) }
            }
            & $olcf @rtArgs
            if ($LASTEXITCODE -ne 0) {
                $runtimeNote = 'the Spark runtime could not be installed (see the messages above). OLCF works without it for SQL/lakehouses/UI; retry with: olcf runtime install --with-jdk (if the message says the signing key is not trusted: run olcf init first, or set OLCF_RUNTIME_TRUST_DIR to your key directory)'
                Warn $runtimeNote
            } else { Ok 'runtime installed' }
        }
    }

    if ($WithService) {
        Step 'installing the logon task'
        if ($DryRun) { Say '    would run: olcf service install' }
        else {
            & $olcf --data-dir $DataDir service install
            if ($LASTEXITCODE -ne 0) { Warn "could not install the service; run 'olcf service install' later to see why" }
        }
    }

    Say ''
    Say "OLCF $targetVersion is installed."
    Say '  start it:       olcf serve            (or: olcf quickstart)'
    Say '  check it:       olcf doctor'
    Say '  upgrade later:  olcf upgrade --version X.Y.Z     roll back: olcf upgrade --rollback'
    Say '  remove it:      scripts\uninstall.ps1'
    if ($runtimeNote) { Say "  note: $runtimeNote" }
    if (-not $NoModifyPath) { Say '  Open a new terminal so that olcf is on your PATH.' }
}
finally {
    if ($partial -and (Test-Path -LiteralPath $partial)) { Remove-Item -LiteralPath $partial -Recurse -Force -ErrorAction SilentlyContinue }
    if ($tmp -and (Test-Path -LiteralPath $tmp)) { Remove-Item -LiteralPath $tmp -Recurse -Force -ErrorAction SilentlyContinue }
}
