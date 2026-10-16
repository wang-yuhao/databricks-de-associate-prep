# One-time setup of the local practice environment (Windows).
#   powershell -ExecutionPolicy Bypass -File .\setup\setup_env.ps1
# Creates .venv (Python 3.11), installs the pinned packages and the `adelab` package, registers the
# "Python (adelab)" Jupyter kernel, downloads the Delta Lake jars, and runs the environment check.
param([switch]$Recreate)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

# Pick up JAVA_HOME / HADOOP_HOME even if this terminal was opened before they were set.
foreach ($var in "JAVA_HOME", "HADOOP_HOME") {
    $val = [Environment]::GetEnvironmentVariable($var, "User")
    if (-not $val) { $val = [Environment]::GetEnvironmentVariable($var, "Machine") }
    if ($val) { Set-Item "Env:$var" $val; $env:Path = "$val\bin;$env:Path" }
}

if ($Recreate -and (Test-Path .venv)) { Remove-Item -Recurse -Force .venv }
# $ErrorActionPreference does not stop on a failing native command: check each exit code.
function Invoke-Step([string]$what, [scriptblock]$cmd) {
    & $cmd
    if ($LASTEXITCODE -ne 0) { throw "Setup failed while trying to $what (exit code $LASTEXITCODE)." }
}

if (-not (Test-Path .venv\Scripts\python.exe)) {
    Write-Host "Creating .venv with Python 3.11 ..."
    Invoke-Step "create .venv (is Python 3.11 installed? check: py -3.11 --version)" { py -3.11 -m venv .venv }
}
$py = Join-Path $root ".venv\Scripts\python.exe"

Invoke-Step "upgrade pip" { & $py -m pip install --upgrade pip --quiet }
Invoke-Step "install requirements.txt" { & $py -m pip install -r requirements.txt -e . --quiet }
Invoke-Step "register the Jupyter kernel" { & $py -m ipykernel install --user --name adelab --display-name "Python (adelab)" }
Invoke-Step "download the Delta Lake jars" { & $py -c "from adelab.spark import resolve_jars; print(len(resolve_jars()), 'Delta Lake jars ready')" }
& $py -m adelab.doctor
