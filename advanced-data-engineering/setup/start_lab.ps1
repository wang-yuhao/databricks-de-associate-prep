# Start the local "Databricks workspace": JupyterLab on the course folders, with the adelab kernel.
#   powershell -ExecutionPolicy Bypass -File .\setup\start_lab.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

foreach ($var in "JAVA_HOME", "HADOOP_HOME") {
    $val = [Environment]::GetEnvironmentVariable($var, "User")
    if (-not $val) { $val = [Environment]::GetEnvironmentVariable($var, "Machine") }
    if ($val) { Set-Item "Env:$var" $val; $env:Path = "$val\bin;$env:Path" }
}

& "$root\.venv\Scripts\jupyter.exe" lab --notebook-dir "$root"
