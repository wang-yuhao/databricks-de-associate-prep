# Launches the local "Databricks lab" workspace in JupyterLab.
#   Usage (from the project folder):  powershell -ExecutionPolicy Bypass -File .\start_lab.ps1
$ErrorActionPreference = "Stop"
$root = $PSScriptRoot

# Pick up JAVA_HOME / HADOOP_HOME even if this terminal was opened before they were set.
foreach ($var in "JAVA_HOME", "HADOOP_HOME") {
    $val = [Environment]::GetEnvironmentVariable($var, "User")
    if ($val) {
        Set-Item "Env:$var" $val
        $env:Path = "$val\bin;$env:Path"
    }
}

& "$root\.venv\Scripts\jupyter.exe" lab --notebook-dir "$root\notebooks"
