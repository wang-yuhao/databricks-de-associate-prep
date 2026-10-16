# Stop stray Spark JVMs and Python workers started from this project's .venv (for example after a
# kernel crash, or when a new session hangs at startup because an old one still holds resources).
#   powershell -ExecutionPolicy Bypass -File .\setup\stop_spark.ps1
$root = Split-Path -Parent $PSScriptRoot
$marker = Join-Path $root ".venv"
$procs = Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -and $_.CommandLine.Contains($marker) -and $_.Name -in @("java.exe", "python.exe", "cmd.exe")
}
foreach ($p in $procs) {
    Write-Host ("stopping {0,-10} pid {1}" -f $p.Name, $p.ProcessId)
    Stop-Process -Id $p.ProcessId -Force -ErrorAction SilentlyContinue
}
if (-not $procs) { Write-Host "no Spark processes from $marker are running" }
