param(
    [switch]$WithHealth
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $projectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) {
    $python = "python"
}

Write-Output "==> py_compile"
& $python -m py_compile app.py scanner_worker.py check_scanner_health.py
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Output "==> unit tests"
& $python -m unittest discover -s tests -p "test_*.py" -v
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($WithHealth) {
    Write-Output "==> health check (optional)"
    & $python check_scanner_health.py --health scanner_health.json --max-stale-sec 300
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

Write-Output "OK: safe update checks passed."

