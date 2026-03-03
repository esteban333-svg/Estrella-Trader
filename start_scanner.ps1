param(
    [switch]$Once,
    [switch]$Debug
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$workerScript = Join-Path $projectRoot "scanner_worker.py"
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $workerScript)) {
    Write-Error "No se encontro scanner_worker.py en: $projectRoot"
    exit 1
}

$pythonCmd = "python"
if (Test-Path $venvPython) {
    $pythonCmd = $venvPython
}

$args = @($workerScript)
if ($Once) {
    $args += "--once"
}
if ($Debug) {
    $args += "--debug"
}

if (-not $Once) {
    $existing = Get-CimInstance Win32_Process |
        Where-Object {
            $_.Name -match "python" -and
            $_.CommandLine -match "scanner_worker\.py"
        }
    if ($existing) {
        Write-Output "Scanner ya esta corriendo."
        exit 0
    }
}

if ($Once) {
    & $pythonCmd @args
} else {
    Start-Process -FilePath $pythonCmd -ArgumentList $args -WorkingDirectory $projectRoot -WindowStyle Hidden
    Write-Output "Scanner iniciado en segundo plano."
}
