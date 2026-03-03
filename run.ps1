param(
    [int]$Port = 8501,
    [switch]$NoBrowser
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$appFile = Join-Path $projectRoot "app.py"
$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"

if (-not (Test-Path $appFile)) {
    Write-Error "No se encontro app.py en: $projectRoot"
    exit 1
}

$pythonCmd = "python"
if (Test-Path $venvPython) {
    $pythonCmd = $venvPython
}

$headless = "false"
if ($NoBrowser) {
    $headless = "true"
}

& $pythonCmd -m streamlit run $appFile --server.headless $headless --server.port $Port
