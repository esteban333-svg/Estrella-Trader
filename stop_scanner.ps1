$procs = Get-CimInstance Win32_Process |
    Where-Object {
        $_.Name -match "python" -and
        $_.CommandLine -match "scanner_worker\.py"
    }

if (-not $procs) {
    Write-Output "No hay scanner en ejecucion."
    exit 0
}

foreach ($proc in $procs) {
    try {
        Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop
        Write-Output "Scanner detenido. PID=$($proc.ProcessId)"
    } catch {
        Write-Warning "No se pudo detener PID=$($proc.ProcessId): $($_.Exception.Message)"
    }
}
