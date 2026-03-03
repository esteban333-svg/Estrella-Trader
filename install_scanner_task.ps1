param(
    [string]$TaskName = "EstrellaTraderScanner"
)

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$startScript = Join-Path $projectRoot "start_scanner.ps1"

if (-not (Test-Path $startScript)) {
    Write-Error "No se encontro start_scanner.ps1 en: $projectRoot"
    exit 1
}

$action = New-ScheduledTaskAction `
    -Execute "powershell.exe" `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$startScript`""
$trigger = New-ScheduledTaskTrigger -AtLogOn
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 0)

try {
    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger $trigger `
        -Settings $settings `
        -User $env:USERNAME `
        -RunLevel Limited `
        -Description "Escaner Dorado de Estrella Trader (forex + cripto)." `
        -Force | Out-Null
    Write-Output "Tarea instalada: $TaskName"
} catch {
    Write-Error "No se pudo instalar la tarea: $($_.Exception.Message)"
    exit 1
}
