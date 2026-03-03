param(
    [string]$TaskName = "EstrellaTraderScanner"
)

try {
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false -ErrorAction Stop
    Write-Output "Tarea eliminada: $TaskName"
} catch {
    Write-Warning "No se pudo eliminar la tarea '$TaskName': $($_.Exception.Message)"
}
