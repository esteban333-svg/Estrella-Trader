param(
    [string]$Prefix = "stable",
    [switch]$Push
)

$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
    Write-Error "git no esta disponible."
    exit 1
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$sha = (git rev-parse --short HEAD).Trim()
if (-not $sha) {
    Write-Error "No se pudo leer commit actual."
    exit 1
}

$tag = "$Prefix-$timestamp-$sha"
git tag -a $tag -m "Release $tag"
if ($LASTEXITCODE -ne 0) {
    Write-Error "No se pudo crear tag: $tag"
    exit 1
}

Write-Output "Tag creada: $tag"
if ($Push) {
    git push origin $tag
    if ($LASTEXITCODE -ne 0) {
        Write-Error "No se pudo subir tag al remoto."
        exit 1
    }
    Write-Output "Tag subida a origin: $tag"
}

