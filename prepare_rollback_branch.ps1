param(
    [Parameter(Mandatory = $true)]
    [string]$Tag,
    [string]$BranchPrefix = "rollback/"
)

$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
    Write-Error "git no esta disponible."
    exit 1
}

git fetch --tags
if ($LASTEXITCODE -ne 0) {
    Write-Error "No se pudo actualizar tags."
    exit 1
}

$safeTag = ($Tag -replace "[^a-zA-Z0-9._/-]", "-")
$branch = "$BranchPrefix$safeTag"

git switch -C $branch $Tag
if ($LASTEXITCODE -ne 0) {
    Write-Error "No se pudo crear branch $branch desde tag $Tag."
    exit 1
}

git push -u origin $branch
if ($LASTEXITCODE -ne 0) {
    Write-Error "No se pudo subir branch de rollback al remoto."
    exit 1
}

Write-Output "Branch de rollback lista: $branch"
Write-Output "En Render usa Manual Deploy del branch: $branch"

