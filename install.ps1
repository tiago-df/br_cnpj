# install.ps1 - Install the BR CNPJ -> POI pipeline on Windows.
#
# Creates a virtual environment, installs dependencies, creates the
# required directory structure, and copies .env.example if no .env exists.
#
# Usage:
#   .\install.ps1
#   .\install.ps1 -InstallDir "C:\tools\cnpj-poi"

param(
    [string]$InstallDir = $PSScriptRoot
)

$ErrorActionPreference = "Stop"

Write-Host "============================================================"
Write-Host " BR CNPJ -> POI Pipeline - Installation"
Write-Host " Install directory: $InstallDir"
Write-Host "============================================================"

# -- Create directory structure ---------------------------------------------
$dirs = @(
    "data\input",
    "data\intermediate",
    "data\cache",
    "data\output",
    "logs"
)
foreach ($d in $dirs) {
    New-Item -ItemType Directory -Force -Path (Join-Path $InstallDir $d) | Out-Null
}
Write-Host "[1/4] Directory structure created."

# -- Copy project files (if installing to a different directory) ------------
if ($InstallDir -ne $PSScriptRoot) {
    Write-Host "[2/4] Copying project files to $InstallDir..."
    $excludes = @('.git', 'venv', 'data', 'logs', '__pycache__')
    Get-ChildItem -Path $PSScriptRoot | Where-Object {
        $excludes -notcontains $_.Name
    } | Copy-Item -Destination $InstallDir -Recurse -Force
} else {
    Write-Host "[2/4] Installing in-place - no copy needed."
}

# -- Virtual environment ----------------------------------------------------
$venv = Join-Path $InstallDir "venv"
if (-not (Test-Path $venv)) {
    Write-Host "[3/4] Creating virtual environment at $venv..."
    python -m venv $venv
} else {
    Write-Host "[3/4] Virtual environment already exists - skipping."
}

& "$venv\Scripts\pip.exe" install --upgrade pip --quiet
& "$venv\Scripts\pip.exe" install -r (Join-Path $InstallDir "requirements.txt") --quiet
Write-Host "      Dependencies installed."

# -- Environment file -------------------------------------------------------
$envFile    = Join-Path $InstallDir ".env"
$envExample = Join-Path $InstallDir ".env.example"
if (-not (Test-Path $envFile)) {
    Copy-Item $envExample $envFile
    Write-Host "[4/4] Created .env from .env.example - edit it if needed."
} else {
    Write-Host "[4/4] .env already exists - not overwritten."
}

Write-Host ""
Write-Host "============================================================"
Write-Host " Installation complete."
Write-Host ""
Write-Host " Activate the virtual environment:"
Write-Host "   $venv\Scripts\Activate.ps1"
Write-Host ""
Write-Host " Run the pipeline:"
Write-Host "   python -m app.main --dump-date 2026-05"
Write-Host "   python -m app.main --help"
Write-Host "============================================================"
