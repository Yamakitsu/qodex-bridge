param(
    [Parameter()]
    [ValidatePattern('^\d+\.\d+\.\d+$')]
    [string]$Version = '0.1.0'
)

$ErrorActionPreference = 'Stop'
$projectRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$tempBase = [IO.Path]::GetFullPath([IO.Path]::GetTempPath())
$buildRoot = Join-Path $tempBase ('qodex_portable_' + [Guid]::NewGuid().ToString('N'))
$releaseRoot = Join-Path $projectRoot 'release'
$packageName = "QodexBridge-$Version-windows-x64"
$packageRoot = Join-Path $releaseRoot $packageName
$zipPath = Join-Path $releaseRoot ($packageName + '.zip')
$checksumPath = $zipPath + '.sha256'
$releasePrefix = [IO.Path]::GetFullPath($releaseRoot) + [IO.Path]::DirectorySeparatorChar
$resolvedPackageRoot = [IO.Path]::GetFullPath($packageRoot)
if (-not $resolvedPackageRoot.StartsWith($releasePrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "Package path escaped release directory: $resolvedPackageRoot"
}

try {
    New-Item -ItemType Directory -Path $buildRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $releaseRoot -Force | Out-Null

    $venvRoot = Join-Path $buildRoot 'venv'
    python -m venv $venvRoot
    if ($LASTEXITCODE -ne 0) { throw 'Unable to create build environment.' }
    $python = Join-Path $venvRoot 'Scripts\python.exe'
    & $python -m pip install --disable-pip-version-check --upgrade pip
    if ($LASTEXITCODE -ne 0) { throw 'Unable to update pip.' }
    & $python -m pip install --disable-pip-version-check 'pyinstaller>=6,<7' $projectRoot
    if ($LASTEXITCODE -ne 0) { throw 'Unable to install build dependencies.' }

    $distRoot = Join-Path $buildRoot 'dist'
    $workRoot = Join-Path $buildRoot 'work'
    $specRoot = Join-Path $buildRoot 'spec'
    & $python -m PyInstaller `
        --noconfirm `
        --clean `
        --onedir `
        --console `
        --name QodexBridge `
        --collect-data qq_codex_bridge `
        --paths (Join-Path $projectRoot 'src') `
        --distpath $distRoot `
        --workpath $workRoot `
        --specpath $specRoot `
        (Join-Path $projectRoot 'packaging\pyinstaller_entry.py')
    if ($LASTEXITCODE -ne 0) { throw 'PyInstaller build failed.' }

    if (Test-Path -LiteralPath $packageRoot) {
        Remove-Item -LiteralPath $packageRoot -Recurse -Force
    }
    New-Item -ItemType Directory -Path $packageRoot | Out-Null
    Copy-Item -Path (Join-Path $distRoot 'QodexBridge\*') -Destination $packageRoot -Recurse -Force
    Copy-Item -LiteralPath (Join-Path $projectRoot 'config.example.toml') -Destination $packageRoot
    Copy-Item -LiteralPath (Join-Path $projectRoot 'packaging\portable\启动 Qodex Bridge.bat') -Destination $packageRoot
    Copy-Item -LiteralPath (Join-Path $projectRoot 'packaging\portable\便携版说明.txt') -Destination $packageRoot

    if (Test-Path -LiteralPath $zipPath) {
        Remove-Item -LiteralPath $zipPath -Force
    }
    if (Test-Path -LiteralPath $checksumPath) {
        Remove-Item -LiteralPath $checksumPath -Force
    }
    Compress-Archive -LiteralPath $packageRoot -DestinationPath $zipPath -CompressionLevel Optimal

    $hash = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
    Set-Content -LiteralPath $checksumPath -Value ($hash + '  ' + (Split-Path -Leaf $zipPath)) -Encoding ascii
    Write-Host "Portable directory: $packageRoot"
    Write-Host "ZIP: $zipPath"
    Write-Host "Checksum: $checksumPath"
    Write-Host "SHA256: $hash"
}
finally {
    $resolvedBuildRoot = [IO.Path]::GetFullPath($buildRoot)
    if ($resolvedBuildRoot.StartsWith($tempBase, [StringComparison]::OrdinalIgnoreCase) -and (Test-Path -LiteralPath $resolvedBuildRoot)) {
        Remove-Item -LiteralPath $resolvedBuildRoot -Recurse -Force
    }
}
