param(
    [Parameter(Mandatory = $true)]
    [string]$Version
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ProjectDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$DistDir = Join-Path $ProjectDir "dist"
$StageDir = Join-Path $ProjectDir ".windows-package"
$ClientBinary = Join-Path $ProjectDir "target\release\icedcomm-i2p.exe"
$ServerBinary = Join-Path $ProjectDir "SERVER\target\release\deaddrop-server.exe"
$ClientReadme = Join-Path $ProjectDir "README.md"
$ServerReadme = Join-Path $ProjectDir "SERVER\README.md"
$License = Join-Path $ProjectDir "LICENSE"
$Notice = Join-Path $ProjectDir "NOTICE"

$ClientPackageName = "icedcomm-i2p-v$Version-windows-x86_64"
$ServerPackageName = "deaddrop-server-v$Version-windows-x86_64"
$ClientPackageDir = Join-Path $StageDir $ClientPackageName
$ServerPackageDir = Join-Path $StageDir $ServerPackageName
$ClientArchive = Join-Path $DistDir "$ClientPackageName.zip"
$ServerArchive = Join-Path $DistDir "$ServerPackageName.zip"

$RequiredFiles = @(
    $ClientBinary,
    $ServerBinary,
    $ClientReadme,
    $ServerReadme,
    $License,
    $Notice
)

foreach ($Required in $RequiredFiles) {
    if (-not (Test-Path -LiteralPath $Required -PathType Leaf)) {
        throw "Required Windows packaging input is missing: $Required"
    }
}

Remove-Item -LiteralPath $StageDir -Recurse -Force -ErrorAction SilentlyContinue
Remove-Item -LiteralPath $DistDir -Recurse -Force -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Path $ClientPackageDir -Force | Out-Null
New-Item -ItemType Directory -Path $ServerPackageDir -Force | Out-Null
New-Item -ItemType Directory -Path $DistDir -Force | Out-Null

Copy-Item -LiteralPath $ClientBinary -Destination (Join-Path $ClientPackageDir "icedcomm-i2p.exe")
Copy-Item -LiteralPath $ClientReadme -Destination (Join-Path $ClientPackageDir "README.md")
Copy-Item -LiteralPath $License -Destination (Join-Path $ClientPackageDir "LICENSE")
Copy-Item -LiteralPath $Notice -Destination (Join-Path $ClientPackageDir "NOTICE")

Copy-Item -LiteralPath $ServerBinary -Destination (Join-Path $ServerPackageDir "deaddrop-server.exe")
Copy-Item -LiteralPath $ServerReadme -Destination (Join-Path $ServerPackageDir "README.md")
Copy-Item -LiteralPath $License -Destination (Join-Path $ServerPackageDir "LICENSE")
Copy-Item -LiteralPath $Notice -Destination (Join-Path $ServerPackageDir "NOTICE")

Compress-Archive -LiteralPath $ClientPackageDir -DestinationPath $ClientArchive -CompressionLevel Optimal
Compress-Archive -LiteralPath $ServerPackageDir -DestinationPath $ServerArchive -CompressionLevel Optimal

$ChecksumLines = foreach ($Archive in @($ClientArchive, $ServerArchive)) {
    $Hash = (Get-FileHash -LiteralPath $Archive -Algorithm SHA256).Hash.ToLowerInvariant()
    "$Hash  $([System.IO.Path]::GetFileName($Archive))"
}

$ChecksumText = ($ChecksumLines -join "`n") + "`n"
$Utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
[System.IO.File]::WriteAllText(
    (Join-Path $DistDir "SHA256SUMS"),
    $ChecksumText,
    $Utf8WithoutBom
)
