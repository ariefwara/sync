# sync — Windows installer
# Run:  powershell -ExecutionPolicy Bypass -c "irm https://raw.githubusercontent.com/ariefwara/sync/main/install.ps1 | iex"

$Repo = "ariefwara/sync"
$BinDir = $env:SYNC_BIN
if (-not $BinDir) {
    $GoBin = go env GOBIN 2>$null
    if ($GoBin) {
        $BinDir = $GoBin
    } else {
        $BinDir = "$env:USERPROFILE\go\bin"
    }
}
$BinPath = "$BinDir\sync.exe"

function Write-Info($msg) { Write-Host "==> $msg" -ForegroundColor Green }
function Write-Err($msg) { Write-Host "!!> $msg" -ForegroundColor Red; exit 1 }

# ---- check Go ----
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Err "Go is required but not installed. Download from https://go.dev/dl/"
}

Write-Info "Installing sync to $BinPath"

# ---- create temp directory ----
$TmpDir = Join-Path $env:TEMP "sync-install-$([System.IO.Path]::GetRandomFileName())"
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null
Push-Location $TmpDir
try {
    Write-Info "Cloning $Repo ..."
    git clone --depth=1 "https://github.com/$Repo.git" . 2>&1 | Out-Null
    if (-not (Test-Path ".\go.mod")) {
        Write-Err "Failed to clone repository"
    }

    Write-Info "Building sync (LAN broadcast version) ..."
    $Build = go build -o "$BinPath" .\cmd\sync-lan 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Err "Build failed: $Build"
    }

    Write-Info "Installed to $BinPath"
    Write-Info "Make sure $BinDir is in your PATH"
    Write-Info "Run 'sync .' to start syncing"
}
finally {
    Pop-Location
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}
