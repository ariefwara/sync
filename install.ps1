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
function Write-Warn($msg) { Write-Host "==> $msg" -ForegroundColor Yellow }
function Write-Err($msg) { Write-Host "!!> $msg" -ForegroundColor Red; exit 1 }

# ---- try downloading pre-built binary ----
$ErrorActionPreference = 'Stop'
$TmpDir = Join-Path $env:TEMP "sync-install-$([System.IO.Path]::GetRandomFileName())"
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null

try {
    Write-Info "Fetching latest release from GitHub ..."
    $ApiUrl = "https://api.github.com/repos/$Repo/releases/latest"
    
    try {
        $Release = Invoke-RestMethod -Uri $ApiUrl -ErrorAction Stop
        $Asset = $Release.assets | Where-Object { $_.name -like "*windows-amd64*" } | Select-Object -First 1
        
        if ($Asset) {
            $DownloadUrl = $Asset.browser_download_url
            Write-Info "Downloading $($Asset.name) ..."
            
            $ZipPath = Join-Path $TmpDir "sync.zip"
            Invoke-WebRequest -Uri $DownloadUrl -OutFile $ZipPath
            
            # The binary is just an exe, not a zip
            # Check if it's actually a zip or just an exe
            $Header = [System.IO.File]::ReadAllBytes($ZipPath)[0..1]
            if ($Header[0] -eq 0x50 -and $Header[1] -eq 0x4B) {
                # It's a zip file
                Expand-Archive -Path $ZipPath -DestinationPath $TmpDir -Force
                $ExePath = Join-Path $TmpDir "sync.exe"
            } else {
                # It's a raw exe
                $ExePath = $ZipPath
            }
            
            if (-not (Test-Path $ExePath)) {
                # Try to find the exe in the temp dir
                $ExePath = Get-ChildItem -Path $TmpDir -Recurse -Filter "sync*.exe" | Select-Object -First 1 -ExpandProperty FullName
                if (-not $ExePath) {
                    $ExePath = Join-Path $TmpDir "sync-windows-amd64.exe"
                }
            }
            
            if (Test-Path $ExePath) {
                if (-not (Test-Path $BinDir)) {
                    New-Item -ItemType Directory -Path $BinDir -Force | Out-Null
                }
                Copy-Item -Path $ExePath -Destination $BinPath -Force
                Write-Info "Installed to $BinPath"
                Write-Info "Make sure $BinDir is in your PATH"
                Write-Info "Run 'sync .' to start syncing"
                exit 0
            }
        }
    }
    catch {
        Write-Warn "No release found — building from source"
    }
}
finally {
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}

# ---- fallback: build from source ----
if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    Write-Err "Go is required. Install from https://go.dev/dl/ or push a version tag to create a pre-built release."
}

$TmpDir = Join-Path $env:TEMP "sync-install-$([System.IO.Path]::GetRandomFileName())"
New-Item -ItemType Directory -Path $TmpDir -Force | Out-Null
Push-Location $TmpDir
try {
    Write-Info "Cloning $Repo ..."
    git clone --depth=1 "https://github.com/$Repo.git" . 2>&1 | Out-Null
    if (-not (Test-Path ".\go.mod")) {
        Write-Err "Failed to clone repository"
    }

    Write-Info "Building sync ..."
    $Build = go build -o "$BinPath" .\cmd\sync-lan 2>&1
    if ($LASTEXITCODE -ne 0) {
        Write-Err "Build failed: $Build"
    }

    Write-Info "Installed to $BinPath (built from source)"
    Write-Info "Run 'sync .' to start syncing"
}
finally {
    Pop-Location
    Remove-Item -Recurse -Force $TmpDir -ErrorAction SilentlyContinue
}
