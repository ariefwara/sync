#!/usr/bin/env bash
set -euo pipefail

REPO="ariefwara/sync"
BIN="${SYNC_BIN:-/usr/local/bin/lansync}"
TMPDIR=""

cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

# ---- colors ----
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { printf "${GREEN}==>${NC} %s\n" "$*"; }
warn()  { printf "${YELLOW}==>${NC} %s\n" "$*"; }
err()   { printf "${RED}!!>${NC} %s\n" "$*" >&2; exit 1; }

# ---- detect platform ----
OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
ARCH="$(uname -m)"
case "$ARCH" in
  x86_64|amd64) ARCH="amd64" ;;
  aarch64|arm64) ARCH="arm64" ;;
  *) warn "Unsupported architecture: $ARCH — falling back to source build"; FALLBACK=1 ;;
esac
# Normalize OS names for GitHub release asset names
case "$OS" in
  linux)   RELEASE_OS="linux" ;;
  darwin)  RELEASE_OS="darwin" ;;
  *)       warn "Unsupported OS: $OS — falling back to source build"; FALLBACK=1 ;;
esac

# ---- try downloading pre-built binary ----
if [ -z "${FALLBACK:-}" ]; then
  TMPDIR="$(mktemp -d)"
  
  info "Fetching latest release from GitHub ..."
  
  # Get latest release download URL
  API_URL="https://api.github.com/repos/$REPO/releases/latest"
  ASSET="lansync-${RELEASE_OS}-${ARCH}"
  DOWNLOAD_URL=$(curl -sSL "$API_URL" | grep -oP '"browser_download_url": "\K[^"]*'"$ASSET"'[^"]*' | head -1 || true)
  
  if [ -n "$DOWNLOAD_URL" ]; then
    info "Downloading $ASSET ..."
    curl -sSL -o "$TMPDIR/lansync" "$DOWNLOAD_URL" || { warn "Download failed — falling back to source build"; FALLBACK=1; }
    
    if [ -z "${FALLBACK:-}" ]; then
      chmod +x "$TMPDIR/lansync"
      
      # Test it
      "$TMPDIR/lansync" --help >/dev/null 2>&1 || true
      
      if [ -w "$(dirname "$BIN")" ]; then
        cp "$TMPDIR/lansync" "$BIN"
      else
        info "Installing to $BIN (requires sudo) ..."
        sudo cp "$TMPDIR/lansync" "$BIN"
      fi
      
      echo ""
      info "Installed to $BIN"
      info "Run 'lansync .' to start syncing the current directory"
      exit 0
    fi
  else
    warn "No release found — building from source"
    FALLBACK=1
  fi
fi

# ---- fallback: build from source ----
if [ -n "${FALLBACK:-}" ]; then
  command -v go >/dev/null 2>&1 || err "Go is required. Install from https://go.dev/dl/ or push a version tag to create a pre-built release."

  TMPDIR="$(mktemp -d)"
  cd "$TMPDIR"

  info "Cloning $REPO ..."
  git clone --depth=1 "https://github.com/$REPO.git" . 2>/dev/null || err "Failed to clone repository"

  info "Building lansync ..."
  go build -o "$BIN" ./cmd/lansync 2>&1 || err "Build failed"
  chmod +x "$BIN"

  echo ""
  info "Installed to $BIN (built from source)"
  info "Run 'lansync .' to start syncing the current directory"
  exit 0
fi
