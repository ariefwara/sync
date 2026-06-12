#!/usr/bin/env bash
set -euo pipefail

REPO="ariefwara/sync"
BIN="${SYNC_BIN:-/usr/local/bin/sync}"
TMPDIR=""

cleanup() {
  rm -rf "$TMPDIR"
}
trap cleanup EXIT

# ---- colors ----
RED='\033[0;31m'; GREEN='\033[0;32m'; NC='\033[0m'
info()  { printf "${GREEN}==>${NC} %s\n" "$*"; }
err()   { printf "${RED}!!>${NC} %s\n" "$*" >&2; exit 1; }

# ---- check prerequisites ----
command -v go >/dev/null 2>&1 || err "Go is required but not installed. See https://go.dev/dl/"

# ---- detect go install path ----
GOBIN="$(go env GOBIN 2>/dev/null || true)"
if [ -z "$GOBIN" ]; then
  GOPATH="$(go env GOPATH 2>/dev/null || echo "$HOME/go")"
  GOBIN="$GOPATH/bin"
fi

# ---- install ----
info "Installing sync to $BIN ..."

# Create temp directory for building
TMPDIR="$(mktemp -d)"
cd "$TMPDIR"

info "Cloning $REPO ..."
git clone --depth=1 "https://github.com/$REPO.git" . 2>/dev/null || err "Failed to clone repository"

info "Building sync (LAN broadcast version) ..."
go build -o "$BIN" ./cmd/sync-lan 2>&1 || err "Build failed"

chmod +x "$BIN"

echo ""
info "Installed to $BIN"
info "Run 'sync .' to start syncing the current directory"
