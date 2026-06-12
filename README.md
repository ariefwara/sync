# lansync — P2P Folder Sync over LAN

Zero-config peer-to-peer folder synchronization for local networks. No server, no setup, no cloud.

```bash
# On machine A
lansync ./project

# On machine B
lansync ./project
```

Files are synced in real-time via UDP discovery + TCP transfer.

## Install

### macOS / Linux (one-liner)

```bash
curl -sSL https://raw.githubusercontent.com/ariefwara/sync/main/install.sh | bash
```

Installs to `/usr/local/bin/lansync`.

### Windows (PowerShell)

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://raw.githubusercontent.com/ariefwara/sync/main/install.ps1 | iex"
```

Installs to `%USERPROFILE%\go\bin\lansync.exe`.

### Build from source (any OS)

```bash
git clone https://github.com/ariefwara/sync.git
cd sync
go build -o /usr/local/bin/lansync ./cmd/lansync
```

Requires Go 1.21+.

## Usage

```bash
lansync .                  # sync current directory
lansync ~/Documents/proj   # sync a specific directory
```

The device name is taken from your OS hostname automatically. No flags, no config file.

### Two-machine example

**Machine A (MacBook):**
```
$ lansync ~/projects/notes
lansync — syncing /Users/alice/projects/notes
      device: macbook-pro
      waiting for peers on LAN...
```

**Machine B (Desktop):**
```
$ lansync ~/projects/notes
lansync — syncing /home/bob/projects/notes
      device: desktop-pc
      waiting for peers on LAN...

  + peer joined           ← discovered automatically
```

Now create a file on either machine:
```bash
echo "meeting notes" > ~/projects/notes/todo.md
```

It appears on the other machine in seconds:
```
  ↓ todo.md
```

## How it works

1. Every 5 seconds, each peer broadcasts a **UDP PING** on port **43210**
2. Other peers respond with a **PONG** containing their TCP address
3. File changes are detected via `fsnotify` (real-time) + periodic full scan
4. Changed files are identified by **SHA256** hash — only differences are transferred
5. Metadata is broadcast first; file content is pulled on demand via TCP

| Port | Protocol | Purpose |
|------|----------|---------|
| 43210 | UDP | Peer discovery (broadcast) |
| 43211 | TCP | File transfer |

## Output reference

```
  + peer joined     a new peer was discovered on the LAN
  ↑ report.docx     file sent to peer
  ↓ photo.png       file received from peer
```

## Port conflict

If another instance is already running on the same ports, the second one exits immediately:

```
lansync is already running (port 43211 is in use)
```

## Limitations

- **LAN only** — UDP broadcast does not cross routers. Use a VPN (Tailscale, ZeroTier) if you need sync over the internet.
- **No encryption** — data is sent in plain TCP. Use a VPN on untrusted networks.
- **Conflict resolution** — *last-writer-wins* (the file with the most recent modification time wins).
- **Not a backup** — deletes are propagated. If you delete a file on one machine, it is deleted everywhere.

## License

MIT
