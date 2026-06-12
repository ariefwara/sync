# sync — Sinkronisasi Folder P2P via LAN

Tool sinkronisasi folder *peer-to-peer* tanpa server. Cukup jalankan di dua komputer yang terhubung di LAN, folder akan otomatis sinkron.

## Cara Pakai

```bash
sync .                  # sync folder saat ini
sync /path/ke/folder    # sync folder tertentu
```

Nama komputer diambil otomatis dari OS (`hostname`). Tidak perlu konfigurasi.

## Cara Kerja

1. Setiap peer mengirim **UDP broadcast** (port 43210) setiap 5 detik
2. Peer lain yang mendengar akan membalas dan terhubung via **TCP** (port 43211)
3. Perubahan file (`fsnotify` + periodic scan) langsung disebarkan ke semua peer
4. File diidentifikasi dengan hash **SHA256** — hanya file yang berbeda yang ditransfer

## Instalasi

### Build dari source

```bash
git clone https://github.com/ariefwara/sync.git
cd sync
go build -o sync ./cmd/sync-lan
```

Atau tinggal copy binary yang sudah di-build:

```bash
cp build/sync ~/bin/sync
```

## Penggunaan di Dua Komputer

**Komputer A:**
```bash
sync ~/Documents/proyek
```

**Komputer B:**
```bash
sync ~/Documents/proyek
```

Setelah kedua peer jalan, coba buat file di salah satu komputer:

```bash
echo "halo" > ~/Documents/proyek/test.txt
```

File akan muncul di komputer lain dalam beberapa detik.

## Port yang Digunakan

| Port | Protocol | Fungsi |
|------|----------|--------|
| 43210 | UDP | Discovery (broadcast PING/PONG) |
| 43211 | TCP | Transfer file |

## Jika Port Sudah Dipakai

Jika ada instance lain yang sudah menggunakan port yang sama (misalnya sudah jalan di folder lain), instance kedua akan langsung berhenti dengan pesan:

```
sync sudah berjalan di folder ini (port 43211 sudah dipakai)
```

## Output

```
sync — mensinkronkan /Users/ariefwara/Documents/proyek
      device: macbook-pro
      menunggu peer di LAN...

  + peer bergabung
  ↑ laporan.docx
  ↓ foto.png
```

- `+` peer ditemukan
- `↑` file terkirim ke peer
- `↓` file diterima dari peer

## Batasan

- Hanya bekerja dalam **satu subnet LAN** (karena UDP broadcast tidak melewati router)
- Tidak ada enkripsi bawaan (gunakan VPN jika perlu keamanan di jaringan publik)
- Konflik file menggunakan strategi *last-writer-wins* (file dengan modifikasi terbaru yang menang)

## License

MIT
