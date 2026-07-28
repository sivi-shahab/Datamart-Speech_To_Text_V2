# api_insert_table

Service internal untuk **menggabungkan data history campaign telemarketing (TMS) dengan data rekaman panggilan (e-centrix)**, lalu menyimpannya sebagai satu tabel datar siap-konsumsi di PostgreSQL: `dashboard.recording_tms_api`.

Repo ini berisi dua komponen yang berjalan terpisah:

| Komponen | File | Bentuk | Fungsi |
|---|---|---|---|
| **Batch job (utama)** | `app/refresh_recording_tms_api.py` | CLI, dijadwalkan cron | Menarik history + antrian predictive dialer + rekaman, menjodohkannya, lalu `INSERT` ke `dashboard.recording_tms_api` |
| **API pembacaan** | `app/main.py` | FastAPI (HTTP) | Menyajikan hasil gabungan history + rekaman secara *on-the-fly* lewat endpoint `GET /history-recording-flat` |

**Untuk siapa:** tim Data Management / dashboard internal. Tabel hasil (`recording_tms_api`) dipakai sebagai sumber daftar file rekaman yang perlu diproses lebih lanjut (mis. speech-to-text), ditandai lewat kolom `status_data`.

> **Status saat ini:** kedua komponen berfungsi. Batch job berjalan normal (tabel tujuan terisi harian, ±186.000 baris, `load_date` mengikuti hari berjalan), dan endpoint API sudah diverifikasi mengembalikan data setelah perbaikan bug *case-sensitivity* identifier — lihat [Catatan & Known Issues](#catatan--known-issues).

---

## Tech stack

Diambil dari `requirements.txt` dan versi yang benar-benar terpasang di conda env `/data/api_insert_table`:

| Komponen | Paket | Versi terpasang |
|---|---|---|
| Bahasa | Python | 3.10.18 |
| Web framework | `fastapi` | 0.118.2 |
| ASGI server | `uvicorn[standard]` | 0.37.0 |
| ORM / SQL toolkit | `SQLAlchemy` | 2.0.43 (mode `future=True`) |
| Driver database | `psycopg2-binary` | 2.9.10 |
| Config management | `pydantic-settings` | 2.11.0 (di atas `pydantic` 2.12.0) |
| Loader `.env` | `python-dotenv` | 1.1.1 |
| (transitif) | `starlette` | 0.48.0 |
| Database | PostgreSQL | schema `dashboard` |

> `requirements.txt` **tidak berisi pin versi**. Versi pada tabel di atas adalah hasil `pip list` pada environment yang berjalan sekarang, bukan versi yang dijamin oleh file dependency.

Tidak ada ORM model, migration tool (Alembic), Docker, message broker, maupun cache di repo ini. Semua akses database dilakukan lewat **raw SQL** yang dieksekusi melalui `sqlalchemy.text()`.

---

## Prasyarat sistem

- **Python 3.10** (environment produksi memakai conda env dengan prefix `/data/api_insert_table`)
- **Akses jaringan ke PostgreSQL** internal pada port `5432`
- Kredensial database dengan hak:
  - `SELECT` pada `dashboard."manaf_HISTORY_TMS_PROSPECT_DETAIL_CAMPAIGN"`, `dashboard."MANAF_ecentrix_recording"`, `dashboard."manaf_acs_predictive_queue"`
  - `INSERT` pada `dashboard.recording_tms_api`
- Tabel tujuan `dashboard.recording_tms_api` **sudah harus ada** — repo ini tidak memuat DDL maupun migration. Skema aktualnya didokumentasikan di [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

---

## Instalasi & menjalankan secara lokal

### 1. Siapkan environment

```bash
# opsi A — conda (sesuai produksi)
conda create -p /path/ke/env python=3.10
conda activate /path/ke/env

# opsi B — venv
python3.10 -m venv .venv && source .venv/bin/activate
```

### 2. Install dependency

```bash
pip install -r requirements.txt
```

### 3. Konfigurasi environment variable

Buat file `app/.env` (lihat bagian [Environment variables](#environment-variables)):

```bash
DATABASE_URL=postgresql+psycopg2://<user>:<password>@<host>:5432/<database>
```

> Kedua komponen membaca `.env` dengan path **relatif terhadap current working directory**, bukan relatif terhadap lokasi file Python. Karena itu semua perintah di bawah dijalankan dari dalam direktori `app/`.

### 4a. Menjalankan batch job (komponen utama)

```bash
cd app
python refresh_recording_tms_api.py --source-api http://localhost:8888
```

Argumen CLI:

| Argumen | Wajib | Format | Keterangan |
|---|---|---|---|
| `--source-api` | ya | string bebas | Nilai yang ditulis ke kolom `source_api` sebagai penanda asal data. Nilai yang dipakai job ini di produksi: `http://localhost:8888` |
| `--date-from` | tidak | `YYYY-MM-DD` | Batas bawah (inklusif) filter tanggal pada tabel history |
| `--date-to` | tidak | `YYYY-MM-DD` | Batas atas (inklusif) filter tanggal pada tabel history |

Contoh dengan rentang tanggal:

```bash
python refresh_recording_tms_api.py \
  --source-api http://localhost:8888 \
  --date-from 2026-07-01 --date-to 2026-07-27
```

Job bersifat **idempotent**: `INSERT ... ON CONFLICT (tiket_id, file_path) DO NOTHING`, sehingga aman dijalankan ulang untuk rentang tanggal yang sama.

### 4b. Menjalankan API

```bash
cd app
uvicorn main:app --host 0.0.0.0 --port 8888
```

Dokumentasi interaktif otomatis dari FastAPI:

- Swagger UI — `http://localhost:8888/docs`
- ReDoc — `http://localhost:8888/redoc`
- OpenAPI JSON — `http://localhost:8888/openapi.json`

Detail endpoint ada di [`docs/API.md`](docs/API.md).

---

## Penjadwalan produksi

Batch job dijalankan lewat **cron, setiap hari pukul 20:20**, memakai conda env berbasis prefix:

```cron
20 20 * * * /bin/bash -lc 'cd /data/api_insert_table/app && \
  /home/sys-adm/miniconda3/bin/conda run -p /data/api_insert_table --no-capture-output \
  python refresh_recording_tms_api.py --source-api http://localhost:8888 \
  >> /home/sys-adm/logs/refresh_recording_tms_api_$(date +\%F).log 2>&1'
```

Dua hal yang wajib diperhatikan bila entri ini dipindah atau disalin: `cd` ke `app/` bersifat wajib agar `.env` terbaca, dan `conda run` memakai `-p` (*prefix*), bukan `-n` (nama env), karena environment berada di `/data/api_insert_table`.

---

## Menjalankan test

**Tidak ada test di repo ini** — tidak ada direktori `tests/`, file `test_*.py`, maupun konfigurasi pytest/tox/CI.

Verifikasi manual yang bisa dipakai sebagai gantinya:

```bash
# 1. Cek konfigurasi terbaca
cd app && python -c "from refresh_recording_tms_api import Settings; print(Settings().sqlalchemy_uri)"

# 2. Jalankan job untuk rentang 1 hari
python refresh_recording_tms_api.py --source-api manual-test --date-from 2026-07-27 --date-to 2026-07-27
```

```sql
-- 3. Verifikasi baris yang masuk
SELECT load_date, source_api, count(*)
FROM dashboard.recording_tms_api
GROUP BY 1, 2 ORDER BY 1 DESC LIMIT 10;
```

---

## Environment variables

Tidak ada `.env.example` di repo. Variabel di bawah adalah yang benar-benar dibaca oleh kode (class `Settings` pada `main.py` dan `refresh_recording_tms_api.py`):

| Variable | Wajib | Default | Dibaca oleh | Keterangan |
|---|---|---|---|---|
| `DATABASE_URL` | **ya** | — | `main.py`, `refresh_recording_tms_api.py` | URI SQLAlchemy lengkap. Format: `postgresql+psycopg2://<user>:<password>@<host>:<port>/<database>`. Aplikasi gagal start bila variabel ini tidak ada. |

Konstanta berikut didefinisikan sebagai field `Settings` di `main.py` sehingga **bisa** dioverride lewat environment variable, walau di produksi selalu memakai nilai default:

| Variable | Default | Keterangan |
|---|---|---|
| `SCHEMA_HISTORY` | `dashboard` | Schema tabel history |
| `TABLE_HISTORY` | `manaf_HISTORY_TMS_PROSPECT_DETAIL_CAMPAIGN` | Nama tabel history |
| `SCHEMA_RECORDING` | `dashboard` | Schema tabel rekaman |
| `TABLE_RECORDING` | `MANAF_ecentrix_recording` | Nama tabel rekaman |

Pada `refresh_recording_tms_api.py`, nama tabel **di-hardcode** sebagai konstanta modul (`TABLE_HISTORY`, `TABLE_RECORDING`, `TABLE_QUEQUE`) dan tidak dapat diubah lewat environment variable. Schema `dashboard` juga ditulis langsung di dalam string SQL.

> **Catatan keamanan:** `.env` dan `app/.env` sebelumnya ikut ter-*track* di git. Keduanya sudah dikeluarkan dari version control (`git rm --cached`, file tetap ada di disk) dan `.gitignore` kini memuat pola `.env`.
>
> **Yang masih harus dikerjakan:** kredensial tersebut **tetap tersimpan di history git** pada commit `6c5dba2` dan masih dapat dibaca siapa pun yang punya akses ke repo. Menghapus dari tracking tidak menghapusnya dari history. Karena itu password database **wajib dirotasi**; bila repo pernah di-*push* ke remote, history-nya juga perlu ditulis ulang (`git filter-repo`) atau repo dibuat ulang dari awal.

---

## Struktur folder

Repo ini berada di `/data/api_insert_table`, yang **sekaligus merupakan prefix conda environment**. Direktori standar conda (`bin/`, `lib/`, `include/`, `share/`, `conda-meta/`, `ssl/`, `etc/`, `man/`, `compiler_compat/`, `x86_64-conda-linux-gnu/`) ikut berada di direktori ini namun **bukan bagian dari kode aplikasi**.

Sebelumnya 10.892 file environment tersebut ter-*track* di git. Seluruhnya kini dikeluarkan dari version control (`git rm -r --cached`, file tetap ada di disk) dan sudah tercakup `.gitignore`, sehingga repo hanya melacak kode aplikasi dan dokumentasi. Environment-nya sendiri direproduksi lewat `requirements.txt`, bukan lewat git.

> File environment masih tersimpan di history commit `6c5dba2`, jadi ukuran objek `.git` tidak ikut mengecil kecuali history ditulis ulang.

Kode aplikasi yang relevan hanya sebagai berikut:

```
/data/api_insert_table
├── README.md                        # dokumen ini
├── requirements.txt                 # dependency aplikasi (tanpa pin versi)
├── docs/
│   ├── ARCHITECTURE.md              # arsitektur, alur data, keputusan desain
│   └── API.md                       # spesifikasi endpoint HTTP
└── app/                             # seluruh kode aplikasi
    ├── __init__.py                  # penanda package
    ├── .env                         # konfigurasi DATABASE_URL (tidak untuk di-commit)
    ├── main.py                      # FastAPI: endpoint baca GET /history-recording-flat
    └── refresh_recording_tms_api.py # batch job CLI: ETL ke dashboard.recording_tms_api
```

| Path | Tanggung jawab |
|---|---|
| `app/` | Satu-satunya direktori berisi kode aplikasi. Struktur *flat* — tidak ada pemisahan layer router/service/repository. |
| `app/main.py` | Definisi `FastAPI()`, `Settings`, engine SQLAlchemy, model response Pydantic, template SQL, dan satu endpoint. |
| `app/refresh_recording_tms_api.py` | Query ETL besar (`INSERT ... SELECT` dengan 5 CTE), pembangun klausa tanggal dinamis, dan entry point `argparse`. |
| `docs/` | Dokumentasi teknis. |

---

## Catatan & Known Issues

### Sudah diperbaiki

**A. Identifier *mixed-case* disisipkan ke SQL tanpa tanda kutip ganda.**

PostgreSQL melipat identifier tak-berkutip menjadi huruf kecil, sehingga relasi/kolom bernama huruf besar tidak ditemukan. Bug ini muncul di **kedua** komponen — empat titik, seluruhnya kini dikutip dengan benar:

| File | Lokasi | Sebelum | Sesudah |
|---|---|---|---|
| `main.py` | `SQL_HISTORY` | `{SCHEMA_HISTORY}.{TABLE_HISTORY}` | `{SCHEMA_HISTORY}."{TABLE_HISTORY}"` |
| `main.py` | `SQL_RECORDING_TOP2` | `{SCHEMA_RECORDING}.{TABLE_RECORDING}` | `{SCHEMA_RECORDING}."{TABLE_RECORDING}"` |
| `main.py` | `COALESCE_DATE`, `COALESCE_TS` | `h.mis_date` | `h."MIS_DATE"` |
| `refresh_recording_tms_api.py` | `build_date_clause()` | `h.mis_date` | `h."MIS_DATE"` |

Dampak sebelum perbaikan: endpoint API gagal total dengan `UndefinedTable`, dan **batch job gagal setiap kali dijalankan dengan `--date-from`/`--date-to`** dengan `UndefinedColumn`. Jalur tanpa argumen tanggal — yaitu yang dipakai cron harian — tidak terdampak, sehingga kegagalan ini hanya muncul saat backfill manual.

Audit terhadap seluruh identifier lain yang direferensikan kedua file (`id`, `agent_id`, `status`, `created_time`, `prospect_id`, `recording_customer_id`, `a_number`, `context`, `file_path`, `"JULIAN_MIS_DATE"`, `"handPhone1"`) menunjukkan semuanya sudah benar. Perlu dicatat, `j.mis_date` pada bagian `INSERT ... SELECT` **memang benar tanpa kutip** — itu merujuk alias keluaran CTE `hist` (`h."MIS_DATE" AS mis_date`), bukan kolom tabel asli.

> Bila mengoverride `TABLE_HISTORY`/`TABLE_RECORDING` lewat environment variable, tulis nama tabel **tanpa** tanda kutip — kutip ganda sudah ditambahkan oleh template SQL.

**B. Batch job selalu keluar dengan exit code 0 meski gagal.**

Blok `try/except` di `__main__` mencetak error ke `stderr` tetapi tidak memanggil `sys.exit(1)`, sehingga Python berakhir dengan status `0` dan cron menganggap job sukses. Kini ditambahkan `sys.exit(1)` pada blok tersebut.

Exit code setelah perbaikan:

| Kondisi | Exit code |
|---|---|
| Sukses | `0` |
| Gagal koneksi / kredensial / SQL error | `1` |
| `DATABASE_URL` tidak ada (`.env` tidak terbaca) | `1` |
| Argumen CLI salah atau kurang | `2` (ditangani `argparse`) |

Bug A pada `build_date_clause()` ditemukan justru **karena** perbaikan B — kegagalannya selama ini tertelan exit code 0.

### Masih terbuka

1. **Filter rekaman pada batch job selalu 7 hari terakhir.** CTE `rec` memakai `WHERE r.created_time >= CURRENT_DATE - INTERVAL '7 days'` yang bersifat tetap dan **tidak** ikut menyesuaikan `--date-from`/`--date-to`. Menjalankan ulang job untuk tanggal yang lebih lama dari 7 hari akan selesai normal (exit `0`) namun tidak menemukan rekaman apa pun.

2. **Tidak ada logging terstruktur maupun laporan jumlah baris.** Job hanya mencetak satu baris `INSERT done` tanpa menyebut berapa baris yang benar-benar tersimpan, sehingga hasil tiap eksekusi sulit diaudit dari log.

3. **Kredensial ter-*commit*.** Lihat catatan pada bagian [Environment variables](#environment-variables).

---

## Dokumentasi lanjutan

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — diagram komponen, alur data, penjelasan tiap modul, keputusan desain
- [`docs/API.md`](docs/API.md) — daftar endpoint, parameter, contoh request & response
