# API Reference — api_insert_table

Spesifikasi diambil langsung dari `app/main.py`. Aplikasi FastAPI didefinisikan dengan judul:

> `History -> Recording (flat, tiket_id with _a/_b) [PostgreSQL @ dashboard]`

**Base URL (produksi/lokal):** `http://localhost:8888`

Tidak ada autentikasi, middleware, CORS, prefix router, maupun versioning yang dikonfigurasi — endpoint terpasang langsung di root.

Endpoint sudah diverifikasi berfungsi (`HTTP 200`) setelah perbaikan bug *case-sensitivity* identifier; riwayatnya tercatat di [README — Catatan & Known Issues](../README.md#catatan--known-issues).

---

## Ringkasan endpoint

| Method | Path | Ringkasan |
|---|---|---|
| `GET` | `/history-recording-flat` | Gabungan datar history campaign + rekaman panggilan |
| `GET` | `/docs` | Swagger UI (otomatis dari FastAPI) |
| `GET` | `/redoc` | ReDoc (otomatis dari FastAPI) |
| `GET` | `/openapi.json` | Skema OpenAPI (otomatis dari FastAPI) |

---

## `GET /history-recording-flat`

Mengembalikan satu baris datar per rekaman: menggabungkan kolom dari tabel history dengan kolom dari tabel rekaman, tanpa nesting.

Hanya baris history dengan `status = 4` yang diproses — nilai ini **di-hardcode** di dalam query dan tidak dapat diubah lewat parameter.

### Query parameters

| Parameter | Tipe | Wajib | Default | Batas | Keterangan |
|---|---|---|---|---|---|
| `date_from` | `date` (`YYYY-MM-DD`) | tidak | `null` | — | Filter tanggal (inklusif) batas bawah pada `created_date`, yaitu `COALESCE(created_time::date, to_date(mis_date,'YYYYMMDD'))` |
| `date_to` | `date` (`YYYY-MM-DD`) | tidak | `null` | — | Filter tanggal (inklusif) batas atas pada `created_date` |
| `limit` | `integer` | tidak | `500` | `1`–`10000` | Jumlah maksimum baris **history** yang diambil. Bukan jumlah baris response — satu baris history dapat menghasilkan 2 baris output. |
| `batch_size` | `integer` | tidak | `800` | `50`–`2000` | Ukuran batch daftar `prospect_id` saat mengambil data rekaman. Parameter tuning internal, tidak memengaruhi isi hasil. |

Bila `date_from` dan `date_to` tidak diberikan, tidak ada filter tanggal yang diterapkan.

### Response `200 OK`

```jsonc
{
  "count": 0,      // jumlah elemen dalam items
  "items": []      // array FlatRow
}
```

Objek `FlatRow` — **seluruh field bersifat opsional dan dapat bernilai `null`**:

| Field | Tipe | Asal | Keterangan |
|---|---|---|---|
| `tiket_id` | `string` | history `id` | ID history, dengan suffix `_a`/`_b` bila ada 2 rekaman (lihat [aturan di bawah](#aturan-pembentukan-tiket_id)) |
| `agent_id` | `string` | history | Sudah di-*trim* spasi |
| `status` | `integer` | history | Selalu bernilai `4` karena difilter di query |
| `created_time` | `string` | history | Format `YYYY-MM-DD HH:MM:SS.mmm` (string, bukan tipe datetime) |
| `prospect_id` | `string` | history | Sudah di-*trim* spasi; dipakai sebagai kunci pencocokan ke rekaman |
| `a_number` | `string` | rekaman | Nomor telepon; `null` bila tidak ada rekaman |
| `context` | `string` | rekaman | `null` bila tidak ada rekaman |
| `file_path` | `string` | rekaman | Path file rekaman; `null` bila tidak ada rekaman |

### Aturan pembentukan `tiket_id`

Rekaman dijodohkan ke history berdasarkan pasangan `(prospect_id, created_date)`, dan diambil maksimal **2 rekaman terbaru** per pasangan tersebut.

| Kondisi | `tiket_id` | Baris output |
|---|---|---|
| Ada 2 rekaman pada tanggal yang sama | `<id>_a` untuk yang terbaru, `<id>_b` untuk berikutnya | 2 baris |
| Hanya 1 rekaman | `<id>` — tanpa suffix | 1 baris |
| Tidak ada rekaman | `<id>` — tanpa suffix, kolom rekaman `null` | 1 baris |

Karena itu `count` pada response dapat lebih besar daripada jumlah baris history yang diambil.

### Contoh request

```bash
# tanpa filter (default: 500 baris history terbaru)
curl "http://localhost:8888/history-recording-flat"

# dengan rentang tanggal dan limit
curl "http://localhost:8888/history-recording-flat?date_from=2026-07-01&date_to=2026-07-27&limit=1000"

# dengan batch size khusus
curl "http://localhost:8888/history-recording-flat?date_from=2026-07-27&limit=200&batch_size=500"
```

### Contoh response

Bentuk payload sesuai model `FlatResponse`/`FlatRow`. Contoh berikut mengilustrasikan ketiga kasus `tiket_id` sekaligus. Strukturnya diambil dari response sungguhan, namun **`prospect_id` dan `a_number` telah disamarkan** karena memuat data pelanggan:

```json
{
  "count": 4,
  "items": [
    {
      "tiket_id": "270355fjw1_a",
      "agent_id": "ade801",
      "status": 4,
      "created_time": "2026-07-27 17:59:26.000",
      "prospect_id": "<hash-32-karakter>",
      "a_number": "08xxxxxxxxx",
      "context": "ntb",
      "file_path": "/home/ecentrix/recording/2026/07/27/20260727175844-201112-3a22fd5675.gsm"
    },
    {
      "tiket_id": "270355fjw1_b",
      "agent_id": "ade801",
      "status": 4,
      "created_time": "2026-07-27 17:59:26.000",
      "prospect_id": "<hash-32-karakter>",
      "a_number": "08xxxxxxxxx",
      "context": "ntb",
      "file_path": "/home/ecentrix/recording/2026/07/27/20260727101202-201112-9f11ab2c04.gsm"
    },
    {
      "tiket_id": "270555Zpqy",
      "agent_id": "maulida801",
      "status": 4,
      "created_time": "2026-07-27 17:56:55.000",
      "prospect_id": "<hash-32-karakter>",
      "a_number": "08xxxxxxxxx",
      "context": "ntb",
      "file_path": "/home/ecentrix/recording/2026/07/27/20260727175612-201485-37d3526186.gsm"
    },
    {
      "tiket_id": "270523ROWU",
      "agent_id": "nisi801",
      "status": 4,
      "created_time": "2026-07-27 17:43:23.000",
      "prospect_id": "<hash-32-karakter>",
      "a_number": null,
      "context": null,
      "file_path": null
    }
  ]
}
```

Response saat tidak ada baris history yang cocok:

```json
{ "count": 0, "items": [] }
```

### Response error

Tidak ada handler error kustom di `main.py`. Perilaku yang berlaku:

| Status | Kondisi | Bentuk body |
|---|---|---|
| `422 Unprocessable Entity` | Parameter tidak valid — mis. `limit=0`, `limit=20000`, `batch_size=10`, atau format tanggal salah | Body validasi standar FastAPI |
| `500 Internal Server Error` | Error database (koneksi putus, tabel/kolom tidak ditemukan, timeout) | Exception dipropagasi tanpa ditangkap; detail muncul di log server |

Contoh body `422` untuk `limit=0`:

```json
{
  "detail": [
    {
      "type": "greater_than_equal",
      "loc": ["query", "limit"],
      "msg": "Input should be greater than or equal to 1",
      "input": "0",
      "ctx": { "ge": 1 }
    }
  ]
}
```

---

## Antarmuka non-HTTP: batch job

`app/refresh_recording_tms_api.py` **tidak memiliki endpoint HTTP** — komponen ini adalah CLI. Argumen dan cara pemakaiannya didokumentasikan di [README — Menjalankan batch job](../README.md#4a-menjalankan-batch-job-komponen-utama).

Perlu diperhatikan bahwa argumen `--source-api` hanya berupa **label teks** yang disimpan ke kolom `source_api`; job tidak pernah melakukan panggilan HTTP ke alamat tersebut, meskipun nilainya di produksi berbentuk URL (`http://localhost:8888`).
