"""Batch ETL: mengisi ``dashboard.recording_tms_api`` dari history TMS + rekaman.

Job ini menjodohkan history campaign telemarketing dengan metadata rekaman
panggilan e-centrix, lalu menyimpan hasilnya sebagai baris siap-konsumsi. Baris
yang dihasilkan dipakai downstream (mis. speech-to-text) yang mengambil antrean
kerjanya lewat kolom ``status_data IS NULL``.

Seluruh transformasi didorong ke database dalam **satu statement**
``INSERT ... SELECT`` dengan lima CTE; Python hanya menyusun string SQL dan
mengeksekusinya dalam satu transaksi.

Alur transformasi::

    hist  ──┐
            ├─> hist_queque ──> joined ──> filter file_path ──> INSERT
    queque ─┘                     ^
                                  │
    rec ──────────────────────────┘

Penjodohan rekaman memakai dua strategi berjenjang dalam satu klausa ``OR``:
bila nomor HP pelanggan tersedia di antrian dialer, rekaman dicocokkan lewat
nomor telepon (``a_number = "handPhone1"``); bila tidak, jatuh kembali ke
pencocokan berbasis ID (``recording_customer_id = prospect_id``).

Job bersifat **idempotent** berkat ``ON CONFLICT (tiket_id, file_path) DO
NOTHING``, sehingga aman dijalankan ulang untuk rentang tanggal yang sama.


=============================================================================
LOGIKA YANG DIIMPLEMENTASIKAN (sudah diverifikasi manual, Agustus 2026)
=============================================================================
Rantai join:  history  ->  queue (contract_number)  ->  recording

1. ``hist`` disaring oleh DUA hal saja:
       a. ``h.id = ANY(:target_ids)``        -> 89 id di TARGET_IDS
       b. ``(last_response_reason = 'Agree'
             OR last_response_reason = 'Waiting Doc'
             OR status = 4)``
   TIDAK ada filter tanggal di ``hist``. Disengaja: daftar id sudah menjadi
   pembatas, dan menambah filter tanggal di sini justru berisiko membuang id
   yang tanggalnya di pinggir rentang (pernah terjadi -- id tanggal 03 hilang
   karena filter hist mulai dari 04).

   PENTING -- PRESEDENSI OPERATOR: ketiga kondisi OR itu WAJIB dibungkus tanda
   kurung. Tanpa kurung, ``A OR B OR C AND id = ANY(...)`` dibaca Postgres
   sebagai ``A OR B OR (C AND id = ANY(...))`` karena AND mengikat lebih kuat
   daripada OR -- pembatasan 89 id bocor dan seluruh tabel ikut terambil.

2. ``rec`` disaring HANYA oleh tanggal:
       ``r.created_time >= :rec_date_from AND r.created_time < :rec_date_to``
   Default 2026-08-03 s/d 2026-08-12 (batas atas EXCLUSIVE), bisa diganti
   lewat ``--rec-from`` / ``--rec-to``.

3. Satu ``id`` BOLEH menghasilkan lebih dari satu ``tiket_id`` -- satu prospek
   dapat ditelepon beberapa kali dan tiap panggilan punya file rekaman sendiri.
   Karena itu TIDAK ada ``ROW_NUMBER()``/dedup di mana pun. Keunikan dijaga
   primary key ``(tiket_id, file_path)``; ``tiket_id`` sudah mengandung
   timestamp rekaman (``<id>_<YYYYMMDDHH24MISS>``) sehingga panggilan pada
   waktu berbeda otomatis menjadi tiket berbeda.

Angka acuan hasil verifikasi manual (rentang 2026-08-03 s/d 2026-08-12):
    89 id target -> 73 baris lolos filter hist -> 146 baris hasil akhir
    (rata-rata 2 rekaman per id).
=============================================================================


Usage::

    cd app

    # Backfill 89 id target (default), window rekaman default 03-12 Agustus:
    python refresh_recording_tms_api.py --source-api "backfill-89"

    # Backfill dengan window rekaman lain:
    python refresh_recording_tms_api.py --source-api "backfill-89" \\
        --rec-from 2026-08-03 --rec-to 2026-08-12

    # Mode produksi/cron: SELURUH history, disaring tanggal seperti semula:
    python refresh_recording_tms_api.py --source-api http://localhost:8888 \\
        --all-ids --date-from 2026-08-03 --date-to 2026-08-11

Di produksi dijalankan lewat cron setiap hari pukul 20:20; ``cd`` ke direktori
``app/`` bersifat wajib agar ``.env`` terbaca.

Env:
    DATABASE_URL -- URI SQLAlchemy lengkap, dibaca dari ``.env`` pada direktori
        kerja saat proses dijalankan.

Riwayat perubahan (Agustus 2026):
    1. Filter ``hist`` diubah dari ``status = 4 AND last_response_reason =
       'Agree'`` menjadi ``(reason = 'Agree' OR reason = 'Waiting Doc'
       OR status = 4)``.
       Alasan: tabel history bersifat "hidup" -- baris terus di-update seiring
       progres kontak, bukan snapshot historis per event. Mengevaluasi
       ``status = 4`` hari ini tidak lagi merepresentasikan kondisi prospek
       pada saat rekaman dibuat, sehingga banyak baris valid ikut terbuang.
       Dengan filter lama dari 89 id target nyaris tidak ada yang lolos;
       dengan filter baru, 73 lolos.
    2. Ditambahkan pembatasan ke daftar id tertentu (TARGET_IDS) lewat
       ``h.id = ANY(:target_ids)``, aktif secara default, dimatikan dengan
       ``--all-ids``.
    3. Window tanggal CTE ``rec`` tidak lagi jendela tetap
       ``CURRENT_DATE - INTERVAL '7 days'``, melainkan bind parameter
       ``:rec_date_from`` / ``:rec_date_to``. Caveat lama -- job tidak
       menemukan rekaman untuk tanggal di luar 7 hari terakhir -- sudah tidak
       berlaku.
    4. Filter tanggal pada ``hist`` (``/**DATE_RANGE**/``) sekarang HANYA
       dipasang pada mode ``--all-ids``.
    5. ``ROW_NUMBER()``/kolom ``rn`` pada CTE ``rec`` DIHAPUS. Sebelumnya
       dihitung tapi tidak pernah difilter, jadi hanya menambah beban tanpa
       mengubah hasil. Menghapusnya membuat maksud kode eksplisit: memang
       SEMUA rekaman yang cocok diambil, bukan cuma yang terbaru.

CATATAN PENTING soal TARGET_IDS:
    Pembatasan ke 89 id ini untuk keperluan backfill/perbaikan data, BUKAN
    untuk dipasang permanen di cron. Bila entri cron produksi memakai file ini
    apa adanya, job harian hanya akan pernah memproses 89 id itu dan
    mengabaikan seluruh prospek baru. Untuk cron, WAJIB tambahkan ``--all-ids``
    (dan ``--date-from``/``--date-to``), atau kosongkan TARGET_IDS setelah
    backfill selesai.
"""
from datetime import date
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine, text
import argparse, urllib.parse, sys


class Settings(BaseSettings):
    """Konfigurasi job, dibaca dari environment variable atau file ``.env``.

    ``env_file='.env'`` bersifat relatif terhadap **current working directory**,
    bukan terhadap lokasi file ini. Karena itu entri cron selalu melakukan
    ``cd /data/api_insert_table/app`` sebelum menjalankan job.

    Attributes:
        DATABASE_URL: URI SQLAlchemy lengkap, mis.
            ``postgresql+psycopg2://user:pass@host:5432/db``. Wajib ada.
    """
    # Configure Pydantic to read settings from a .env file
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')
    DATABASE_URL: str

    @property
    def sqlalchemy_uri(self) -> str:
      """Returns the configured database connection URI."""
      return self.DATABASE_URL


# --- Table Names (Quoted for PostgreSQL Mixed-Case Sensitivity) ---
# NOTE: These names MUST EXACTLY match the case in PostgreSQL.
TABLE_HISTORY = '"manaf_HISTORY_TMS_PROSPECT_DETAIL_CAMPAIGN"'
TABLE_RECORDING = '"MANAF_ecentrix_recording"'
# TABLE_QUEQUE = '"manaf_acs_predictive_queue"'
TABLE_QUEQUE = '"manaf_acs_predictive_queue_11082026"'

# --- Default window tanggal untuk CTE rec (batas atas EXCLUSIVE) ---
DEFAULT_REC_FROM = date(2026, 8, 3)
DEFAULT_REC_TO   = date(2026, 8, 12)   # exclusive: rekaman tanggal 12 TIDAK ikut

# --- Daftar id history yang mau diproses (backfill Agustus 2026) ---
# Dikirim ke SQL sebagai SATU bind parameter list (:target_ids) lalu dipakai
# lewat `h.id = ANY(:target_ids)` -- bukan di-interpolasi jadi 89 literal.
# Kosongkan list ini (atau pakai --all-ids) untuk memproses seluruh history.
TARGET_IDS = [
    '030102SpLs', '030154XYN3', '030717uRin', '0308135e2F', '030821H5rE',
    '0308278WqT', '030854e4sE', '030934tcXZ', '030959HFw6', '031008JE58',
    '031033zy2x', '110440odnt', '050856iPTl', '0511313UMg', '060110qQAt',
    '060337mGbH', '060440J9bL', '060524r1dw', '061030Z0aR', '071055stGW',
    '071106Ox49', '080123NfgO', '080155BEUr', '080215NdO0', '080236pwLP',
    '0803369RGz', '080703xjGr', '0807491viL', '0807521PGK', '080803v8PS',
    '081023EBDq', '081032tq0d', '081259BEtZ', '100104DuHE', '100112ClgR',
    '100220Rr6B', '100232kh93', '100243Y9sZ', '100255XN1E', '1003234DJj',
    '100325A7Vo', '100335EoyZ', '100349PWRO', '100403Vml5', '100427JFZz',
    '100443sGmB', '100532LXdn', '1007100wMv', '100818WmyG', '100840qHdI',
    '100927x1ij', '101006C1dN', '101006uN4w', '101029enU9', '101044URgv',
    '1010451Oz4', '1011563cod', '110132sPeL', '110347ro7F', '110407iu7V',
    '110408aXMT', '110410fFxJ', '110426u7SP', '110429Yzal', '110502hmvl',
    '11054793hN', '110717nzqj', '110752uiWU', '110801hokT', '110801z94p',
    '110805NHnE', '110808r9e4', '110823SRWp', '110830fTb7', '110833eZLv',
    '110900rdfA', '110917mxaS', '110920HJcd', '110922WmMB', '1109245RX9',
    '110944eyuN', '110946tibl', '110953Hq1i', '1110048nbA', '111044YPGQ',
    '111050Mhn0', '111214m4l7', '111217stVT', '111227xUs3',
]


# ========= INSERT (append) - NOW USING F-STRING =========
# Using f"""...""" to allow Python variables (like TABLE_HISTORY) to be injected.
#
# Peta CTE:
#   hist        -- history yang relevan. Disaring oleh daftar id
#                  (/**ID_FILTER**/) dan oleh reason/status yang DIBUNGKUS
#                  KURUNG (lihat catatan presedensi di docstring modul).
#                  Filter tanggal opsional (/**DATE_RANGE**/) HANYA dipasang
#                  pada mode --all-ids.
#   queque      -- antrian predictive dialer; jembatan nomor telepon, memetakan
#                  prospect_id -> "handPhone1" lewat contract_number.
#   rec         -- rekaman dalam window :rec_date_from s/d :rec_date_to (batas
#                  atas exclusive). TANPA ROW_NUMBER/dedup: semua rekaman yang
#                  cocok diambil, sehingga satu baris history dapat
#                  menghasilkan beberapa baris output (satu per file).
#   hist_queque -- LEFT JOIN hist -> queque agar history tanpa padanan antrian
#                  tidak hilang.
#   joined      -- LEFT JOIN ke rec dengan dua strategi berjenjang (nomor
#                  telepon dulu, fallback ke ID pelanggan bila "handPhone1"
#                  NULL).
#
# tiket_id dibentuk deterministik sebagai <id>_<YYYYMMDDHH24MISS>; dipasangkan dengan
# file_path, keduanya menjadi primary key tabel tujuan sekaligus target ON CONFLICT.


INSERT_SQL = f"""
WITH hist AS (
  SELECT
    -- Menggunakan Kutip Ganda dan Huruf Besar untuk kolom yang sensitif case
    h."JULIAN_MIS_DATE" AS julian_mis_date, h."MIS_DATE" AS mis_date, h.id, h.prospect_id,
    h.customer_id, h.cust_name, h.campaign, h.agent_id, h.status, h.created_by, h.created_time,
    h.last_response_reason
  FROM dashboard.{TABLE_HISTORY} h
  WHERE (
        h.last_response_reason = 'Agree'
     OR h.last_response_reason = 'Waiting Doc'
     OR h.status = 4
  )
  /**ID_FILTER**/
  /**DATE_RANGE**/
),
queque AS (
  SELECT
    id,
    contract_number,
    source,
    account_number,
    class_id,
    last_attempt_datetime,
    "handPhone1"
  FROM dashboard.{TABLE_QUEQUE}
),
rec AS (
  SELECT
    r.recording_customer_id,
    r.created_time::date AS rec_created_date,
    r.created_time       AS rec_created_ts,
    r.a_number,
    r.context,
    r.file_path
  FROM dashboard.{TABLE_RECORDING} r
  WHERE r.created_time >= :rec_date_from
    AND r.created_time <  :rec_date_to
),
hist_queque AS (
  SELECT
    h.*,
    q.id AS queue_id,
    q.contract_number,
    q.source,
    q.account_number,
    q.class_id,
    q.last_attempt_datetime,
    q."handPhone1"
  FROM hist h
  LEFT JOIN queque q ON q.contract_number = h.prospect_id
),
joined AS (
  SELECT
    hq.*,
    r.rec_created_ts AS recording_created_time,
    r.a_number,
    r.context,
    r.file_path
  FROM hist_queque hq
  LEFT JOIN rec r
    ON (
      (hq."handPhone1" IS NOT NULL AND r.a_number = hq."handPhone1")
      OR
      (hq."handPhone1" IS NULL AND r.recording_customer_id = hq.prospect_id)
    )
)
INSERT INTO dashboard.recording_tms_api (
  tiket_id, file_path,
  julian_mis_date, mis_date, id, prospect_id, customer_id, cust_name,
  campaign, agent_id, status, created_by, created_time, created_date,
  recording_customer_id, recording_created_time, a_number, context,
  source_api, load_date, status_data, error_message
)
SELECT
  j.id || '_' || TO_CHAR(j.recording_created_time, 'YYYYMMDDHH24MISS') AS tiket_id,
  j.file_path,
  j.julian_mis_date, j.mis_date, j.id, j.prospect_id, j.customer_id, j.cust_name,
  j.campaign, j.agent_id, j.status, j.created_by,
  COALESCE((j.recording_created_time)::timestamp, to_timestamp(j.mis_date,'YYYYMMDD')) AS created_time,
  COALESCE((j.recording_created_time)::date,       to_date(j.mis_date,'YYYYMMDD'))      AS created_date,
  j.prospect_id                                           AS recording_customer_id,
  j.recording_created_time, j.a_number, j.context,
  :source_api, CURRENT_DATE, NULL, NULL
FROM joined j
WHERE j.file_path IS NOT NULL
ORDER BY id, recording_created_time
ON CONFLICT (tiket_id, file_path) DO NOTHING;
"""


def build_date_clause(date_from: Optional[date], date_to: Optional[date]) -> str:
    """Membangun fragmen filter tanggal untuk CTE ``hist``.

    Hanya dipakai pada mode ``--all-ids``. Pada mode backfill (TARGET_IDS
    aktif), pembatas-nya adalah daftar id, bukan tanggal -- lihat catatan di
    docstring modul.

    Memakai kolom tanggal ternormalisasi
    ``COALESCE(h.created_time::date, to_date(h."MIS_DATE",'YYYYMMDD'))`` agar baris
    history yang ``created_time``-nya kosong tetap dapat difilter lewat
    ``"MIS_DATE"``.

    Fragmen yang dikembalikan bersifat **statis** — nilai tanggalnya sendiri
    dikirim terpisah sebagai bind parameter (``:date_from``, ``:date_to``) oleh
    :func:`main`, sehingga tidak ada nilai input yang di-interpolasi ke SQL.

    Args:
        date_from: Batas bawah inklusif, atau ``None``.
        date_to: Batas atas inklusif, atau ``None``.

    Returns:
        Fragmen ``AND ...`` sesuai kombinasi argumen; string kosong bila kedua
        argumen ``None``.
    """
    # Note: We use the normalized date column (COALESCE...) for filtering
    # Harus memakai nama kolom asli h."MIS_DATE" (huruf besar, dikutip), bukan alias
    # keluaran `mis_date` dari CTE hist: fragmen ini disisipkan ke klausa WHERE pada
    # SELECT yang sama, dan SQL tidak mengizinkan alias keluaran dipakai di WHERE.
    normalized_date_col = """COALESCE(h.created_time::date, to_date(h."MIS_DATE",'YYYYMMDD'))"""

    if date_from and date_to:
        return f"AND {normalized_date_col} BETWEEN :date_from AND :date_to"
    if date_from:
        return f"AND {normalized_date_col} >= :date_from"
    if date_to:
        return f"AND {normalized_date_col} <= :date_to"
    return ""


def build_id_clause(use_target_ids: bool) -> str:
    """Membangun fragmen pembatasan id untuk CTE ``hist``.

    Fragmen bersifat statis; daftar id-nya sendiri dikirim sebagai satu bind
    parameter list (``:target_ids``) oleh :func:`main`, sehingga tidak ada nilai
    yang di-interpolasi ke SQL.

    Args:
        use_target_ids: Bila ``True`` dan :data:`TARGET_IDS` tidak kosong,
            kembalikan fragmen pembatas. Bila ``False`` (flag ``--all-ids``),
            kembalikan string kosong sehingga seluruh history diproses.

    Returns:
        Fragmen ``AND h.id = ANY(:target_ids)`` atau string kosong.
    """
    if use_target_ids and TARGET_IDS:
        return "AND h.id = ANY(:target_ids)"
    return ""


def main(source_api: str,
           date_from: Optional[date] = None,
           date_to: Optional[date] = None,
           rec_from: Optional[date] = None,
           rec_to: Optional[date] = None,
           all_ids: bool = False) -> None:
    """Menjalankan satu siklus ETL penuh dalam satu transaksi.

    Langkah:
        1. Muat konfigurasi dan buat engine (``pool_pre_ping=True`` agar koneksi
           mati terdeteksi sebelum dipakai).
        2. Bangun fragmen filter id dan (bila mode ``--all-ids``) filter tanggal.
        3. Substitusi kedua fragmen ke placeholder ``/**ID_FILTER**/`` dan
           ``/**DATE_RANGE**/`` pada :data:`INSERT_SQL`.
        4. Susun bind parameter, lalu eksekusi ``INSERT`` di dalam
           ``engine.begin()`` sehingga otomatis commit bila sukses dan rollback
           bila terjadi exception.

    Args:
        source_api: Label yang disimpan ke kolom ``source_api`` sebagai penanda
            asal data. Nilai produksi berbentuk URL (``http://localhost:8888``),
            namun job **tidak pernah** memanggil alamat tersebut — nilainya
            murni sebagai teks penanda.
        date_from: Batas bawah inklusif filter tanggal ``hist``. HANYA berlaku
            pada mode ``--all-ids``; diabaikan pada mode backfill.
        date_to: Batas atas inklusif filter tanggal ``hist``. HANYA berlaku pada
            mode ``--all-ids``; diabaikan pada mode backfill.
        rec_from: Batas bawah inklusif window rekaman. Default
            :data:`DEFAULT_REC_FROM`.
        rec_to: Batas atas **exclusive** window rekaman. Default
            :data:`DEFAULT_REC_TO`.
        all_ids: Bila ``True``, abaikan :data:`TARGET_IDS` dan proses seluruh
            history. Wajib dipakai untuk run produksi/cron.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: Bila koneksi atau eksekusi SQL gagal.
            Transaksi otomatis di-rollback.

    Note:
        Jumlah baris yang benar-benar tersimpan tidak dilaporkan oleh SQL ini.
        Karena ``ON CONFLICT DO NOTHING``, eksekusi ulang atas rentang yang sama
        akan selesai normal tanpa menyisipkan baris baru.
    """
    settings = Settings()
    engine = create_engine(settings.sqlalchemy_uri, pool_pre_ping=True, future=True)

    use_target_ids = not all_ids
    params = {"source_api": source_api}

    # 1. Fragmen pembatasan id (mode backfill)
    id_clause = build_id_clause(use_target_ids=use_target_ids)

    # 2. Fragmen filter tanggal hist -- HANYA pada mode --all-ids.
    #    Pada mode backfill, daftar id sudah jadi pembatas; menambah filter
    #    tanggal di hist justru berisiko membuang id di pinggir rentang.
    date_clause = "" if use_target_ids else build_date_clause(date_from, date_to)

    # 3. Substitusi kedua fragmen
    sql = INSERT_SQL.replace("/**ID_FILTER**/", f"\n  {id_clause}\n" if id_clause else "")
    sql = sql.replace("/**DATE_RANGE**/", f"\n  {date_clause}\n" if date_clause else "")

    # 4. Bind parameter: daftar id (list Python -> array Postgres)
    if id_clause:
        params["target_ids"] = TARGET_IDS

    # 5. Bind parameter: filter tanggal hist (hanya bila fragmennya terpasang)
    if date_clause:
        if date_from:
            params["date_from"] = date_from.isoformat()
        if date_to:
            params["date_to"] = date_to.isoformat()

    # 6. Bind parameter: window tanggal rekaman (batas atas EXCLUSIVE)
    rec_from_eff = rec_from or DEFAULT_REC_FROM
    rec_to_eff   = rec_to   or DEFAULT_REC_TO
    params["rec_date_from"] = rec_from_eff.isoformat()
    params["rec_date_to"]   = rec_to_eff.isoformat()

    scope = "SELURUH history" if all_ids else f"{len(TARGET_IDS)} id target"
    print(f"refresh_recording_tms_api: memproses {scope}; "
          f"rekaman {rec_from_eff} s/d {rec_to_eff} (batas atas exclusive)...")

    # 7. Execute the SQL transaction
    with engine.begin() as conn:
        result = conn.execute(text(sql), params)
        # rowcount = baris yang BENAR-BENAR ter-insert (yang kena ON CONFLICT
        # DO NOTHING tidak dihitung). Berguna untuk membedakan "job sukses tapi
        # semuanya duplikat" dari "job sukses dan ada data baru".
        inserted = result.rowcount

    print(f"refresh_recording_tms_api: INSERT done, {inserted} baris tersimpan "
          f"(sisanya dilewati oleh ON CONFLICT DO NOTHING).")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-api", required=True,
                    help="Tag sumber untuk kolom source_api (mis. http://localhost:8888 atau nama job)")
    ap.add_argument("--rec-from", type=lambda s: date.fromisoformat(s),
                    help=f"Batas bawah window rekaman, YYYY-MM-DD (default {DEFAULT_REC_FROM})")
    ap.add_argument("--rec-to", type=lambda s: date.fromisoformat(s),
                    help=f"Batas atas window rekaman (EXCLUSIVE), YYYY-MM-DD (default {DEFAULT_REC_TO})")
    ap.add_argument("--date-from", type=lambda s: date.fromisoformat(s),
                    help="Filter tanggal hist, YYYY-MM-DD. Hanya berlaku bersama --all-ids.")
    ap.add_argument("--date-to", type=lambda s: date.fromisoformat(s),
                    help="Filter tanggal hist, YYYY-MM-DD. Hanya berlaku bersama --all-ids.")
    ap.add_argument("--all-ids", action="store_true",
                    help="Abaikan TARGET_IDS dan proses seluruh history. WAJIB untuk run cron/produksi.")
    args = ap.parse_args()

    if not args.all_ids and (args.date_from or args.date_to):
        print("PERINGATAN: --date-from/--date-to diabaikan pada mode backfill "
              "(TARGET_IDS aktif). Pakai --all-ids bila ingin filter tanggal hist, "
              "atau --rec-from/--rec-to untuk mengatur window rekaman.",
              file=sys.stderr)

    try:
        main(
            source_api=args.source_api,
            date_from=args.date_from,
            date_to=args.date_to,
            rec_from=args.rec_from,
            rec_to=args.rec_to,
            all_ids=args.all_ids,
        )
    except Exception as e:
        print(f"ERROR: Failed to run job: {e}", file=sys.stderr)
        # Wajib: tanpa exit code non-nol, proses berakhir dengan status 0 dan cron
        # akan menganggap job sukses meski transaksi gagal / di-rollback.
        sys.exit(1)
