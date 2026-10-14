"""Batch ETL: mengisi ``dashboard.recording_tms_api`` dari TMS prospect time frame + rekaman.

Job ini mengambil prospek berstatus Agree dalam rentang tanggal tertentu (default:
kemarin + hari ini) dari ``tms_prospect_time_frame`` (replika ecentrix), menentukan
agent yang berhak atas Agree tersebut, lalu menjodohkannya dengan rekaman e-centrix.
Baris hasil dipakai downstream (speech-to-text) lewat ``status_data IS NULL``.

Aturan bisnis:
    1. Agree terakhir per ticket_id + campaign_id dalam rentang tanggal.
    2. Jika Agree dicatat oleh ``UPDATE BY MICROSITE``, agent_id diisi dengan agent
       terakhir (group_id = 'Agent Tele Test') yang menghubungi ticket + campaign
       yang sama sebelum waktu Agree, tanpa batas tanggal dan apa pun statusnya.
    3. Recording diambil dari agent tersebut untuk nasabah yang sama, durasi > 60 detik,
       dalam 7 hari kalender ke belakang dari tanggal Agree s/d akhir hari Agree.
       Semua recording yang memenuhi syarat dimasukkan (satu baris per recording).

Alur transformasi::

    base -> filled -> agree_only ─┐
                                   ├─> joined -> INSERT
    recording (H-7 s/d hari Agree, durasi > 60 dtk) ┘

Job bersifat idempotent berkat ``ON CONFLICT (tiket_id, file_path) DO NOTHING``,
sehingga aman dijalankan berulang kali (mis. tiap jam).

Usage::

    cd app
    # default: Agree kemarin + hari ini
    python refresh_recording_tms_api.py --source-api http://localhost:8888

    # backfill rentang tertentu (end-date eksklusif)
    python refresh_recording_tms_api.py --source-api http://localhost:8888 \\
        --start-date 2026-10-02 --end-date 2026-10-03

Env:
    DATABASE_URL -- URI SQLAlchemy lengkap, dibaca dari ``.env`` di direktori kerja.

Index yang disarankan (dibuat sekali)::

    CREATE INDEX IF NOT EXISTS idx_tf_ticket_campaign_time
        ON dashboard."manaf_tms_prospect_time_frame" (ticket_id, campaign_id, created_time DESC);
    CREATE INDEX IF NOT EXISTS idx_rec_cust_agent_time
        ON dashboard."MANAF_ecentrix_recording" (recording_customer_id, agent_id, created_time DESC);
    CREATE UNIQUE INDEX IF NOT EXISTS ux_recording_tms_api_tiket_file
        ON dashboard.recording_tms_api (tiket_id, file_path);
"""
from datetime import date, timedelta
import argparse
import sys

from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine, text


class Settings(BaseSettings):
    """Konfigurasi job dari environment variable atau ``.env`` (relatif ke CWD)."""
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")
    DATABASE_URL: str

    @property
    def sqlalchemy_uri(self) -> str:
        return self.DATABASE_URL


# --- Table Names (kutip bila nama tabel mixed-case di PostgreSQL) ---
TABLE_TIME_FRAME = '"manaf_tms_prospect_time_frame"'
TABLE_RECORDING  = '"MANAF_ecentrix_recording"'
TABLE_CAMPAIGN   = '"tms_campaign"'

# --- Business rules ---
MICROSITE_AGENT   = "UPDATE BY MICROSITE"
AGENT_GROUP       = "Agent Tele Test"
MIN_DURATION_SEC  = 60
RECORDING_LOOKBACK_DAYS = 7


INSERT_SQL = f"""
WITH base AS (                                   -- Agree terakhir per ticket + campaign
    SELECT DISTINCT ON (a.ticket_id, a.campaign_id)
        a.ticket_id      AS id,
        a.prospect_id,
        a.customer_id,
        a.campaign_id    AS campaign_code,
        a.created_by     AS agent_id_raw,
        a.created_time   AS created_time_raw
    FROM dashboard.{TABLE_TIME_FRAME} a
    WHERE a.created_time >= :start_date
      AND a.created_time <  :end_date
      AND a.response_status = 'Agree'
    ORDER BY a.ticket_id, a.campaign_id, a.created_time DESC
),
filled AS (                                      -- microsite -> agent terakhir di grup agent
    SELECT
        b.*,
        CASE WHEN b.agent_id_raw = '{MICROSITE_AGENT}'
             THEN h.created_by   ELSE b.agent_id_raw     END AS agent_id,
        CASE WHEN b.agent_id_raw = '{MICROSITE_AGENT}'
             THEN h.created_time ELSE b.created_time_raw END AS last_contact_time
    FROM base b
    LEFT JOIN LATERAL (
        SELECT x.created_by, x.created_time
        FROM dashboard.{TABLE_TIME_FRAME} x
        WHERE b.agent_id_raw = '{MICROSITE_AGENT}'
          AND x.ticket_id   = b.id
          AND x.campaign_id = b.campaign_code
          AND x.group_id    = '{AGENT_GROUP}'
          AND x.created_by IS NOT NULL
          AND x.created_by <> '{MICROSITE_AGENT}'
          AND x.created_time < b.created_time_raw
        ORDER BY x.created_time DESC
        LIMIT 1
    ) h ON TRUE
),
agree_only AS (
    SELECT *
    FROM filled
    WHERE agent_id IS NOT NULL
),
joined AS (                                      -- semua recording H-7 s/d hari Agree, > 1 menit
    SELECT
        TO_CHAR(a.last_contact_time, 'YYYYDDD')  AS julian_mis_date,
        TO_CHAR(a.last_contact_time, 'YYYYMMDD') AS mis_date,
        a.id,
        a.prospect_id,
        a.customer_id,
        NULL::text                               AS cust_name,
        c.name                                   AS campaign,
        a.agent_id,
        a.agent_id_raw                           AS created_by,
        rr.created_time                          AS recording_created_time,
        rr.a_number,
        a.campaign_code                          AS context,
        rr.file_path
    FROM agree_only a
    LEFT JOIN dashboard.{TABLE_CAMPAIGN} c
           ON c.code = a.campaign_code
    JOIN dashboard.{TABLE_RECORDING} rr
           ON rr.recording_customer_id = a.prospect_id
          AND rr.agent_id      = a.agent_id
          AND rr.duration      > {MIN_DURATION_SEC}
          AND rr.file_path IS NOT NULL
          AND rr.created_time >= a.created_time_raw::date - INTERVAL '{RECORDING_LOOKBACK_DAYS} days'
          AND rr.created_time <  a.created_time_raw::date + INTERVAL '1 day'
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
  j.campaign, j.agent_id,
  NULL                                                                  AS status,
  j.created_by,
  COALESCE(j.recording_created_time::timestamp, to_timestamp(j.mis_date, 'YYYYMMDD')) AS created_time,
  COALESCE(j.recording_created_time::date,      to_date(j.mis_date, 'YYYYMMDD'))      AS created_date,
  j.prospect_id                                                         AS recording_customer_id,
  j.recording_created_time, j.a_number, j.context,
  :source_api, CURRENT_DATE, NULL, NULL
FROM joined j
ON CONFLICT (tiket_id, file_path) DO NOTHING;
"""


def main(source_api: str, start_date: date, end_date: date) -> None:
    """Menjalankan satu siklus ETL dalam satu transaksi (commit/rollback otomatis)."""
    if end_date <= start_date:
        raise ValueError(f"end_date ({end_date}) harus lebih besar dari start_date ({start_date})")

    settings = Settings()
    engine = create_engine(settings.sqlalchemy_uri, pool_pre_ping=True, future=True)

    with engine.begin() as conn:
        result = conn.execute(text(INSERT_SQL), {
            "start_date": start_date,
            "end_date":   end_date,
            "source_api": source_api,
        })
        inserted = result.rowcount

    print(
        f"refresh_recording_tms_api: Agree {start_date} s/d {end_date - timedelta(days=1)}, "
        f"inserted {inserted} row(s) (ON CONFLICT DO NOTHING)."
    )


if __name__ == "__main__":
    today = date.today()

    ap = argparse.ArgumentParser()
    ap.add_argument("--source-api", required=True,
                    help="Tag sumber untuk kolom source_api (mis. http://localhost:8888 atau nama job)")
    ap.add_argument("--start-date", type=date.fromisoformat, default=today - timedelta(days=1),
                    help="Tanggal awal Agree, inklusif (YYYY-MM-DD). Default: kemarin")
    ap.add_argument("--end-date", type=date.fromisoformat, default=today + timedelta(days=1),
                    help="Tanggal akhir Agree, eksklusif (YYYY-MM-DD). Default: besok (hari ini ikut)")
    args = ap.parse_args()

    try:
        main(source_api=args.source_api, start_date=args.start_date, end_date=args.end_date)
    except Exception as e:
        print(f"ERROR: Failed to run job: {e}", file=sys.stderr)
        sys.exit(1)  # wajib non-nol agar cron tidak menganggap sukses