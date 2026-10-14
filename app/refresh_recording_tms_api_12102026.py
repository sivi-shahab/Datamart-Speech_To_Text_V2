"""Batch ETL harian: mengisi ``dashboard.recording_tms_api`` dari TMS prospect time frame + rekaman.

Job ini mengambil prospek berstatus Agree hari ini (CURRENT_DATE) dari
``tms_prospect_time_frame``, menurunkan agent asli (bukan ``UPDATE BY MICROSITE``),
lalu menjodohkannya dengan rekaman e-centrix hari ini berdasarkan agent + prospect_id.
Baris hasil dipakai downstream (speech-to-text) lewat ``status_data IS NULL``.

Alur transformasi::

    base -> tagged -> grouped -> filled -> agree_only ─┐
                                                        ├─> joined -> INSERT
    recording (hari ini, durasi > 60 dtk, terbaru) ────┘

Job bersifat idempotent berkat ``ON CONFLICT (tiket_id, file_path) DO NOTHING``.

Usage::

    cd app
    python refresh_recording_tms_api.py --source-api http://localhost:8888

Env:
    DATABASE_URL -- URI SQLAlchemy lengkap, dibaca dari ``.env`` di direktori kerja.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine, text
import argparse
import sys


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


INSERT_SQL = f"""
WITH base AS (
    SELECT
        a.id,
        a.prospect_id,
        a.customer_id,
        a.campaign          AS campaign_code,
        a.agent_id_raw,
        a.created_time_raw,
        a.status
    FROM dashboard.{TABLE_TIME_FRAME} a
    WHERE a.created_time_raw >= CURRENT_DATE - INTERVAL '1 day'
      AND a.created_time_raw <  CURRENT_DATE
),
tagged AS (
    SELECT *,
        CASE WHEN agent_id_raw <> 'UPDATE BY MICROSITE' THEN 1 ELSE 0 END AS is_real_agent
    FROM base
),
grouped AS (
    SELECT *,
        SUM(is_real_agent) OVER (
            PARTITION BY id ORDER BY created_time_raw ROWS UNBOUNDED PRECEDING
        ) AS grp
    FROM tagged
),
filled AS (
    SELECT *,
        MAX(CASE WHEN is_real_agent = 1 THEN agent_id_raw END)
            OVER (PARTITION BY id, grp) AS agent_id,
        MAX(CASE WHEN is_real_agent = 1 THEN created_time_raw END)
            OVER (PARTITION BY id, grp) AS last_contact_time
    FROM grouped
),
agree_only AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY id, prospect_id ORDER BY created_time_raw ASC
        ) AS rn
    FROM filled
    WHERE status = 'Agree'
      AND agent_id IS NOT NULL
),
recording AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY recording_customer_id, rec_agent_id
            ORDER BY rec_created_ts DESC
        ) AS rn_rec
    FROM (
        SELECT r.recording_customer_id,
               r.created_time AS rec_created_ts,
               r.a_number,
               r.agent_id     AS rec_agent_id,
               r.file_path
        FROM dashboard.{TABLE_RECORDING} r
        WHERE r.duration > 60
          AND r.file_path IS NOT NULL
    ) sub
),
joined AS (
    SELECT
        TO_CHAR(a.last_contact_time, 'YYYYDDD')  AS julian_mis_date,
        TO_CHAR(a.last_contact_time, 'YYYYMMDD') AS mis_date,
        a.id,
        a.prospect_id,
        a.customer_id,
        NULL::text                               AS cust_name,
        b.name                                   AS campaign,
        a.agent_id,
        a.agent_id_raw                           AS created_by,
        r.rec_created_ts                         AS recording_created_time,
        r.a_number,
        a.campaign_code                          AS context,
        r.file_path
    FROM agree_only a
    LEFT JOIN dashboard.{TABLE_CAMPAIGN} b
           ON b.code = a.campaign_code
    JOIN recording r
           ON r.rec_agent_id          = a.agent_id
          AND r.recording_customer_id = a.prospect_id
          AND r.rn_rec = 1
    WHERE a.rn = 1
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
WHERE j.file_path IS NOT NULL
ON CONFLICT (tiket_id, file_path) DO NOTHING;
"""

def main(source_api: str) -> None:
    """Menjalankan satu siklus ETL harian dalam satu transaksi (commit/rollback otomatis)."""
    settings = Settings()
    engine = create_engine(settings.sqlalchemy_uri, pool_pre_ping=True, future=True)

    with engine.begin() as conn:
        result = conn.execute(text(INSERT_SQL), {"source_api": source_api})
        inserted = result.rowcount

    print(f"refresh_recording_tms_api: CURRENT_DATE, inserted {inserted} row(s) (ON CONFLICT DO NOTHING).")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-api", required=True,
                    help="Tag sumber untuk kolom source_api (mis. http://localhost:8888 atau nama job)")
    args = ap.parse_args()

    try:
        main(source_api=args.source_api)
    except Exception as e:
        print(f"ERROR: Failed to run job: {e}", file=sys.stderr)
        sys.exit(1)  # wajib non-nol agar cron tidak menganggap sukses