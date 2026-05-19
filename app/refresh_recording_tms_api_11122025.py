"""
Usage:
  python -m app.jobs.refresh_recording_tms_api --source-api http://localhost:8888
  python -m app.jobs.refresh_recording_tms_api --source-api "etl-batch" --date-from 2025-09-01 --date-to 2025-09-30
Env:
  PG_HOST, PG_PORT, PG_USER, PG_PASSWORD, PG_DATABASE
"""
from datetime import date
from typing import Optional
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy import create_engine, text
import argparse, urllib.parse, sys

class Settings(BaseSettings):
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
TABLE_QUEQUE = '"manaf_acs_predictive_queue"'

# ========= INSERT (append) - NOW USING F-STRING =========
# Using f"""...""" to allow Python variables (like TABLE_HISTORY) to be injected.
INSERT_SQL = f"""
WITH hist AS (
  SELECT
    -- Menggunakan Kutip Ganda dan Huruf Besar untuk kolom yang sensitif case
    h."JULIAN_MIS_DATE" AS julian_mis_date, h."MIS_DATE" AS mis_date, h.id, h.prospect_id, h.customer_id, h.cust_name, h.file_id, 
    h.campaign, h.agent_id, h.max_transfer, h.group_prospect, h.last_response_reason, 
    h.last_response_sub_reason, h.last_description, h.first_contact_time, h.last_contact_time, 
    h.last_contact_by, h.priority, h.qc_group, h.qc_by, h.qc_status, h.qc_sub_status, 
    h.qc_sub_sub_status, h.qc_pickup_time, h.qc_notes, h.is_customer, 
    h.is_response_agent, h.is_sync, h.is_upload_doc, h.file_document, h.upload_by, 
    h.notes, h.total_input, h.service_level, h.status, h.revision, 
    h.is_main_campaign, h.reff, h.seq_number, h.status_microsite, h.created_by, h.created_time
    
  FROM dashboard.{TABLE_HISTORY} h -- Variable injected here
  WHERE h.status = 4 AND h.last_response_reason = 'Agree' 
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
    r.file_path,
    ROW_NUMBER() OVER (
      PARTITION BY r.recording_customer_id, r.a_number
      ORDER BY r.created_time DESC
    ) AS rn
  FROM dashboard.{TABLE_RECORDING} r -- Variable injected here
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
	JOIN queque q
	ON q.contract_number = h.prospect_id
),
joined AS (
  SELECT
    hq.*,
    r.rec_created_ts         AS recording_created_time,
    r.a_number,
    r.context,
    r.file_path,
    r.rn
  FROM hist_queque hq
  LEFT JOIN rec r
    ON r.a_number = hq."handPhone1"
    
)
INSERT INTO dashboard.recording_tms_api (
  -- KEY
  tiket_id, file_path,
  -- HISTORY (sesuai kolom yang kamu minta)
  julian_mis_date, mis_date, id, prospect_id, customer_id, cust_name,
  campaign, agent_id, status, created_by, created_time, created_date,
  -- RECORDING ringkas
  recording_customer_id, recording_created_time, a_number, context,
  -- META
  source_api, load_date, status_data, error_message
)
SELECT
  CASE WHEN j.rn = 1 THEN j.id || '_a'
       WHEN j.rn = 2 THEN j.id || '_b'
       WHEN j.rn = 3 THEN j.id || '_c'
       WHEN j.rn = 4 THEN j.id || '_d'
       ELSE j.id END                                      AS tiket_id,
  j.file_path,

  j.julian_mis_date, j.mis_date, j.id, j.prospect_id, j.customer_id, j.cust_name,
  j.campaign, j.agent_id, j.status, j.created_by,

  -- hitung langsung, tanpa alias *_norm
  COALESCE((j.recording_created_time)::timestamp, to_timestamp(j.mis_date,'YYYYMMDD')) AS created_time,
  COALESCE((j.recording_created_time)::date,       to_date(j.mis_date,'YYYYMMDD'))      AS created_date,

  j.prospect_id                                           AS recording_customer_id,
  j.recording_created_time, j.a_number, j.context,

  :source_api, CURRENT_DATE, NULL, NULL
FROM joined j
WHERE j.file_path IS NOT NULL
  AND (j.rn IS NULL OR j.rn IN (1,2, 3, 4))
ON CONFLICT (tiket_id, file_path) DO NOTHING;
"""

def build_date_clause(date_from: Optional[date], date_to: Optional[date]) -> str:
    """Builds the WHERE clause for date filtering."""
    # Note: We use the normalized date column (COALESCE...) for filtering
    normalized_date_col = "COALESCE(h.created_time::date, to_date(h.mis_date,'YYYYMMDD'))"
    
    if date_from and date_to:
        return f"AND {normalized_date_col} BETWEEN :date_from AND :date_to"
    if date_from:
        return f"AND {normalized_date_col} >= :date_from"
    if date_to:
        return f"AND {normalized_date_col} <= :date_to"
    return ""

def main(source_api: str,
           date_from: Optional[date],
           date_to: Optional[date]) -> None:
    settings = Settings()
    engine = create_engine(settings.sqlalchemy_uri, pool_pre_ping=True, future=True)

    params = {"source_api": source_api}
    
    # 1. Build the dynamic date filtering clause
    date_clause = build_date_clause(date_from, date_to)
    
    # 2. Inject the date filtering into the SQL string
    sql = INSERT_SQL.replace("/**DATE_RANGE**/", f"\n  {date_clause}\n" if date_clause else "")

    # 3. Add date parameters if needed
    if date_from:
        params["date_from"] = date_from.isoformat()
    if date_to:
        params["date_to"]  = date_to.isoformat()

    # 4. Execute the SQL transaction
    with engine.begin() as conn:
        conn.execute(text(sql), params)

    print("refresh_recording_tms_api: INSERT done (ON CONFLICT DO NOTHING).")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source-api", required=True, help="Tag sumber untuk kolom source_api (mis. http://localhost:8888 atau nama job)")
    ap.add_argument("--date-from", type=lambda s: date.fromisoformat(s), help="YYYY-MM-DD")
    ap.add_argument("--date-to",   type=lambda s: date.fromisoformat(s), help="YYYY-MM-DD")
    args = ap.parse_args()

    try:
        main(
            source_api=args.source_api,
            date_from=args.date_from,
            date_to=args.date_to,
        )
    except Exception as e:
        print(f"ERROR: Failed to run job: {e}", file=sys.stderr)
