# app/main.py
from fastapi import FastAPI, Query
from typing import List, Optional, Dict, Tuple
from datetime import date
from pydantic import BaseModel
from pydantic_settings import BaseSettings
from sqlalchemy import create_engine, text, bindparam
import urllib.parse

app = FastAPI(title="History -> Recording (flat, tiket_id with _a/_b) [PostgreSQL]")

# ---------------- Settings (PostgreSQL) ----------------
class Settings(BaseSettings):
    PG_HOST: str
    PG_PORT: int = 5432
    PG_USER: str
    PG_PASSWORD: str
    PG_DATABASE: str

    # Opsional: nama schema untuk tabel sumber (silakan sesuaikan)
    SCHEMA_HISTORY: str = "stg_host"
    TABLE_HISTORY: str = "HISTORY_TMS_PROSPECT_DETAIL_CAMPAIGN"

    SCHEMA_RECORDING: str = "stg_host_restore"
    TABLE_RECORDING: str = "manaf_ecentrix_recording"

    @property
    def sqlalchemy_uri(self) -> str:
        user = urllib.parse.quote_plus(self.PG_USER)
        pwd = urllib.parse.quote_plus(self.PG_PASSWORD)
        host = self.PG_HOST
        port = self.PG_PORT
        db   = self.PG_DATABASE
        return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

settings = Settings()
engine = create_engine(settings.sqlalchemy_uri, pool_pre_ping=True, pool_recycle=1800, future=True)

# ---------------- Schemas (response) ----------------
class FlatRow(BaseModel):
    # Tabel 1 (history)
    tiket_id: Optional[str] = None
    agent_id: Optional[str] = None
    status: Optional[int] = None
    created_time: Optional[str] = None  # "YYYY-MM-DD HH:MM:SS.mmm"
    prospect_id: Optional[str] = None
    # Tabel 2 (recording)
    a_number: Optional[str] = None
    context: Optional[str] = None
    file_path: Optional[str] = None

class FlatResponse(BaseModel):
    count: int
    items: List[FlatRow]

# ---------------- SQL templates (PostgreSQL dialect) ----------------
# Step 1: ambil HISTORY (ganti schema.table sesuai env kamu)
SQL_HISTORY = f"""
SELECT
    h.id,
    CAST(h.agent_id AS VARCHAR(64)) AS agent_id,
    h.status,
    to_char(h.created_time, 'YYYY-MM-DD HH24:MI:SS.MS') AS created_time,
    h.created_time::date AS created_date,
    CAST(h.prospect_id AS VARCHAR(64)) AS prospect_id
FROM {settings.SCHEMA_HISTORY}."{settings.TABLE_HISTORY}" AS h
WHERE h.status = :status
  /**DATE_FROM**/
  /**DATE_TO**/
ORDER BY h.created_time DESC
LIMIT :limit;
"""

def _build_history_sql(date_from: Optional[date], date_to: Optional[date]) -> str:
    sql = SQL_HISTORY
    sql = sql.replace("/**DATE_FROM**/", "AND h.created_time::date >= :date_from" if date_from else "")
    sql = sql.replace("/**DATE_TO**/", "AND h.created_time::date <= :date_to" if date_to else "")
    return sql

# Step 2: ambil TOP-2 terbaru per (recording_customer_id, created_date)
# created_time_recording dipakai untuk sortir; tidak ditampilkan ke klien
SQL_RECORDING_TOP2 = f"""
WITH rec AS (
  SELECT
      btrim(left(CAST(r.customer_data AS VARCHAR(256)), 32)) AS recording_customer_id,
      r.created_time::date AS created_date,
      to_char(r.created_time, 'YYYY-MM-DD HH24:MI:SS.MS') AS created_time_recording,
      r.a_number,
      r.context AS context,
      r.file_path,
      ROW_NUMBER() OVER (
        PARTITION BY btrim(left(CAST(r.customer_data AS VARCHAR(256)), 32)),
                     r.created_time::date
        ORDER BY r.created_time DESC
      ) AS rn
  FROM {settings.SCHEMA_RECORDING}."{settings.TABLE_RECORDING}" AS r
  WHERE btrim(left(CAST(r.customer_data AS VARCHAR(256)), 32)) = ANY(:ids)
    AND r.created_time::date = ANY(:dt)
)
SELECT
  recording_customer_id,
  created_date,
  created_time_recording,
  a_number,
  context,
  file_path
FROM rec
WHERE rn <= 2;
"""

def _chunk(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# ---------------- Endpoint ----------------
@app.get("/history-recording-flat", response_model=FlatResponse)
def history_recording_flat(
    status: int = Query(4, description="Filter status di HISTORY (default 4)"),
    date_from: Optional[date] = Query(None, description="Filter DATE (inklusif) dari HISTORY.created_time"),
    date_to: Optional[date] = Query(None, description="Filter DATE (inklusif) sampai HISTORY.created_time"),
    limit: int = Query(500, ge=1, le=10000, description="Maks baris dari HISTORY yang diambil"),
    batch_size: int = Query(800, ge=50, le=2000, description="Ukuran batch untuk array-param RECORDING"),
):
    """
    Output flat per rekaman:
      tiket_id, agent_id, status, created_time, prospect_id, a_number, context, file_path

    Aturan tiket_id:
      - Jika sebuah id history punya 2 recording pada hari yang sama,
        maka tiket paling baru: "<id>_a", dan yang kedua: "<id>_b".
      - Jika hanya 1 recording, pakai "<id>" tanpa suffix.
      - Jika tidak ada recording, tetap satu baris dengan kolom tabel 2 = null dan tiket_id = "<id>".
    """
    # ---- Step 1: HISTORY ----
    sql_hist = _build_history_sql(date_from, date_to)
    params_hist = {"status": status, "limit": limit}
    if date_from: params_hist["date_from"] = date_from.isoformat()
    if date_to:   params_hist["date_to"] = date_to.isoformat()

    with engine.connect() as conn:
        hist_rows = conn.execute(text(sql_hist), params_hist).mappings().all()
        if not hist_rows:
            return FlatResponse(count=0, items=[])

        # kumpulkan key (prospect_id, created_date) & simpan data untuk merge
        keys: List[Tuple[str, date]] = []
        hist_core: List[Dict] = []
        for r in hist_rows:
            pid = (r.get("prospect_id") or "").strip()
            cdt = r.get("created_date")
            hist_core.append({
                "id": r.get("id"),
                "agent_id": (r.get("agent_id") or "").strip() if r.get("agent_id") else None,
                "status": r.get("status"),
                "created_time": r.get("created_time"),
                "prospect_id": pid,
                "created_date": cdt,
            })
            if pid and cdt:
                keys.append((pid, cdt))

        if not keys:
            items = [
                FlatRow(
                    tiket_id=str(h["id"]) if h["id"] is not None else None,
                    agent_id=h["agent_id"],
                    status=h["status"],
                    created_time=h["created_time"],
                    prospect_id=h["prospect_id"],
                    a_number=None, context=None, file_path=None,
                )
                for h in hist_core
            ]
            return FlatResponse(count=len(items), items=items)

        unique_ids = sorted(set(pid for pid, _ in keys))
        unique_dates = sorted(set(dt for _, dt in keys))

        # (pid, date) -> list[dict(created_time_recording, a_number, context, file_path)]
        rec_map: Dict[Tuple[str, date], List[Dict]] = {}

        # ---- Step 2: RECORDING (no JOIN, dibatch) ----
        for ids_batch in _chunk(unique_ids, batch_size):
            # di Postgres, array-parameter gunakan ANY(:param)
            # SQLAlchemy: tidak perlu expanding=True; kirim list → dialect PG akan map ke array
            stmt = text(SQL_RECORDING_TOP2).bindparams(
                bindparam("ids"),   # list[str]
                bindparam("dt"),    # list[date]
            )
            rec_rows = conn.execute(stmt, {"ids": ids_batch, "dt": unique_dates}).mappings().all()
            for rr in rec_rows:
                rid = (rr.get("recording_customer_id") or "").strip()
                rdt = rr.get("created_date")
                if rid and rdt:
                    rec_map.setdefault((rid, rdt), []).append(
                        {
                            "created_time_recording": rr.get("created_time_recording"),
                            "a_number": rr.get("a_number"),
                            "context": rr.get("context"),
                            "file_path": rr.get("file_path"),
                        }
                    )

    # ---- Step 3: FLATTEN + suffix _a/_b ----
    flat_items: List[FlatRow] = []
    for h in hist_core:
        key = (h["prospect_id"], h["created_date"])
        recs = rec_map.get(key, [])

        if recs:
            # Sortir DESC by created_time_recording (yang terbaru dulu)
            recs_sorted = sorted(
                recs,
                key=lambda x: x.get("created_time_recording") or "",
                reverse=True
            )
            # jika 2 rekaman, suffix _a untuk terbaru, _b untuk kedua
            if len(recs_sorted) >= 2:
                suffixes = ["_a", "_b"]
            else:
                suffixes = [""]  # satu rekaman: tanpa suffix

            for idx, rec in enumerate(recs_sorted[:2]):
                suffix = suffixes[idx] if idx < len(suffixes) else ""
                tiket_id = (str(h["id"]) if h["id"] is not None else None)
                if tiket_id is not None and suffix:
                    tiket_id = f"{tiket_id}{suffix}"

                flat_items.append(
                    FlatRow(
                        tiket_id=tiket_id,
                        agent_id=h["agent_id"],
                        status=h["status"],
                        created_time=h["created_time"],
                        prospect_id=h["prospect_id"],
                        a_number=rec.get("a_number"),
                        context=rec.get("context"),
                        file_path=rec.get("file_path"),
                    )
                )
        else:
            # tidak ada recording: satu baris, tanpa suffix
            flat_items.append(
                FlatRow(
                    tiket_id=str(h["id"]) if h["id"] is not None else None,
                    agent_id=h["agent_id"],
                    status=h["status"],
                    created_time=h["created_time"],
                    prospect_id=h["prospect_id"],
                    a_number=None, context=None, file_path=None,
                )
            )

    return FlatResponse(count=len(flat_items), items=flat_items)
