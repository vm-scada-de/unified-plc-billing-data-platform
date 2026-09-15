from datetime import datetime
from zoneinfo import ZoneInfo
import scripts.data_manager as DM
import scripts.mp_model as MPM 

ID_SOURCE = 1
CONN_ID = 'vendor_a_conn' # Airflow Connection ID for Vendor A database

def get_last_readings():
    """
    Fetches the latest telemetry snapshots from Vendor A database cluster in UTC bounds.
    """
    query = f"""
        SELECT "DeviceId", "ArcDate", "JsonVal", "RecDate", "JsonErr"
        FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY "DeviceId" ORDER BY "ArcDate" DESC) as rn
              FROM public."ArchiveTable{datetime.now(ZoneInfo("UTC")).year}") t
        WHERE rn = 1;
    """
    return DM.pgHook_getRecords(CONN_ID, query, None)

def transform(data: list):
    """
    Maps localized Vendor A hardware keys onto global system database identifiers.
    """
    if not data:
        return []
    
    mpoints = {k.id_mp_src: (k.id,) for k in MPM.points(id_connection_type=ID_SOURCE).keys()}
    
    return [
        [
            d.astimezone(ZoneInfo("Europe/Moscow")),     # 1. Timestamp (dt)
            mpoints[str(d)],                             # 2. Global Station ID (id_mp)
            MPM.value_for_jsonb(0, float(d['val']), 0.0) # 3. Formatted JSONB structure
        ] 
        for d in data
    ]
