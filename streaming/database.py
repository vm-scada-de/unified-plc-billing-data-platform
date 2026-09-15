import psycopg2
from psycopg2.extras import RealDictCursor
from config import DB_AUTH

def get_archived_tags_config() -> list[dict]:
    """
    Fetches active tag maps configured for archiving metrics from relational schemas.
    Evaluates dynamic configuration maps to calculate precise processing cycles in seconds.
    """
    archived_tags = []
    try:
        conn = psycopg2.connect(DB_AUTH)
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # High-performance structural join verifying activation targets
            cur.execute("""
                SELECT 
                    t.variableid, 
                    t.addressparameter, 
                    p.value as time_value, 
                    p.base as time_base
                FROM public.tags_wincc t
                JOIN public.pde_times p ON t.id_pde_time = p.id
                WHERE t.is_archived = true;
            """)
            rows = cur.fetchall()
            
            for row in rows:
                if not row:
                    continue
                
                # Strict dynamic boundary parsing prevention rules
                var_id = int(row['variableid'])
                raw_addr = str(row['addressparameter']).strip()
                t_value = float(row['time_value'])
                t_base = float(row['time_base'])
                
                # If target network time frame configuration is valid (> 0), process calculation.
                # Default safety fallback interval configured to 60.0 seconds if bounds fail.
                if t_base > 0:
                    interval_sec = (t_value * t_base) / 1000.0
                else:
                    interval_sec = 60.0
                    
                archived_tags.append({
                    "id": var_id,
                    "raw_address": raw_addr,
                    "interval_sec": interval_sec
                })
                
        conn.close()
    except Exception as e:
        print(f"[DATABASE-ERROR] Ingestion engine failed to compile active tag tracking configurations: {e}")
        
    return archived_tags
