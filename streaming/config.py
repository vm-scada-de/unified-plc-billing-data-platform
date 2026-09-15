import json
import psycopg2
from psycopg2.extras import RealDictCursor

# CORE POSTGRESQL CONNECTION STRING MAPPING
# (Central metadata parameters for the AsutpIndustrialArchiver engine environment)
DB_AUTH = "host='127.0.0.1' port=5432 dbname='db' user='user' password='password'"

def load_clickhouse_config() -> tuple[dict, dict]:
    """Dynamically retrieves ClickHouse target connection layouts and engine boundaries from sys_config."""
    # Production-ready fallback settings definitions
    ch_cfg = {"host": "127.0.0.1", "port": 8123, "username": "default", "password": ""}
    engine_cfg = {"batch_max_size": 500, "batch_flush_interval": 5}
    
    try:
        conn = psycopg2.connect(DB_AUTH)
        conn.set_client_encoding('UTF8')
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT cfg_key, cfg_value 
                FROM public.sys_config 
                WHERE cfg_key IN ('clickhouse_connections', 'archiver_engine_settings');
            """)
            rows = cur.fetchall()
            
            for row in rows:
                key = row['cfg_key']
                val = row['cfg_value']
                
                if isinstance(val, str):
                    try: 
                        val = json.loads(val)
                    except Exception: 
                        continue
                
                if key == 'clickhouse_connections':
                    ch_cfg = val
                elif key == 'archiver_engine_settings':
                    engine_cfg = val
        conn.close()
    except Exception as e:
        print(f"[CONFIG] Warning: Failed to read sys_config parameter matrices, defaults loaded. Details: {e}")
        
    return ch_cfg, engine_cfg

# Global extraction assignments available for downstream services
CLICKHOUSE_CONFIG, ENGINE_SETTINGS = load_clickhouse_config()
