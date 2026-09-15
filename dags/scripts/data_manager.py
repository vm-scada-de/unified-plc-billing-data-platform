from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from zoneinfo import ZoneInfo
import pymssql
import struct
import json

def msk_00_now():
    """Returns the current Moscow timezone datetime truncated precisely to the hour boundary."""
    return datetime.now(ZoneInfo('Europe/Moscow')).replace(minute=0, second=0, microsecond=0)

def json_to_dict(value) -> dict:
    """
    Safely converts dynamic JSON string data inputs into clean Python dict representations.
    """
    if isinstance(value, dict):
        return value
    try:
        return json.loads(value) if value else {}
    except Exception as e:
        raise ValueError(f"Critical JSON processing error! Source payload: {value}. Details: {e}")

def decode_bytes(raw_data: bytes, data_type: str) -> float | int:
    """
    Decodes raw industrial telemetry byte streams based on IEC specifications (TIA Portal matching).
    Enforces Big-Endian network architecture constraints native to industrial programmable logic controllers.
    """
    if not raw_data:
        return 0.0

    dt = data_type.upper()

    try:
        if dt == "REAL":
            # 32-bit floating-point variable representation -> Maps to Python Float structures
            value = struct.unpack(">f", raw_data[:4])[0]
            return round(float(value), 2)
            
        elif dt in ("DINT", "INT32"):
            # 32-bit signed double-word integer parameters
            return struct.unpack(">i", raw_data[:4])[0]
            
        elif dt in ("DWORD", "UDINT"):
            # 32-bit unsigned double-word accumulation parameters (handles rolling counters over 2 billion)
            return struct.unpack(">I", raw_data[:4])[0]
            
        elif dt == "INT":
            # 16-bit standard signed integer indicators
            return struct.unpack(">h", raw_data[:2])[0]
            
        elif dt in ("WORD", "UINT"):
            # 16-bit unsigned discrete industrial status words
            return struct.unpack(">H", raw_data[:2])[0]
            
        else:
            print(f"⚠️ [DM-DECODE] Fallback constraint applied: Unknown type protocol '{data_type}'. Assuming DINT.")
            return int.from_bytes(raw_data, byteorder='big', signed=True)
            
    except Exception as e:
        print(f"❌ [DM-DECODE] Structural parsing failure for industrial data block format {dt}: {e}")
        return 0.0

def pgHook_getTableColumns(conn_id: str, table_name: str) -> list:
    """
    Queries relational system schemas dynamically to map structural column indices in target tables.
    Returns physical property lists ordered by exact database column layout constraints.
    """
    query = """
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.columns 
        WHERE TABLE_SCHEMA = 'public' AND TABLE_NAME = %s
        ORDER BY ordinal_position;
    """
    records = pgHook_getRecords(conn_id, query, (table_name,))
    return [row[0] for row in records] if records else []

def pgHook_getRecords(conn_id, query, param):
    """Executes an isolated query against PostgreSQL using Airflow connections."""
    return PostgresHook(postgres_conn_id=conn_id).get_records(sql=query, parameters=param)

def pgHook_insertRows(conn_id, table, rows, target_fields):
    """
    Performs transactional multi-row batch injection routines via PostgresHook core layers.
    """
    return PostgresHook(postgres_conn_id=conn_id).insert_rows(
        table=table, 
        rows=rows, 
        target_fields=target_fields
    )

def execute_mssql_query(Host, User, Password, Schema, Query):
    """
    Opens an independent gateway session to target vendor MSSQL database engine nodes.
    """
    conn = pymssql.connect(
        server=Host, 
        user=User, 
        password=Password, 
        database=Schema,
        tds_version="7.0"
    )
    try:
        cursor = conn.cursor()
        cursor.execute(Query)
        result = cursor.fetchall()
        return result
    except Exception as e:
        print(f"Operational session error across legacy Vendor MSSQL interface architecture:\n{e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
