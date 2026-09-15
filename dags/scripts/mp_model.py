import scripts.data_manager as DM
from collections import namedtuple
import json

CONN_ID = 'asutp'                  # Global target relational tracking identifier within Airflow
NAME_TABLE_MP = 'mp'
TARGET_COLUMNS = ['dt', 'id_mp', 'value']
TABLE_MP_HOURLY_VALUES = 'mp_hourly_values'

# 🌟 AUTOMATED INFERENCE ENGINE: Extracting structural tracking keys dynamically from disk configurations
db_cols = DM.pgHook_getTableColumns(CONN_ID, NAME_TABLE_MP) or []

class MPFields:
    """
    Intelligent schema discovery engine.
    Resolves variations in structural layout property strings dynamically.
    """
    ID            = 'id' if 'id' in db_cols else (db_cols[0] if db_cols else 'id')
    NAME          = 'name' if 'name' in db_cols else 'name'
    CONFIG        = 'config' if 'config' in db_cols else 'config'
    IS_ACTIVE     = 'is_active' if 'is_active' in db_cols else 'is_active'
    SEND_EMAIL    = 'send_to_email' if 'send_to_email' in db_cols else 'send_to_email'
    
    ID_CONNECTION = 'id_connection' if 'id_connection' in db_cols else \
                    ([c for c in db_cols if 'conn' in c or 'link' in c] or ['id_connection'])[0]
                    
    ID_RES_TYPE   = 'id_resource_type' if 'id_resource_type' in db_cols else \
                    ([c for c in db_cols if 'res' in c or 'resource' in c] or ['id_resource_type'])[0]
                    
    ID_OBJECT     = 'id_object' if 'id_object' in db_cols else \
                    ([c for c in db_cols if 'obj' in c or 'station' in c] or ['id_object'])[0]
                    
    ID_MP_SRC     = 'id_mp_src' if 'id_mp_src' in db_cols else \
                    ([c for c in db_cols if 'src' in c or 'ext' in c or 'source' in c] or ['id_mp_src'])[0]


# 👑 Universal Data Engineering Passport Tuples
DBKey        = namedtuple('DBKey', 'id id_mp_src')
PlcS7Key     = namedtuple('PlcS7Key', 'id ip port rack slot')
ModbusTcpKey = namedtuple('ModbusTcpKey', 'id ip port slave_id')

def value_for_jsonb(code_err: int, value_plus: float, value_minus: float) -> str:
    """Wraps localized industrial dataset records into structured warehouse JSONB entities."""
    return json.dumps({"code_err": code_err, "value_plus": value_plus, "value_minus": value_minus})

def connections() -> dict:
    """Extracts systemic network channel registries from targeted operational parameters."""
    Connection = namedtuple('Connection', ['id_connection_type', 'name', 'config'])

    query = "SELECT id, id_connection_type, name, config FROM public.connections;"
    records = DM.pgHook_getRecords(CONN_ID, query, None)

    return {col[0]: Connection(*col[1:]) for col in records} if records else {}

def points(id_connection_type: int | None = None, point_ids: list | None = None, return_all: bool = False) -> dict:
    """
    Generates structured physical passport allocations for tracking endpoints.
    Network configuration constraints are cleanly separated into typed NamedTuple keys.
    Memory register profiles (mp_config) are guaranteed to be positioned at the trailing sequence index [-1].
    """
    param = None
    add_string_query = ""

    if not return_all:
        if id_connection_type is not None:
            param = (id_connection_type,)
            add_string_query = f" AND c.id_connection_type = %s"
        elif point_ids is not None:
            param = point_ids
            add_string_query = f" AND m.{MPFields.ID} IN ({', '.join(['%s'] * len(point_ids))})"

    query = f"""
        SELECT 
            m.{MPFields.ID}, 
            m.{MPFields.ID_CONNECTION}, 
            c.id_connection_type, 
            m.{MPFields.ID_RES_TYPE}, 
            m.{MPFields.ID_OBJECT}, 
            m.{MPFields.IS_ACTIVE}, 
            m.{MPFields.SEND_EMAIL}, 
            m.{MPFields.ID_MP_SRC}, 
            m.{MPFields.NAME},
            COALESCE(m.{MPFields.CONFIG}, jsonb_build_object())::text AS mp_config,
            (CASE 
                WHEN c.id_connection_type > 2 THEN COALESCE(c.config, jsonb_build_object())
                ELSE jsonb_build_object()
            END)::text AS conn_config
        FROM {NAME_TABLE_MP} m
        INNER JOIN connections c ON m.{MPFields.ID_CONNECTION} = c.id
        WHERE m.{MPFields.IS_ACTIVE}='TRUE'{add_string_query};
    """
    
    records = DM.pgHook_getRecords(CONN_ID, query, param)
    if not records:
        return {}

    fields = [
        MPFields.ID, MPFields.ID_CONNECTION, 'id_connection_type', MPFields.ID_RES_TYPE, 
        MPFields.ID_OBJECT, MPFields.IS_ACTIVE, MPFields.SEND_EMAIL, MPFields.ID_MP_SRC, MPFields.NAME
    ]

    idx_id        = fields.index(MPFields.ID)
    idx_conn_type = fields.index('id_connection_type')
    idx_src       = fields.index(MPFields.ID_MP_SRC)
    
    prepared_dict = {}
    
    for col in records:
        point_id = col[idx_id]          
        conn_type = col[idx_conn_type]  
        id_mp_src = col[idx_src]        
        
        mp_config_dict = json.loads(col[-2]) if col[-2] else {}
        conn_config_dict = json.loads(col[-1]) if col[-1] else {}
        
        # --- GENERATING ENFORCED TYPED SMART IDENTITY PASSES ---
        if conn_type in (1, 2):
            #1 - Vendor A: PostgreSQL Database connection profile
            #2 - Vendor B: Microsoft SQL Server connection profile
            smart_key = DBKey(id=point_id, id_mp_src=id_mp_src)
            
        elif conn_type == 3:
            # Industrial Ethernet: Siemens S7 Communication Protocol via ISO-on-TCP
            smart_key = PlcS7Key(
                id=point_id,
                ip=conn_config_dict.get("ip", "unknown_ip"),
                port=conn_config_dict.get("port", 102),
                rack=mp_config_dict.get("rack", conn_config_dict.get("rack", 0)),
                slot=mp_config_dict.get("slot", conn_config_dict.get("slot", 1))
            )
        elif conn_type == 4:
            # Industrial Telemetry: Modbus TCP Protocol
            smart_key = ModbusTcpKey(
                id=point_id,
                ip=conn_config_dict.get("ip", "unknown_ip"),
                port=conn_config_dict.get("port", 502),
                slave_id=mp_config_dict.get("slave_id", conn_config_dict.get("slave_id", 1))
            )
            
        # Isolate system properties: drop ID prefixes and remove secondary parameters from end positions.
        # This keeps the essential configuration maps locked inside index location [-1].
        prepared_dict[smart_key] = tuple(col[1:-1])
        
    return prepared_dict

def transform(data: list) -> list:
    """
    Universal transformation adapter engine for plc_s7 and mb_tcp pipelines.
    Extracts chronological window benchmarks automatically from the target execution environment.
    """
    if not data:
        return []

    dt_hour = DM.msk_00_now()
    return [
        (
            dt_hour, 
            d["id"], 
            value_for_jsonb(d["code_err"], d["value_plus"], d["value_minus"])
        ) 
        for d in data
    ]

def load(data: list, name_conn_id: str, name_table: str, list_columns: list, list_replace_index: list):
    """
    Unified high-performance data loader abstracting transactions via Airflow PostgresHook layers.
    """
    if not data:
        print("⚠️ [MP-LOAD] Target payload array contains no data elements. Transaction aborted.")
        return

    tgt_hook = DM.PostgresHook(postgres_conn_id=name_conn_id)
    tgt_hook.insert_rows(
        table=name_table, 
        rows=data, 
        target_fields=list_columns, 
        replace=True, 
        replace_index=list_replace_index
    )
    print(f"✅ [MP-LOAD] Stream batch containing {len(data)} rows successfully committed into table {name_table} via PostgresHook.")
