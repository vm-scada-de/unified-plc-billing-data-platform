import scripts.data_manager as DM
import scripts.mp_model as MPM
from datetime import datetime
from zoneinfo import ZoneInfo

ID_SOURCE = 2
CONNECTION_ROW_ID = 2

# UTC
def weekly_data():
    # 1. Retrieving the connection reference data via `namedtuple`
    all_conns = MPM.connections()
    if CONNECTION_ROW_ID not in all_conns:
        raise ValueError(f"Connection with ID={CONNECTION_ROW_ID} not found!")
        
    # 2. Retrieving configuration based on property name
    db_config = DM.json_to_dict(all_conns[CONNECTION_ROW_ID].config)

    # 3. Collect metering point IDs
    points_dict = MPM.points(id_connection_type=ID_SOURCE)
    in_uin = tuple([str(k.id_mp_src) for k in points_dict.keys()])
    if not in_uin:
        return []

    sql = f"""
        SELECT uin, dt, value
        FROM rg_cnt_value 
        WHERE 
            dt >= DATEADD(DAY, -7, GETDATE()) AND
            dt >= '2026-09-01 00:00:00' AND
            DATEPART(MINUTE, dt) = 0 AND 
            DATEPART(SECOND, dt) = 0 AND 
            uin IN {in_uin};
    """
    return DM.execute_mssql_query(
        db_config['host'],
        db_config['user'],
        db_config['password'],
        db_config['database'],  
        sql                     
    )

def transform(data: list):
    print('[---TRANSFORM VENDOR_B---]')
    if not data:
        return []
    
    # Building the Mapping Dictionary Based on Metering Points
    mpoints = {str(k.id_mp_src): (k.id,) for k in MPM.points(id_connection_type=ID_SOURCE).keys()}
    
    # Returning a clean array for TARGET_COLUMNS = ['dt', 'id_mp', 'value']
    return [
        [
            d[1].astimezone(ZoneInfo("Europe/Moscow")),    # 1. Timestamp (dt)
            mpoints[str(d[0])][0],                         # 2. Global Station ID (id_mp)
            MPM.value_for_jsonb(0, float(d[2]), 0.0)       # 3. Formatted JSONB structure
        ] 
        for d in data
    ]
