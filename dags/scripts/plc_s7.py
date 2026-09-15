import scripts.data_manager as DM
import scripts.mp_model as MPM 
from datetime import datetime
from zoneinfo import ZoneInfo
import time
from typing import Dict, Any
import struct
import json
import snap7
from snap7.util import get_dint

ID_SOURCE = 3

def read_all_mp() -> list:
    """
    Industrial processing architecture: Aggregates active Siemens S7 assets from system schemas,
    groups targets logically by controller channel IDs, and reads data via shared Snap7 sessions.
    """
    active_points = MPM.points(id_connection_type=ID_SOURCE)
    
    if not active_points:
        print("⚠️ [SIEMENS] Active parameter entries absent for target Siemens S7 hardware profiles.")
        return []

    # 🧠 CLUSTER POINTS BY UNIQUE LOGICAL CONNECTION SCHEMAS
    # Target distribution schema: { id_connection: {"conn_tuple": (...), "points": [...] } }
    grouped_by_id_conn = {}
    
    for smart_key, tuple_data in active_points.items():
        id_connection = tuple_data[0]
        
        # Access physical hardware layer constraints natively via Passport Tuple components
        conn_tuple = (smart_key.ip, smart_key.port, smart_key.rack, smart_key.slot)
        
        mp_config = json.loads(tuple_data[-1]) if tuple_data[-1] else {}
        
        # Construct hardware register boundaries for single-session batch polling
        point_entry = {
            "id": smart_key.id,
            "area_type": mp_config.get("area_type", "DB"),
            "db_number": mp_config.get("db_number", 0),
            "start_byte": mp_config.get("start_byte", 0),
            "size": mp_config.get("size", 4),
            "data_type": mp_config.get("data_type", "DINT").upper() # Defaults to signed integer DINT structures
        }
        
        if id_connection not in grouped_by_id_conn:
            grouped_by_id_conn[id_connection] = {
                "conn_tuple": conn_tuple,
                "points": []
            }
            
        grouped_by_id_conn[id_connection]["points"].append(point_entry)

    # 🏁 DUAL-STAGE DRIVER PROCESSING ARCHITECTURE
    result = []
    
    # MASTER TRACKING STAGE: Step through distinct PLC networking paths resolved from backend schemas
    for id_conn, conn_data in grouped_by_id_conn.items():
        conn_tuple = conn_data["conn_tuple"]
        lst_points = conn_data["points"]
        
        print(f"🟢 [PLC INSTANCE ID {id_conn}] Establishing active communication socket layer via parameters: {conn_tuple}...")
        print(f"   Target physical endpoint manages an isolated array of {len(lst_points)} monitoring stations.")
        
        # SUB-ROUTINE EXECUTION STAGE: Batch fetching register fields via shared Snap7 client connection
        polled_pack = read_plc_s7_device_data(conn_tuple, lst_points)
        
        result.extend(polled_pack)
        print(f"   📦 Telemetry processing complete for PLC ID {id_conn}. Actively tracked tags: {len(polled_pack)}")
        print("-" * 75)
        
    return result

def read_plc_s7_device_data(conn: tuple, device_list):
    """
    Executes high-performance block reading across Siemens S7 target data fields.
    
    :param conn: Connection constraint layout coordinates (IP, Port, Rack, Slot).
    :param device_list: Memory configuration map containing targeted register boundary points.
    :return: List of structured telemetry profiles containing parsed real values or error indicators.
    """
    result = []

    plc = snap7.client.Client()
    ip = conn[0]
    port = conn[1]
    rack = conn[2]
    slot = conn[3]
    
    try:
        # LEVEL 1 ARCHITECTURE: Socket initialization sequence on the hardware interface layer
        plc.connect(address=ip, rack=rack, slot=slot)
        
        for device in device_list:
            err, value = 0, 0.0
            area_type = device.get('area_type', 'M').upper()
            start_byte = device.get('start_byte', 0)
            db_number = device.get('db_number', 0)
            size = device.get('size', 4)  
            device_data_type = device.get("data_type", "INT")

            # LEVEL 2 ARCHITECTURE: Safe memory retrieval routines for specific register blocks
            try:
                if area_type == 'DB':
                    raw_data = plc.db_read(db_number, start_byte, size)
                elif area_type == 'M':
                    raw_data = plc.read_area(0x83, 0, start_byte, size)
                elif area_type == 'I':
                    raw_data = plc.read_area(0x81, 0, start_byte, size)
                elif area_type == 'Q':
                    raw_data = plc.read_area(0x82, 0, start_byte, size)
                else:
                    print(f" Industrial memory area parameter exception: Unknown type allocation '{area_type}'.")
                    err = 2
                    continue
                    
                if device_data_type == "REAL":
                    value = struct.unpack(">f", raw_data[:4])[0]
                    value = round(value, 1)
                else:    
                    # Process byte conversion to standard industrial signed integer (Big-Endian format)
                    value = int.from_bytes(raw_data, byteorder='big', signed=True)
                
            except Exception as tag_error:
                err = 1
                print(f" Target register allocation block missing or unmapped at memory boundary {area_type}{start_byte} on host {ip}: {tag_error}")

            result.append({
                "id": device['id'],
                "code_err": err,
                "value_plus": value,
                "value_minus": 0.0
            })

    except Exception as conn_error:
        print(f" КРИТИЧЕСКАЯ ОШИБКА: Interface layer runtime exception. Host connection dropped on device {ip}: {conn_error}")
        
    finally:
        if plc.get_connected():
            plc.disconnect()
            
    return result
