from pyModbusTCP.client import ModbusClient
import scripts.mp_model as MPM

ID_SOURCE = 4

ADDRESS = 0x1000       # Target Modbus start address allocation equivalent to 4096 base 10
REGISTERS_COUNT = 10   # Block register reading limits

def get_counter_value(high: int, low: int, k=1):
    """Computes target byte transformations into decimal telemetry parameters."""
    combined_digits = f"{high:04x}{low:04x}"
    return round(int(combined_digits) * 10**(-k), 1)

def read_all_mp() -> list:
    """
    Core Modbus processing driver: Gathers operational parameters from active Modbus TCP registers.
    Sequentially steps through targeted field passports extracted from global model specifications.
    """
    result = []
    
    # Query current structural network models for active Modbus targets (source type 4)
    active_points = MPM.points(id_connection_type=ID_SOURCE)
    
    if not active_points:
        print("⚠️ [MODBUS-TCP] Target parameters absent. Query aborted for active Modbus TCP devices.")
        return []

    print(f"🟠 [MODBUS-TCP] Found {len(active_points)} active targets inside mp_model schemas. Triggering loop profile...")
    print("=" * 85)

    for smart_key in active_points.keys():
        point_id = smart_key.id
        ip = smart_key.ip
        port = smart_key.port
        slave_id = smart_key.slave_id
        
        print(f"🔌 [MODBUS-TCP ID {point_id}] Establishing interface connection on remote host endpoint {ip}:{port} (UnitID={slave_id})...")
        
        err, d45, d78 = None, None, None
        client = ModbusClient(host=ip, port=port, unit_id=slave_id, timeout=3.0)
        
        try:
            if client.open():
                # Extract input register blocks safely based on device physical profiles
                data = client.read_input_registers(ADDRESS, REGISTERS_COUNT)
            
                if data:
                    err = 0
                    # Apply certified hex alignment conversion algorithms
                    d45 = get_counter_value(data, data, data)
                    d78 = get_counter_value(data, data, data)
                    print(f"    ✅ Telemetry read success: Value(+) = {d45} | Value(-) = {d78}")
                else:
                    err = 41
                    print(f"    ❌ Field failure: Device returned empty frame data from memory registry location {ADDRESS}")
                client.close()
            else:
                err = 41
                print(f"    ❌ Network failure: Failed to resolve communication socket parameters on target port {ip}:{port}")

        except Exception as e:
            err = 42
            print(f"    🚨 Critical driver alert: Exception intercepted during interface scanning sequence: {e}")

        result.append({
            "id": point_id,
            "code_err": err,
            "value_plus": d45,
            "value_minus": d78
        })
        print("-" * 85)

    return result
