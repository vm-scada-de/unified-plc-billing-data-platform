import time
import json
import threading
import snap7
import clickhouse_connect
from datetime import datetime

from config import CLICKHOUSE_CONFIG, ENGINE_SETTINGS, DB_AUTH
from database import get_archived_tags_config

class AsutpIndustrialArchiver:
    def __init__(self):
        # Thread-safe buffer for structural ClickHouse pipeline batch operations
        self.ch_buffer = []
        self.ch_lock = threading.Lock()
        
        # Hydrate dynamic execution configurations from relational tracking limits
        self.max_batch_size = ENGINE_SETTINGS.get("batch_max_size", 500)
        self.flush_interval = ENGINE_SETTINGS.get("batch_flush_interval", 5)
        
        # Low-latency internal mapping cache models
        self.conn_map = {}
        self.last_poll_times = {}
        
        # Activate isolated background thread to monitor chronological time-flush triggers
        threading.Thread(
            target=self._clickhouse_flush_loop, 
            daemon=True, 
            name="CH-Flush-Worker"
        ).start()

    @staticmethod
    def parse_wincc_address(raw_address: str) -> tuple[str, str, int, str, int] | None:
        """
        Parses legacy SCADA layout configuration values. Removes noise parameters 
        and extracts physical PLC maps: (area_letter, data_type, start_byte, conn_name, db_num)
        """
        try:
            if not raw_address:
                return None
            
            s = str(raw_address).strip().replace('"', '').replace("'", "")
            
            # Clean up Siemens industrial legacy trailing artifacts (e.g., ', "", 4')
            if s.count(',') >= 2:
                parts = s.split(',')
                for p in parts:
                    if '[' in p and ']' in p:
                        idx_part = parts.index(p)
                        if idx_part + 1 < len(parts) and any(m in parts[idx_part+1] for m in ('REAL', 'INT', 'WORD', 'DWORD', 'BYTE')):
                            s = f"{p},{parts[idx_part+1]}"
                        else:
                            s = p
                        break

            if '[' not in s or ']' not in s:
                return None
                
            idx_start = s.find('[')
            idx_end = s.find(']')
            
            conn_name = s[idx_start + 1:idx_end].strip()
            tail = s[idx_end + 1:].strip().upper()

            db_num = 0
            if 'DB' in tail:
                area_letter = 'D'
                db_str = ''
                start_collect = False
                for char in tail:
                    if char == 'B' and not start_collect:
                        start_collect = True
                        continue
                    if start_collect:
                        if char.isdigit():
                            db_str += char
                        else:
                            break
                db_num = int(db_str) if db_str else 0
                
                if ',' in tail:
                    mem_address = tail.split(',')[-1].strip()
                else:
                    mem_address = tail[tail.find(db_str) + len(db_str):].strip()
            
            elif 'M' in tail:
                area_letter = 'M'
                m_idx = tail.find('M')
                mem_address = tail[m_idx:].strip()
            else:
                return None
                
            if not mem_address:
                return None
                
            data_type = ''.join([c for c in mem_address if c.isalpha()]).upper()
            if area_letter == 'M' and data_type.startswith('M'):
                data_type = data_type[1:]
                
            start_byte = int(''.join([c for c in mem_address if c.isdigit()]))
            
            return area_letter, data_type, start_byte, conn_name, db_num
        except Exception:
            return None

    def _clickhouse_flush_loop(self):
        """Asynchronous system ticker forcing buffer evaluation cycle ticks."""
        while True:
            time.sleep(self.flush_interval)
            self.flush_to_clickhouse()

    def flush_to_clickhouse(self):
        """Pushes accumulated real-time tracking points directly into ClickHouse analytical layers."""
        batch_to_send = []
        with self.ch_lock:
            if not self.ch_buffer:
                return
            batch_to_send = self.ch_buffer
            self.ch_buffer = []
            
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG.get("host", "127.0.0.1"),
                port=int(CLICKHOUSE_CONFIG.get("port", 8123)),
                username=CLICKHOUSE_CONFIG.get("username", "default"),
                password=CLICKHOUSE_CONFIG.get("password", "")
            )
            
            # Map parameters matching column orders inside targets_realtime_archive tables
            client.insert(
                'tags_realtime_archive', 
                batch_to_send, 
                column_names=['value_id', 'time_stmp', 'val', 'quality']
            )
            print(f"[CLICKHOUSE-STREAM] Successfully committed batch operation allocation of {len(batch_to_send)} logs.")
            client.close()
        except Exception as e:
            print(f"[CLICKHOUSE-ERROR] Pipeline write execution failure intercepted: {e}")
            # Fallback strategy implementation: return datasets back to cache sequence blocks
            with self.ch_lock:
                self.ch_buffer = batch_to_send + self.ch_buffer

    def load_connections_map(self):
        """Pre-allocates remote controller routing properties from relational configurations."""
        import psycopg2
        from psycopg2.extras import RealDictCursor
        new_map = {}
        try:
            conn = psycopg2.connect(DB_AUTH)
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT name, config FROM public.connections;")
                rows = cur.fetchall()
                for row in rows:
                    if not row or not row['name']: 
                        continue
                    cfg = row['config']
                    if isinstance(cfg, str):
                        try: 
                            cfg = json.loads(cfg)
                        except Exception: 
                            continue
                    if isinstance(cfg, dict) and cfg.get("ip"):
                        # Enforce standard lowercase matching keys to minimize processing gaps
                        new_map[str(row['name']).strip().lower()] = cfg
            conn.close()
            self.conn_map = new_map
        except Exception as e:
            print(f"[DATABASE-ERROR] Core gateway engine failed to refresh hardware network topologies: {e}")
            
            
    def run_archiver_loop(self):
        """Continuous execution driver processing low-latency tags polling and cache management profiles."""
        print("[SERVICE-CORE] Activating AsutpIndustrialArchiver telemetry matrix engine...")
        self.load_connections_map()
        
        # High-performance local cache holding persistent controller client session hooks
        plc_clients = {} 
        
        while True:
            try:
                # Refresh dynamic registries where active tracking tags are set
                tags = get_archived_tags_config()
                if not tags:
                    time.sleep(2)
                    continue
                    
                current_now = time.time()
                
                for tag in tags:
                    tag_id = tag["id"]
                    raw_addr = tag["raw_address"]
                    interval = tag["interval_sec"]
                    
                    # Track scheduling frames across individual monitoring channels
                    last_poll = self.last_poll_times.get(tag_id, 0)
                    if current_now - last_poll < interval:
                        continue
                        
                    self.last_poll_times[tag_id] = current_now
                    
                    parsed = self.parse_wincc_address(raw_addr)
                    if not parsed: 
                        continue
                    
                    area, data_type, start, conn_name, db_num = parsed
                    plc_cfg = self.conn_map.get(conn_name.lower())
                    if not plc_cfg: 
                        continue
                    
                    ip = plc_cfg.get("ip")
                    rack = int(plc_cfg.get("rack", 0))
                    slot = int(plc_cfg.get("slot", 1))
                    
                    client = plc_clients.get(ip)
                    if not client:
                        client = snap7.client.Client()
                        plc_clients[ip] = client
                        
                    if not client.get_connected():
                        try: 
                            client.connect(ip, rack, slot)
                        except Exception: 
                            continue
                        
                    # Extract active binary data blocks from targeted execution levels
                    try:
                        quality = 192  # OPC-Standard definition: 192 represents Good quality parameters
                        val = 0.0
                        
                        if area == 'D':
                            # Process industrial data block memory targets
                            if data_type == 'REAL':
                                data = client.db_read(db_num, start, 4)
                                val = snap7.util.get_real(data, 0)
                            elif data_type in ('INT', 'WORD'):
                                data = client.db_read(db_num, start, 2)
                                val = snap7.util.get_int(data, 0)
                        elif area == 'M':
                            # Process discrete register internal diagnostic flags
                            if data_type == 'REAL':
                                data = client.mb_read(start, 4)
                                val = snap7.util.get_real(data, 0)
                            elif data_type in ('INT', 'WORD'):
                                data = client.mb_read(start, 2)
                                val = snap7.util.get_int(data, 0)

                    except Exception as read_err:
                        print(f"[SNAP7-READ-ERROR] Sampling channel failure on tag allocation {tag_id}. Details: {read_err}")
                        quality = 0  # OPC-Standard definition: 0 represents Bad/Failure parameters
                        val = 0.0

                    # Standardize structural datasets with millisecond timestamp markers
                    timestamp_now = datetime.now()

                    with self.ch_lock:
                        self.ch_buffer.append((
                            int(tag_id),
                            timestamp_now,
                            float(val),
                            int(quality)
                        ))

                        # Enforce instant flush operations if storage volume breaks configured ceilings
                        if len(self.ch_buffer) >= self.max_batch_size:
                            threading.Thread(target=self.flush_to_clickhouse, daemon=True).start()

            except Exception as loop_err:
                print(f"[ENGINE-CRITICAL] Main telemetry engine loop caught an unhandled exception state: {loop_err}")
            
            time.sleep(0.05)  # Internal thread brake protecting execution nodes from high CPU loads

if __name__ == "__main__":
    archiver = AsutpIndustrialArchiver()
    archiver.run_archiver_loop()
