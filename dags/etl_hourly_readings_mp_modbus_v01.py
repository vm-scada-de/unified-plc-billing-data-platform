from airflow.sdk import dag, task
from datetime import datetime, timedelta
import scripts.mp_model as MPM
import scripts.mb_tcp as MB_TCP

# ==============================================================================
# 🚀 STAGING PIPELINE: MODBUS TCP (SUR-97) VIA MP_MODEL CORE (VERSION 01)
# ==============================================================================
TARGET_CONN_ID = MPM.CONN_ID                # Target Water Utility DBMS Connection ID
TARGET_TABLE   = MPM.TABLE_MP_HOURLY_VALUES   # Staging table for data ingestion testing
TARGET_COLUMNS = MPM.TARGET_COLUMNS         # Unified database column mappings

default_args = {
    'owner': 'airflow_de',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='etl_hourly_readings_mp_modbus_v01', # Unified Enterprise DAG Identifier
    default_args=default_args,
    start_date=datetime(2026, 8, 31, 21, 0, 0), # Placed in past to enable immediate manual run
    catchup=False,                              # Prevention from backfilling historical intervals
    schedule='0 * * * *',                       # Triggered exactly at 00 minutes every hour
    tags=['mp_model', 'modbus', 'hourly', 'v01']
)
def my_mp_modbus_hourly_dag():
    
    # 1. 📥 EXTRACT: Querying SUR-97 registers using isolated object definitions
    @task()
    def extract():
        print('🟢 [EXTRACT] Gathering SUR-97 metrics using object-mapped mp_model definitions...')
        return MB_TCP.read_all_mp()

    # 2. ⚙️ TRANSFORM: Formatting telemetry values into compliant JSONB structures
    @task()
    def transform(data: list): 
        print('⚙️ [TRANSFORM] Formatting collected values into JSONB payloads via MPM.transform...')
        return MPM.transform(data)
         
    # 3. 📥 LOAD: Batch insertion using PostgresHook mechanics
    @task()
    def load(transformed_data: list):
        if transformed_data: 
            print(f'📥 [LOAD] Bulk inserting batch into target table: {TARGET_TABLE}...')
            
            # Universal data layer storage abstraction
            MPM.load(
                transformed_data, 
                TARGET_CONN_ID, 
                TARGET_TABLE, 
                TARGET_COLUMNS, 
                [col for col in TARGET_COLUMNS if col != 'value'] # Conflict keys for UPSERT: ['dt', 'id_mp']
            )
 
    # 4. Defining the execution graph dependency chain
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)

# Initializing the staging Modbus TCP DAG instance
mp_modbus_hourly_v01_dag = my_mp_modbus_hourly_dag()
