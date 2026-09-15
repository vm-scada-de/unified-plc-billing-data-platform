from datetime import datetime, timedelta
from airflow.sdk import dag, task
import scripts.mp_model as MPM
import scripts.plc_s7 as plc_s7

# ==============================================================================
# 🚀 STAGING PIPELINE: SIEMENS S7 INDUSTRIAL CONTROLLERS (VERSION 01)
# ==============================================================================
TARGET_CONN_ID = MPM.CONN_ID                  # Target Water Utility DBMS Connection ID
TARGET_TABLE   = MPM.TABLE_MP_HOURLY_VALUES   # Staging table for data ingestion testing
TARGET_COLUMNS = MPM.TARGET_COLUMNS           # Unified database column mappings

default_args = {
    'owner': 'airflow_de',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='etl_hourly_readings_mp_plc7_v01',    # Unified Enterprise DAG Identifier
    default_args=default_args,
    start_date=datetime(2026, 8, 31, 21, 0, 0),
    catchup=False,
    schedule='0 * * * *',                       # Triggered exactly at 00 minutes every hour
    tags=['mp_model', 'plc7', 'hourly', 'v01']
)
def my_mp_s7_hourly_dag():
    
    # 1. 📥 EXTRACT: Grouping meter points by connection constraints before querying S7
    @task()
    def extract():
        print('🟢 [EXTRACT] Pulling Siemens S7 telemetry structured by mp_model endpoints...')
        return plc_s7.read_all_mp()

    # 2. ⚙️ TRANSFORM: Processing raw bytes into standardized relational matrices
    @task()
    def transform(data: list): 
        print('⚙️ [TRANSFORM] Packaging metrics into JSONB format via MPM.transform...')
        return MPM.transform(data)
         
    # 3. 📥 LOAD: Writing validated industrial state data into the relational layer
    @task()
    def load(transformed_data: list):
        if transformed_data: 
            print(f'📥 [LOAD] Bulk inserting batch into target table: {TARGET_TABLE}...')
            
            # Pushing datasets using unified model abstractions
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

# Initializing the staging Siemens S7 DAG instance
mp_s7_hourly_v01_dag = my_mp_s7_hourly_dag()
