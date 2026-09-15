from datetime import datetime, timedelta
from airflow.sdk import dag, task
import scripts.mp_model as MPM
import scripts.vendor_a as vendor_a

# ==============================================================================
# 🚀 STAGING PIPELINE: VENDOR A EXTERNAL DATABASE (POSTGRESQL INGESTION)
# ==============================================================================
TARGET_CONN_ID = MPM.CONN_ID                  # Writing to the target Utility DBMS
TARGET_TABLE   = MPM.TABLE_MP_HOURLY_VALUES   # Secure staging table for isolated testing
TARGET_COLUMNS = MPM.TARGET_COLUMNS           # Unified database column mappings

default_args = {
    'owner': 'airflow_de',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='etl_hourly_readings_mp_vendor_a_v01', # Unified Enterprise DAG Identifier
    default_args=default_args,
    start_date=datetime(2026, 8, 31, 21, 0, 0),  
    catchup=False,                               
    schedule='7,37 * * * *',                    # Execution window: 7th and 37th minutes
    tags=['mp_model', 'vendor_a', 'hourly', 'v01']
)
def my_mp_vendor_a_hourly_dag():
    
    # 1. 📥 EXTRACT: Fetching rolling time-window slices from Vendor A tables
    @task()
    def extract():
        print('🟢 [EXTRACT] Fetching rolling window slices from Vendor A relational tables...')
        return vendor_a.get_last_readings()

    # 2. ⚙️ TRANSFORM: Mapping external parameters to unified mp_model fields
    @task()
    def transform(data: list): 
        print('⚙️ [TRANSFORM] Packing raw Vendor A readings via vendor_a.transform...')
        return vendor_a.transform(data)
         
    # 3. 📥 LOAD: High-performance upsert logic into the core warehouse
    @task()
    def load(transformed_data: list):
        if transformed_data: 
            print(f'📥 [LOAD] Bulk inserting batch into target table: {TARGET_TABLE}...')
            MPM.load(
                transformed_data, 
                TARGET_CONN_ID, 
                TARGET_TABLE, 
                TARGET_COLUMNS, 
                [col for col in TARGET_COLUMNS if col != 'value'] # Conflict keys: ['dt', 'id_mp']
            )
 
    # 4. Graph dependencies
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)

mp_vendor_a_hourly_v01_dag = my_mp_vendor_a_hourly_dag()
