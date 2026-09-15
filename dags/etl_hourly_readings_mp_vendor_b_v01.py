from datetime import datetime, timedelta
from airflow.sdk import dag, task
import scripts.vendor_b as vendor_b
import scripts.mp_model as MPM

# ==============================================================================
# 🚀 STAGING PIPELINE: VENDOR B DATA LOGGER INTERFACE (MSSQL INGESTION)
# ==============================================================================
TARGET_CONN_ID = MPM.CONN_ID
TARGET_TABLE = MPM.TABLE_MP_HOURLY_VALUES
TARGET_COLUMNS = MPM.TARGET_COLUMNS

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='etl_hourly_readings_mp_vendor_b_v01',
    default_args=default_args,
    start_date=datetime(2026, 8, 31),
    catchup=False,
    schedule='5 21 * * 0', # Weekly billing tracking loop
    tags=['mp_model', 'vendor_b', 'hourly', 'v01']
)
def my_vendor_b_taskflow_dag():
    
    # 1. 📥 EXTRACT: Querying external Vendor B logging arrays via MSSQL
    @task()
    def extract():
        print('🟢 [EXTRACT] Pulling binary table sequences from Vendor B database server...')
        return vendor_b.weekly_data()
        
    # 2. ⚙️ TRANSFORM: Realigning variable matrix blocks into standard payload profiles
    @task()
    def transform(data: list):
        print('⚙️ [TRANSFORM] Processing Vendor B structures into compliant relational logs...')
        return vendor_b.transform(data)
                 
    # 3. 📥 LOAD: Commit polished records into target operational cluster arrays
    @task()
    def load(transformed_data: list):
        if transformed_data:
            print(f'📥 [LOAD] Bulk inserting batch into target table: {TARGET_TABLE}...')
            MPM.load(
                transformed_data, 
                TARGET_CONN_ID, 
                TARGET_TABLE, 
                TARGET_COLUMNS, 
                [col for col in TARGET_COLUMNS if col != 'value']
            )

    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load(transformed_data)

mp_vendor_b_hourly_v01_dag = my_vendor_b_taskflow_dag()
