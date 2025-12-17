from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os

# --- KONFIGURASI ---
DB_CONN = "postgresql://eratani_user:eratani_pass@postgres:5432/eratani_db"
CSV_PATH = '/opt/airflow/data/agriculture_dataset.csv'

def ingest_csv_to_db():
    """
    Mission 1: Harvest the Data
    Tugas: Ingest CSV ke staging table secara idempotent & log row count.
    """
    # 1. Membaca data
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"File tidak ditemukan di {CSV_PATH}")
    
    df = pd.read_csv(CSV_PATH)
    
    # 2. Logging row count
    row_count = len(df)
    print(f"INFO: Menghitung data... Total baris yang akan di-ingest: {row_count}")

    # 3. Menghindari duplikat
    engine = create_engine(DB_CONN)
    df.to_sql('staging_agriculture', con=engine, if_exists='replace', index=False)
    
    print(f"SUCCESS: Ingestion selesai. {row_count} baris masuk ke 'staging_agriculture'.")

with DAG(
    'eratani_data_pipeline',
    default_args={
        'owner': 'eratani',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Pipeline untuk Harvest, Shape, dan Feeding Insights',
    schedule_interval='0 6 * * *', # Jalan setiap hari jam 06:00 UTC
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['eratani', 'technical_assessment'],
) as dag:

    task_ingest = PythonOperator(
        task_id='harvest_the_data',
        python_callable=ingest_csv_to_db,
    )

    task_dbt = BashOperator(
        task_id='shaping_and_feeding_insights',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir .',
    )

    task_ingest >> task_dbt