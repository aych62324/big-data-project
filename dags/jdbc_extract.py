from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import psycopg2
import csv

def extract_from_postgres():
    # 1. Connect to the Source DB
    conn = psycopg2.connect(
        host="source_db", database="sourcedb", user="admin", password="admin"
    )
    cursor = conn.cursor()
    
    # 2. Create Dummy Table & Data (Simulating a Production DB)
    cursor.execute("CREATE TABLE IF NOT EXISTS sensors (id SERIAL, location TEXT, value INT)")
    cursor.execute("INSERT INTO sensors (location, value) VALUES ('Paris-Sud', 42), ('Lyon-Nord', 10)")
    conn.commit()
    
    # 3. Extract (The JDBC Part)
    print("Extracting from JDBC Source...")
    cursor.execute("SELECT * FROM sensors")
    rows = cursor.fetchall()
    
    # 4. Save to Datalake
    with open('/opt/airflow/datalake/raw/sensor_data.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'location', 'value'])
        writer.writerows(rows)
    
    print("JDBC Extraction Complete.")
    conn.close()

with DAG('05_JDBC_EXTRACT', schedule_interval='@once', start_date=datetime(2023, 1, 1)) as dag:
    extract = PythonOperator(task_id='jdbc_extract', python_callable=extract_from_postgres)