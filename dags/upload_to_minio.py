from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os
import boto3

# MINIO CONFIG
S3_ENDPOINT = "http://minio:9000"
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
BUCKET_NAME = "datalake"

def push_raw_to_s3():
    # Connect to MinIO
    s3 = boto3.client('s3', endpoint_url=S3_ENDPOINT, 
                      aws_access_key_id=ACCESS_KEY, 
                      aws_secret_access_key=SECRET_KEY)
    
    # Create Bucket
    try:
        s3.create_bucket(Bucket=BUCKET_NAME)
    except:
        pass

    # Walk through the local datalake/raw folder and upload everything
    local_root = "/opt/airflow/datalake/raw"
    for root, dirs, files in os.walk(local_root):
        for file in files:
            local_path = os.path.join(root, file)
            # Create S3 Key (path)
            relative_path = os.path.relpath(local_path, local_root)
            s3_key = f"raw/{relative_path}".replace("\\", "/")
            
            print(f"Uploading {file} to s3://{BUCKET_NAME}/{s3_key}")
            s3.upload_file(local_path, BUCKET_NAME, s3_key)

default_args = {'owner': 'student', 'start_date': datetime(2023, 1, 1)}

with DAG('03_MINIO_UPLOAD', schedule_interval='@once', default_args=default_args) as dag:
    # 1. Install Library
    install = BashOperator(task_id='install_boto3', bash_command='pip install boto3')
    # 2. Upload
    upload = PythonOperator(task_id='upload_to_s3', python_callable=push_raw_to_s3)
    
    install >> upload