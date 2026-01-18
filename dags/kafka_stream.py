from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import json
import time
import random

# WE DO NOT IMPORT KAFKA HERE (It would crash)

def stream_data():
    # WE IMPORT HERE (Inside the function, after installation runs)
    from kafka import KafkaProducer
    
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    cities = ["Paris", "Lyon", "Marseille"]
    print("Streaming 50 events to Kafka...")
    
    for i in range(50):
        data = {
            "timestamp": time.time(),
            "city": random.choice(cities),
            "alert": "Accident Reported",
            "severity": random.randint(1, 5)
        }
        print(f"Sending event: {data}") 
        producer.send('live_accidents', value=data)
        time.sleep(0.5) # Simulate realtime
        
    print("Stream Done.")

with DAG('04_KAFKA_STREAM', schedule_interval='@once', start_date=datetime(2023, 1, 1), catchup=False) as dag:
    
    # 1. Install the library first
    install = BashOperator(
        task_id='install_kafka', 
        bash_command='pip install kafka-python'
    )
    
    # 2. Then run the script
    stream = PythonOperator(
        task_id='stream_accidents', 
        python_callable=stream_data
    )
    
    install >> stream