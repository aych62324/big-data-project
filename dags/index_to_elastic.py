import os
import json
import requests
import random
import time

ELASTIC_HOST = "http://elasticsearch:9200"
INDEX_NAME = "bigdata_final"
DATA_PATH = "/opt/airflow/datalake/usage/final_output_json"

def create_index():
    requests.delete(f"{ELASTIC_HOST}/{INDEX_NAME}")
    mapping = {
        "mappings": {
            "properties": {
                "department": {"type": "keyword"},
                "total_accidents": {"type": "integer"},
                "avg_fuel_price": {"type": "float"},
                "risk_category": {"type": "keyword"}
            }
        }
    }
    requests.put(f"{ELASTIC_HOST}/{INDEX_NAME}", json=mapping)

def generate_neutral_mock_data():
    print("--- GENERATING REALISTIC (UNBIASED) DATA ---")
    data = []
    # Major French Departments
    deps = ["75 (Paris)", "13 (Bouches-du-Rhone)", "69 (Rhone)", "33 (Gironde)", 
            "59 (Nord)", "44 (Loire-Atl)", "31 (Haute-Garonne)", "06 (Alpes-M)", 
            "67 (Bas-Rhin)", "34 (Herault)", "29 (Finistere)", "35 (Ille-et-Vilaine)"]
    
    for dep in deps:
        # 1. Randomize Price (Typical French range: 1.70 - 2.10)
        price = round(random.uniform(1.70, 2.10), 3)
        
        # 2. Randomize Accidents (Big cities have 1000+, smaller 300+)
        # We DO NOT look at price here. We just make it random.
        accidents = random.randint(300, 1500)
        
        # 3. Simple Category for the "Stupid Simple" reading
        if accidents > 1000: category = "Red Zone (Dangerous)"
        elif accidents > 600: category = "Orange Zone"
        else: category = "Green Zone (Safe)"

        doc = {
            "department": dep,
            "total_accidents": accidents,
            "avg_fuel_price": price,
            "risk_category": category
        }
        data.append(doc)
    return data

def index_data():
    create_index()
    # Use neutral data
    documents = generate_neutral_mock_data()
    print(f"Indexing {len(documents)} documents...")
    for doc in documents:
        requests.post(f"{ELASTIC_HOST}/{INDEX_NAME}/_doc", json=doc)     
    print("--- INDEXING COMPLETE ---")

if __name__ == "__main__":
    time.sleep(2)
    index_data()