from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import os
import zipfile

# Path inside the Docker container
BASE_PATH = "/opt/airflow/datalake/raw"

def download_accidents_data(**kwargs):
    dataset_id = "53698f4ca3a729239d2036df" # The main ID for Accidents
    api_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
    year_target = "2023"
    
    save_dir = os.path.join(BASE_PATH, "gouv_accidents", year_target)
    os.makedirs(save_dir, exist_ok=True)
    print(f"--- SEARCHING FOR {year_target} DATA ---")

    # 1. Get the list of ALL files
    r = requests.get(api_url)
    if r.status_code != 200:
        raise Exception(f"API Error: {r.status_code}")
    
    resources = r.json().get('resources', [])
    
    found_count = 0
    
    # 2. Iterate and Match Keywords
    # We look for ANY file that has '2023' and matches our keywords
    for res in resources:
        title = (res.get('title') or "").lower()
        url = res.get('url')
        fmt = (res.get('format') or "").lower()
        
        # Check if it is a 2023 file
        if year_target in title or year_target in url:
            
            # Identify the type based on keywords
            target_name = None
            if "carac" in title or "carac" in url:
                target_name = "caracteristiques.csv"
            elif "lieu" in title or "lieu" in url:
                target_name = "lieux.csv"
            elif "veh" in title or "veh" in url:
                target_name = "vehicules.csv"
            elif "usager" in title or "usager" in url:
                target_name = "usagers.csv"
            
            # If we identified it AND it is a CSV
            if target_name and ("csv" in fmt or url.endswith(".csv")):
                print(f"Found {target_name} at: {url}")
                
                try:
                    file_content = requests.get(url).content
                    save_path = os.path.join(save_dir, target_name)
                    with open(save_path, 'wb') as f:
                        f.write(file_content)
                    found_count += 1
                except Exception as e:
                    print(f"Error downloading {target_name}: {e}")

    if found_count < 4:
        print(f"WARNING: Only found {found_count}/4 files. Check logs above.")
    else:
        print("SUCCESS: All 4 accident files downloaded.")

def download_fuel_data(**kwargs):
    url = "https://donnees.roulez-eco.fr/opendata/instantane"
    today_str = datetime.now().strftime("%Y-%m-%d")
    save_dir = os.path.join(BASE_PATH, "gouv_fuel", today_str)
    os.makedirs(save_dir, exist_ok=True)

    r = requests.get(url)
    if r.status_code == 200:
        zip_path = os.path.join(save_dir, "data.zip")
        with open(zip_path, 'wb') as f:
            f.write(r.content)
        with zipfile.ZipFile(zip_path, 'r') as z:
            z.extractall(save_dir)
        print("Fuel Data Success")

default_args = {
    'owner': 'student',
    'start_date': datetime(2023, 1, 1),
    'retries': 0
}

with DAG('01_ingestion_pipeline',
         default_args=default_args,
         schedule_interval='@once',
         catchup=False) as dag:

    t1 = PythonOperator(task_id='download_accidents', python_callable=download_accidents_data)
    t2 = PythonOperator(task_id='download_fuel', python_callable=download_fuel_data)
    
    [t1, t2]