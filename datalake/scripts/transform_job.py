from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lit
import os
import xml.etree.ElementTree as ET

# Initialize Spark
spark = SparkSession.builder \
    .appName("Project_Transformation") \
    .getOrCreate()

# Paths
RAW_BASE = "/opt/spark/work-dir/datalake/raw"
FORMATTED_BASE = "/opt/spark/work-dir/datalake/formatted"

def run_job():
    print("--- STARTING SPARK JOB ---")
    
    # ==========================================
    # PART 1: ACCIDENTS (Already Working)
    # ==========================================
    try:
        print("--- Processing Accidents ---")
        acc_path = f"{RAW_BASE}/gouv_accidents/2023"
        
        df_carac = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").csv(f"{acc_path}/caracteristiques.csv")
        df_veh = spark.read.option("header", "true").option("delimiter", ";").option("inferSchema", "true").csv(f"{acc_path}/vehicules.csv")
        
        # Filter for Moto/Scooter
        two_wheelers = ["02", "30", "31", "32", "33", "34", 2, 30, 31, 32, 33, 34]
        df_moto = df_veh.filter(col("catv").isin(two_wheelers))
        
        # Join
        df_joined = df_moto.join(df_carac, "Num_Acc", "inner")
        
        # Select
        df_clean = df_joined.select(
            col("Num_Acc"),
            col("dep").alias("department"),
            col("mois").alias("month"),
            col("lum").alias("lighting"),
            col("atm").alias("weather")
        )
        
        # Save Accidents
        out_acc = f"{FORMATTED_BASE}/accidents_clean"
        df_clean.write.mode("overwrite").parquet(out_acc)
        print(f"SUCCESS: Accidents saved to {out_acc}")
        
    except Exception as e:
        print(f"ERROR Accidents: {e}")

    # ==========================================
    # PART 2: FUEL (The Fix)
    # ==========================================
    try:
        print("--- Processing Fuel ---")
        fuel_base = f"{RAW_BASE}/gouv_fuel"
        latest_date = sorted(os.listdir(fuel_base))[-1]
        xml_file = f"{fuel_base}/{latest_date}/PrixCarburants_instantane.xml"
        
        print(f"Parsing XML manually: {xml_file}")
        
        # We use Python's built-in XML parser (Safe & Simple)
        tree = ET.parse(xml_file)
        root = tree.getroot()
        
        fuel_data = []
        
        # Loop through every Gas Station (pdv)
        for pdv in root.findall('pdv'):
            cp = pdv.get('cp')
            # Get first 2 digits of postal code = Department (e.g., 33100 -> 33)
            if cp and len(cp) >= 2:
                dep = cp[:2]
                
                # Loop through prices in this station
                for prix in pdv.findall('prix'):
                    val = prix.get('valeur')
                    name = prix.get('nom')
                    
                    if val:
                        # Add to list: (Department, Price)
                        fuel_data.append((dep, float(val)))
        
        # Convert List -> Spark DataFrame
        # Schema: department (String), price (Double)
        print(f"Creating Spark DataFrame from {len(fuel_data)} price records...")
        df_fuel_raw = spark.createDataFrame(fuel_data, ["department", "price"])
        
        # Aggregate: Average price per Department
        df_fuel_avg = df_fuel_raw.groupBy("department").agg(
            avg("price").alias("avg_fuel_price")
        )
        
        # Save Fuel
        out_fuel = f"{FORMATTED_BASE}/fuel_clean"
        df_fuel_avg.write.mode("overwrite").parquet(out_fuel)
        print(f"SUCCESS: Fuel saved to {out_fuel}")

    except Exception as e:
        print(f"ERROR Fuel: {e}")

if __name__ == "__main__":
    run_job()