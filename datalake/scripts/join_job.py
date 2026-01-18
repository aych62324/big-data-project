from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round

# Initialize Spark
spark = SparkSession.builder \
    .appName("Project_Combination") \
    .getOrCreate()

# Paths
FORMATTED_BASE = "/opt/spark/work-dir/datalake/formatted"
USAGE_BASE = "/opt/spark/work-dir/datalake/usage"

def run_combination():
    print("--- STARTING COMBINATION JOB ---")
    
    # 1. READ FORMATTED DATA (Parquet)
    try:
        print("Reading Accidents and Fuel...")
        df_accidents = spark.read.parquet(f"{FORMATTED_BASE}/accidents_clean")
        df_fuel = spark.read.parquet(f"{FORMATTED_BASE}/fuel_clean")
        
        # 2. AGGREGATE ACCIDENTS BY DEPARTMENT
        # We want to know: How many accidents per department?
        acc_by_dep = df_accidents.groupBy("department").agg(
            count("Num_Acc").alias("total_accidents"),
            avg("weather").alias("avg_weather_score") # Just to add more data
        )
        
        # 3. JOIN WITH FUEL PRICE
        # Join Condition: Department == Department
        print("Joining datasets...")
        df_final = acc_by_dep.join(df_fuel, "department", "inner")
        
        # 4. CALCULATE FINAL METRICS
        # Let's sort by Fuel Price to see if expensive areas have more/less accidents
        df_final = df_final.orderBy(col("avg_fuel_price").desc())
        
        # Round the price for cleaner display
        df_final = df_final.withColumn("avg_fuel_price", round(col("avg_fuel_price"), 3))
        
        # Show a preview in the console
        print("--- FINAL DATA PREVIEW ---")
        df_final.show(10)
        
        # 5. SAVE FINAL OUTPUT
        # We save as Parquet (for history) and JSON (for easy Indexing to Elastic later)
        out_path = f"{USAGE_BASE}/final_output"
        
        # Save Parquet
        df_final.write.mode("overwrite").parquet(out_path)
        
        # Save JSON (Easier for the next step: Elasticsearch)
        df_final.coalesce(1).write.mode("overwrite").json(f"{USAGE_BASE}/final_output_json")
        
        print(f"SUCCESS: Combined data saved to {out_path}")
        
    except Exception as e:
        print(f"ERROR in Combination: {e}")

if __name__ == "__main__":
    run_combination()