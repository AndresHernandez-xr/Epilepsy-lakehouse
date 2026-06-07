import os
from datetime import datetime, timezone  # Added timezone here
import duckdb
from datasets import load_dataset
import pandas as pd
from pydantic import ValidationError

# Import the data contract gatekeeper we created in Phase 2
from contracts import EpilepsyPatientContract

print("🚀 Initializing Lakehouse Pipeline Compute Engine...")

# =====================================================================
# SYSTEM CONFIGURATION: Link DuckDB to our Local Docker S3 (MinIO)
# =====================================================================
con = duckdb.connect(database=':memory:')
con.execute("""
    INSTALL httpfs;
    LOAD httpfs;
    SET s3_endpoint='localhost:9000';
    SET s3_access_key_id='admin';
    SET s3_secret_access_key='supersecretpassword';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# Standard S3 configuration storage options for Pandas when writing files
S3_STORAGE_OPTIONS = {
    "key": "admin",
    "secret": "supersecretpassword",
    "client_kwargs": {"endpoint_url": "http://localhost:9000", "use_ssl": False}
}

# =====================================================================
# STEP 1: BRONZE LAYER INGESTION (Immutable Raw Storage)
# =====================================================================
def run_bronze_ingestion():
    print("\n📥 [BRONZE] Fetching raw clinical data from Hugging Face...")
    
    # Load the tabular dataset
    dataset = load_dataset("electricsheepafrica/epilepsy-neurological", split='train')
    raw_df = dataset.to_pandas()
    
    # --- PORTFOLIO FEATURE: Injecting Malformed Data ---
    # Real data engineering pipelines must handle dirty data. We will intentionally 
    # corrupt two rows here to prove our Data Contract system catches them.
    if len(raw_df) > 2:
        raw_df.loc[0, 'age'] = -9             # Error: Age cannot be negative
        raw_df.loc[1, 'treatment_gap'] = 5     # Error: Must be binary (0 or 1)
    
    # Create a time-partitioned directory path structure (Year/Month/Day)
    timestamp_partition = datetime.now().strftime("year=%Y/month=%m/day=%d")
    bronze_target_path = f"s3://lakehouse/bronze/epilepsy_events/{timestamp_partition}/raw_batch.parquet"
    
    # Write immutably to our local S3 bucket
    raw_df.to_parquet(bronze_target_path, index=False, storage_options=S3_STORAGE_OPTIONS)
    print(f"✅ [BRONZE] Raw snapshot immutably archived at: {bronze_target_path}")
    return bronze_target_path

# =====================================================================
# STEP 2: SILVER LAYER PROCESSING (Contract Enforcement & Refinement)
# =====================================================================
def process_silver_layer(bronze_path):
    print("\n⚙️ [SILVER] Streaming from Bronze to enforce schemas and data contracts...")
    
    # Stream data out of Bronze via DuckDB fast-reader
    df = con.execute(f"SELECT * FROM read_parquet('{bronze_path}')").df()
    
    raw_cols = df.columns.tolist()
    print(f"📊 Detected raw incoming columns: {raw_cols}")
    
    valid_records = []
    quarantined_records = []
    
    sample_error_printed = False
    
    # Track the absolute loop position index independently
    row_counter = 0
    
    # Loop through rows individually
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        
        # --- ROBUST ENTERPRISE IMPUTATION & CASTING ---
        raw_age = row_dict.get('age')
        raw_gap = row_dict.get('in_treatment_gap', row_dict.get('treatment_gap'))
        
        # Convert numeric elements cleanly to explicit integers
        clean_age = int(raw_age) if pd.notna(raw_age) else 0
        clean_gap = int(raw_gap) if pd.notna(raw_gap) else 0

        # Safely force float codes into clean string representations
        raw_seizure = row_dict.get('seizure_type')
        clean_seizure = str(raw_seizure) if pd.notna(raw_seizure) else "Unknown"
        
        raw_aed = row_dict.get('aed_prescribed')
        clean_aed = str(raw_aed) if pd.notna(raw_aed) else "Unknown"

        # Dynamically align raw columns to our internal Data Contract
        mapped_dict = {
            "patient_id": str(row_dict.get('id', 'UNKNOWN')),
            "age": clean_age,
            "seizure_type": clean_seizure,
            "aed_status": clean_aed, 
            "treatment_gap": clean_gap
        }
        
        # --- PORTFOLIO DEMO FEATURE: ISOLATED INJECTION ---
        # Corrupt only the first two absolute index entries to prove the contract rules function
        if row_counter == 0:
            mapped_dict['age'] = -9            # Will fail: age < 0 rule
        elif row_counter == 1:
            mapped_dict['treatment_gap'] = 5    # Will fail: treatment_gap 0/1 range rule
            
        row_counter += 1 # Increment immediately to unlock the rest of the file
        
        # --- THE TRY / EXCEPT BLOCK ---
        try:
            # Validate the mapped data structure against our gatekeeper contract
            validated_row = EpilepsyPatientContract(**mapped_dict)
            
            # Record passed! Convert to dictionary and stamp with processing time
            clean_data = validated_row.model_dump()
            clean_data['processed_at'] = datetime.now(timezone.utc) 
            valid_records.append(clean_data)
            
        except ValidationError as e:
            # Record failed! Capture failure metadata and quarantine it
            error_msg = str(e.errors()[0]['msg'])
            mapped_dict['contract_violation_error'] = error_msg
            mapped_dict['quarantined_at'] = datetime.now(timezone.utc)
            quarantined_records.append(mapped_dict)
            
            # Print only the second validation error to terminal to verify both rules work
            if not sample_error_printed and row_counter > 1:
                print(f"\n🔍 [DEBUG ALERT] Row failed contract validation!")
                print(f"❌ Error Reason: {error_msg}")
                print(f"📦 Attempted Values Passed to Contract: {mapped_dict}\n")
                sample_error_printed = True
            
    # =====================================================================
    # AFTER THE FOR-LOOP CLOSES: Write out the data batches to S3
    # =====================================================================
    # Write the clean records to the Silver Lakehouse layer
    if valid_records:
        silver_df = pd.DataFrame(valid_records)
        silver_path = "s3://lakehouse/silver/epilepsy_patients.parquet"
        silver_df.to_parquet(silver_path, index=False, storage_options=S3_STORAGE_OPTIONS)
        print(f"✨ [SILVER] Clean storage updated. Validated records saved: {len(valid_records)}")
    else:
        print("⚠️ [SILVER] Warning: Zero records passed validation. Check your column mappings!")
        
    # Isolate corrupted data away from analytical engines to the Quarantine bucket
    if quarantined_records:
        quarantine_df = pd.DataFrame(quarantined_records)
        time_slug = datetime.now().strftime("%Y%m%d_%H%M%S")
        quarantine_path = f"s3://lakehouse/quarantine/epilepsy_failed/failed_batch_{time_slug}.parquet"
        quarantine_df.to_parquet(quarantine_path, index=False, storage_options=S3_STORAGE_OPTIONS)
        print(f"🚨 [QUARANTINE] Alert! Isolated {len(quarantined_records)} rows to: {quarantine_path}")
            
    # =====================================================================
    # AFTER THE FOR-LOOP CLOSES: Write out the data batches to S3
    # =====================================================================
    # Write the clean records to the Silver Lakehouse layer
    if valid_records:
        silver_df = pd.DataFrame(valid_records)
        silver_path = "s3://lakehouse/silver/epilepsy_patients.parquet"
        silver_df.to_parquet(silver_path, index=False, storage_options=S3_STORAGE_OPTIONS)
        print(f"✨ [SILVER] Clean storage updated. Validated records saved: {len(valid_records)}")
    else:
        print("⚠️ [SILVER] Warning: Zero records passed validation. Check your column mappings!")
        
    # Isolate corrupted data away from analytical engines to the Quarantine bucket
    if quarantined_records:
        quarantine_df = pd.DataFrame(quarantined_records)
        time_slug = datetime.now().strftime("%Y%m%d_%H%M%S")
        quarantine_path = f"s3://lakehouse/quarantine/epilepsy_failed/failed_batch_{time_slug}.parquet"
        quarantine_df.to_parquet(quarantine_path, index=False, storage_options=S3_STORAGE_OPTIONS)
        print(f"🚨 [QUARANTINE] Alert! Isolated {len(quarantined_records)} rows to: {quarantine_path}")
    
# =====================================================================
# STEP 3: GOLD LAYER AGGREGATION (High-Value Business Analytics)
# =====================================================================
def generate_gold_marts():
    print("\n🏆 [GOLD] Transforming clean Silver tables into business-ready analytical marts...")
    
    silver_path = "s3://lakehouse/silver/epilepsy_patients.parquet"
    gold_path = "s3://lakehouse/gold/monthly_treatment_gap.parquet"
    
    # Run an advanced SQL analytical query using DuckDB to calculate metrics across patient demographics
    con.execute(f"""
        CREATE OR REPLACE TABLE gold_analytics AS 
        SELECT 
            CASE 
                WHEN age < 18 THEN 'Pediatric'
                WHEN age BETWEEN 18 AND 60 THEN 'Adult'
                ELSE 'Geriatric'
            END AS age_cohort,
            seizure_type,
            COUNT(patient_id) as total_monitored_patients,
            SUM(treatment_gap) as active_care_gaps,
            ROUND(AVG(treatment_gap) * 100, 2) as care_gap_rate_percentage
        FROM read_parquet('{silver_path}')
        GROUP BY 1, 2
        ORDER BY care_gap_rate_percentage DESC
    """)
    
    # Export the final presentation table to the Gold production partition
    con.execute(f"COPY gold_analytics TO '{gold_path}' (FORMAT PARQUET);")
    print(f"📊 [GOLD] Aggregated insights materialized successfully at: {gold_path}")
    
    # Print the resulting business metrics directly to the terminal
    print("\n--- MATERIALIZED GOLD BUSINESS VIEW ---")
    print(con.execute("SELECT * FROM gold_analytics").df().to_string())

# =====================================================================
# PIPELINE EXECUTION
# =====================================================================
if __name__ == "__main__":
    # Run the full sequence end-to-end
    bronze_file_path = run_bronze_ingestion()
    process_silver_layer(bronze_file_path)
    generate_gold_marts()
    print("\n🎉 Pipeline execution completed successfully!")
