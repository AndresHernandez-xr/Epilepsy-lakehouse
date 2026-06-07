# Epilepsy-lakehouse
An enterprise-grade clinical data lakehouse using a Medallion Architecture (Bronze/Silver/Gold). Built with Python, DuckDB, and Pydantic v2 to enforce strict Data Contracts, the pipeline isolates schema anomalies to a containerized S3 Quarantine zone (Docker/MinIO) while materializing fast, optimized business metrics.

# 🏥 Automated Clinical Lakehouse Pipeline (Medallion Architecture)

An enterprise-grade, end-to-end data lakehouse pipeline built to ingest, validate, and analyze clinical neurological data for epilepsy monitoring. This project demonstrates a **Medallion Architecture (Bronze -> Silver -> Gold)** utilizing modern, high-performance data engineering tools to enforce strict **Data Contracts**, handle malformed records via an automated **Quarantine Zone**, and compute optimized business intelligence metrics.

## 📐 Architecture Overview

The platform implements a single-node modern data stack that mimics cloud-based decoupled storage and compute environments:
[ Hugging Face API ]
│
▼  (Immutable Streaming Ingestion)
┌────────────────────────────────────────────────────────┐
│ BRONZE LAYER: Raw Parquet Archive (S3 / MinIO)         │
└────────────────────────────────────────────────────────┘
│
▼  (Data Contract Gatekeeper & Imputation)
[ Pydantic V2 ] ───► (Validation Failures) ───► [ QUARANTINE ZONE ]
│
▼  (Clean 99.98% Passed Records)
┌────────────────────────────────────────────────────────┐
│ SILVER LAYER: Enforced Schema Parquet (S3 / MinIO)     │
└────────────────────────────────────────────────────────┘
│
▼  (In-Process Columnar OLAP Computations)
[ DuckDB Engine ]
│
▼  (Materialized Business Metrics)
┌────────────────────────────────────────────────────────┐
│ GOLD LAYER: Analytical Presentation Mart (S3)          │
└────────────────────────────────────────────────────────┘
1. **Bronze (Raw Data Lake):** Ingests raw tabular data natively from remote clinical endpoints and streams it into an immutable, time-partitioned directory structure (`year=YYYY/month=MM/day=DD`) inside an S3 environment as compressed Apache Parquet files.
2. **Silver (Cleaned/Validated Warehouse):** Enforces strict data governance using a **Pydantic Data Contract**. Imputes structural anomalies (such as converting float-based `NaN` states to fallback defaults) and safely routes valid records downstream.
3. **Quarantine Zone:** To maintain platform integrity, any row violating schema requirements is dynamically captured with explicit error metadata, stamped with a UTC timestamp, and isolated into a quarantine partition to protect downstream operations.
4. **Gold (Analytical Marts):** Uses **DuckDB**, a high-performance in-process vectorized query engine, to aggregate patient demographics into high-value cohorts and materialize business KPIs.

---

## 🛠️ Tech Stack & Core Engines

* **Data Governance / Contracts:** Pydantic V2 (Strict Python Type and Range Gatekeeping)
* **Compute & Execution Engines:** DuckDB (Vectorized OLAP SQL), Pandas, PyArrow
* **Storage Layer Infrastructure:** MinIO (Local, High-Performance AWS S3-Compatible Object Store)
* **DevOps Environment:** Docker, Docker Compose
* **Data Interchange Format:** Apache Parquet (Columnar, highly compressed metadata-rich storage)

---

## 🚀 Local Deployment & Execution

### 1. Prerequisites
Ensure you have Python 3.11+, Docker, and Docker Compose installed on your system.

### 2. Infrastructure Setup (S3 Object Store)
Spin up the decoupled storage layer using Docker:
```bash
docker-compose up -d

Access the visual MinIO console at http://localhost:9001 (Credentials: admin / supersecretpassword) and ensure the lakehouse bucket is created.

# Windows PowerShell Execution
python -m venv venv
.\venv\Scripts\Activate.ps1
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process

# Install Core Libraries
pip install datasets duckdb pandas pydantic s3fs pyarrow

#Run project
python pipeline.py

🚀 Initializing Lakehouse Pipeline Compute Engine...
📥 [BRONZE] Fetching raw clinical data from Hugging Face...
✅ [BRONZE] Raw snapshot immutably archived at: s3://lakehouse/bronze/epilepsy_events/year=2026/month=06/day=06/raw_batch.parquet

⚙️ [SILVER] Streaming from Bronze to enforce schemas and data contracts...
📊 Detected raw incoming columns: ['id', 'age', 'sex', ..., 'treatment_gap']

🔍 [DEBUG ALERT] Row failed contract validation!
❌ Error Reason: Input should be greater than or equal to 0
📦 Attempted Values Passed to Contract: {'patient_id': '1', 'age': -9, 'seizure_type': 'absence', 'aed_status': 'none', 'treatment_gap': 0}

✨ [SILVER] Clean storage updated. Validated records saved: 9998
🚨 [QUARANTINE] Alert! Isolated 2 rows to: s3://lakehouse/quarantine/epilepsy_failed/failed_batch_20260606_174000.parquet

--- MATERIALIZED GOLD BUSINESS VIEW ---
  age_cohort seizure_type  total_monitored_patients  active_care_gaps  care_gap_rate_percentage
1      Adult        focal                      4120              2110                     51.21
2  Pediatric      absence                      3245              1540                     47.46
3  Geriatric        tonic                      2633               920                     34.94
