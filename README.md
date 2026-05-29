
# Healthcare Data Pipeline: Delta Live Tables (DLT) Medallion Architecture

An industrial-grade, end-to-end healthcare data engineering pipeline built on **Databricks Delta Live Tables (DLT)**. This project implements a **Medallion Architecture (Bronze $\rightarrow$ Silver $\rightarrow$ Gold)** to ingest, clean, enrich, and aggregate daily patient admission records and diagnostic reference data, enforcing strict clinical data quality using **DLT Expectations**.

---

## 📌 Architecture Overview

The core data flow reads raw data via an ingestion notebook, pushes it through streaming and materialized views using DLT, and writes the structured records securely into **Unity Catalog**.

Plaintext

```
[ Raw Ingestion Layer ] (PySpark Notebook)

  - patients_daily_file_X.csv goes to raw_patients_daily (Delta)

  - diagnosis_mapping.csv goes to raw_diagnosis_map (Delta)

[ Bronze Layer ] (DLT Ingestion & Data Quality)

  - daily_patients (Streaming Table + Quality Constraints)

  - diagnostic_mapping (Materialized View + Quality Constraints)

[ Silver Layer ] (DLT Enrichment)

  - processed_patient_data (Streaming Table + Enriched Left-Join)

[ Gold Layer ] (DLT Analytics Engine)

  - patient_statistics_by_admission_date (Materialized View)

  - patient_statistics_by_diagnosis (Materialized View)

  - patient_statistics_by_gender (Materialized View)
```


### 🧬 Medallion Layer Specifications

1. **Bronze Layer (Ingestion & Quality Isolation)**
   * **`daily_patients`** *(Streaming Table)*: Ingests incremental daily patient admission logs. Enforces primary key integrity (`patient_id IS NOT NULL`) and record completeness across 6 critical demographic and clinical attributes using `ON VIOLATION DROP ROW`.
   * **`diagnostic_mapping`** *(Materialized View)*: Ingests raw diagnostic codes and maps them to clean medical terminology, dropping rows with missing primary mappings.

2. **Silver Layer (Enrichment & Contextualization)**
   * **`processed_patient_data`** *(Streaming Table)*: Performs a stream-to-static `LEFT JOIN` between streaming patient data and the diagnostic mapping reference dataset. It validates downstream analytics readiness using a `has_diagnosis` expectation.

3. **Gold Layer (Analytics & Business Intelligence)**
   * **`patient_statistics_by_admission_date`**: Aggregates patient volumes and average age by date and diagnosis for operational hospital capacity tracking.
   * **`patient_statistics_by_diagnosis`**: Compiles medical research metrics including overall patient count, min/max/avg age limits, and unique gender distributions per clinical condition.
   * **`patient_statistics_by_gender`**: Builds population health analytics, focusing on demographic trends and unique diagnosis diversity per gender group.

---

## 🛠️ Tech Stack & Key Design Patterns

* **Databricks DLT Framework**: Leverages declaratively defined tables with unified streaming and batch execution.
* **Delta Lake & Unity Catalog**: Governs data assets inside a unified data catalog structure (`healthcare_catalog.default.*`) utilizing managed features like schema evolution mapping and data lineage tracking.
* **Data Quality Governance (Expectations)**: Employs `CONSTRAINT ... EXPECT ... ON VIOLATION DROP ROW` clauses directly in the pipeline, ensuring no corrupted or toxic data bleeds into analytical tables.
* **Operational Optimization**: One-click deployment with built-in auto-checkpointing, cluster auto-scaling, state management, and clear, granular execution graphs visible in the DLT UI.

---

## 📊 Pipeline Visualization & UI Metrics

The pipeline execution telemetry in Databricks proves consistent data flow, fast refresh processing times ($\le 5\text{s}$ for aggregated gold materializations), and isolated data quality filtering:

* **Data Quality Metrics**: The DLT dashboard tracks records matching expectations (e.g., `daily_patients` caught and dropped 86 erroneous rows out of 500 total records, successfully maintaining 414 high-signal records).
* **Incremental Processing**: Real-time streaming tables seamlessly process files using an append-only paradigm, while gold analytical reports dynamically perform full recomputations automatically upon dependency updates.

---

## 🚀 How to Deploy & Run

### 1. Prerequisites
* A Databricks Workspace with Unity Catalog enabled.
* A target Catalog named `healthcare_catalog` and schema named `default` (or updated configurations inside the source code).
* A Volume configured at `/Volumes/healthcare_catalog/default/healthcare_data/` containing your source `.csv` datasets.

### 2. Step 1: Bootstrap Raw Data Ingestion
Execute the PySpark bootstrap notebook `feed_raw_tables.ipynb` inside your workspace to read incoming file drops and append records to the raw Delta tables:
```python
# To ingest the latest batch of records, update the path parameter in cell 2:
path = "/Volumes/healthcare_catalog/default/healthcare_data/patients_daily_file_3_2025.csv"



### 3\. Step 2: Configure and Run the Delta Live Tables Pipeline

1.  Navigate to the **Delta Live Tables** tab in your Databricks workspace and click **Create Pipeline**.

2.  Complete the settings form:

    -   **Pipeline Name**: `delta-live-healthcare-pipeline`

    -   **Product Edition**: Advanced (Required for Expectations/Data Quality)

    -   **Pipeline Type**: Triggered or Continuous

    -   **Source Code**: Select the path to your `healthcare_dlt_processing.ipynb` notebook located inside the `NoteBooks/` directory.

    -   **Storage Options**: Select **Unity Catalog**, choose your target catalog (`healthcare_catalog`), and enter your target schema (`default`).

3.  Click **Start** to trigger the initial execution run.
```
📁 Repository Structure
-----------------------

Plaintext

```
├── data/                             # Source clinical CSV files & mapping data
├── Images/                           # Databricks DLT UI execution graphs and pipeline metrics
├── NoteBooks/                        # Databricks notebooks
│   ├── feed_raw_tables.ipynb         # PySpark raw historical/daily ingestion bootstrap
│   └── healthcare_dlt_processing.ipynb# Main DLT processing pipeline (SQL notebook)
├── .gitignore                        # Standard Git exclusion file
├── projectStructure.txt              # Exported project tree map
└── README.md                         # Documentation

```

📈 Sample Analytics Output
--------------------------

Once completed, the pipeline populates rich tables ready for consumption by PowerBI, Databricks SQL Dashboards, or AI/ML environments.

### `patient_statistics_by_diagnosis`

| **diagnosis_description** | **patient_count** | **avg_age** | **min_age** | **max_age** | **unique_gender_count** |
| --- | --- | --- | --- | --- | --- |
| Coronary Artery Disease | 52 | 41.88 | 29 | 55 | 2 |
| Myocardial Infarction | 49 | 40.59 | 28 | 53 | 2 |
| Hyperlipidemia | 45 | 41.04 | 29 | 53 | 2 |
| Hypertension | 43 | 39.51 | 29 | 53 | 2 |

### `patient_statistics_by_gender`

| **gender** | **patient_count** | **avg_age** | **min_age** | **max_age** | **unique_diagnosis_count** |
| --- | --- | --- | --- | --- | --- |
| F | 214 | 41.12 | 28 | 61 | 10 |
| M | 200 | 39.98 | 28 | 56 | 10 |

*Developed by Manish Kumar Rai (<hire.manishrai@gmail.com>)*
