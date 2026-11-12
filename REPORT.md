# MLOps Lab 3 Report: Building Airflow ETL Pipeline (Frankfurter API to MongoDB)

- **Student Name**: Muhammad Junaid (L23-1014)
- **Course**: MLOps (Semester 7)
- **Lab**: Task 3 — Building Airflow ETL Pipeline using Astro CLI, Frankfurter API, MongoDB & Mongo Express
- **GitHub Repository Link**: https://github.com/JunaidMu/L231014_Task3_Ops (Clone: `https://github.com/JunaidMu/L231014_Task3_Ops.git`)

---

## 1. Objective

Design and implement an automated **Apache Airflow ETL pipeline** using **Astro CLI** (`astro-runtime`) that:
1. Extracts daily currency exchange rates (`EUR` to `USD, GBP, JPY`) from the public **Frankfurter API v2** (`https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`).
2. Transforms the raw exchange rate response into a standardized schema containing `base_currency`, `target_currency`, `exchange_rate`, `rate_date`, `source`, and `retrieved_at`.
3. Transfers data between tasks explicitly using **Airflow Named XComs** (`ti.xcom_push` and `ti.xcom_pull` with custom keys).
4. Loads the transformed records into **MongoDB** (configured with **Docker Networking** and **Persistent Storage**) and inspects them via **Mongo Express** (configured as a companion Docker service).

---

## 2. Project Structure & Configuration

```text
Lab 3/
├── .astro/
│   └── config.yaml                  # Astro CLI configuration
├── dags/
│   └── currency_exchange_etl.py     # Main 3-task Airflow ETL DAG using Named XComs
├── tests/
│   └── dags/
│       └── test_dag_example.py      # Unit tests for transformation schema
├── Dockerfile                       # Astro Runtime base image (quay.io/astronomer/astro-runtime:12.6.0)
├── docker-compose.override.yml      # MongoDB & Mongo Express services + Networking + Persistent Storage
├── docker-compose.yml               # Local Astro Runtime Airflow orchestration
├── airflow_settings.yaml            # Airflow Connections configuration
├── requirements.txt                 # Python dependencies (pymongo, requests, airflow providers)
└── packages.txt                     # OS-level dependencies
```

---

## 3. MongoDB & Mongo Express Configuration (`docker-compose.override.yml`)

Both **MongoDB** (primary database) and **Mongo Express** (web administration interface) are configured as Docker services with:
- **a) Docker Networking**: Shared `airflow` bridge network connecting Airflow (`scheduler`, `webserver`), `mongodb`, and `mongo-express`.
- **b) Persistent Storage**: Named local Docker volumes (`mongodb_data:/data/db` and `mongodb_config:/data/configdb`) so database records persist across container restarts.
- **Docker Metadata Labels**: Custom `com.mlops.lab3.*` labels attached to services and volumes.

> **[SCREENSHOT 1: Paste screenshot of `docker-compose.override.yml` in VS Code here]**

---

## 4. ETL Pipeline Implementation (`dags/currency_exchange_etl.py`)

The DAG `currency_exchange_etl_pipeline` defines three sequential `PythonOperator` tasks connected via `extract_task >> transform_task >> load_task`:

1. **Task a — `extract_currency_exchange_rates`**:
   - Queries `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`.
   - Pushes the raw JSON array to **Named XCom** key `"raw_exchange_rates"`.
2. **Task b — `transform_exchange_rate_data`**:
   - Pulls `"raw_exchange_rates"` from `extract_currency_exchange_rates` via Named XCom.
   - Transforms each rate into the required JSON structure:
     ```json
     {
       "base_currency": "EUR",
       "target_currency": "USD",
       "exchange_rate": 1.1122,
       "rate_date": "2026-11-12",
       "source": "Frankfurter API",
       "retrieved_at": "2026-11-12T17:54:07"
     }
     ```
   - Pushes the transformed list to **Named XCom** key `"transformed_exchange_rates"`.
3. **Task c — `load_data_into_mongodb`**:
   - Pulls `"transformed_exchange_rates"` from `transform_exchange_rate_data` via Named XCom.
   - Connects to `mongodb://admin:password@mongodb:27017/?authSource=admin` and inserts the documents into `currency_exchange_db.exchange_rates`.
   - Pushes the insertion summary (`inserted_count`, `inserted_ids`) to **Named XCom** key `"mongodb_load_status"`.

> **[SCREENSHOT 2: Paste screenshot of `dags/currency_exchange_etl.py` in VS Code here]**

---

## 5. Pipeline Execution & Outputs

### 5.1 Running Docker Containers (Airflow + MongoDB + Mongo Express)
> **[SCREENSHOT 3: Paste screenshot of `docker ps` in terminal or Docker Desktop showing `airflow_webserver`, `airflow_scheduler`, `airflow_postgres`, `mongodb`, and `mongo_express` running]**

### 5.2 Airflow Web UI — DAG Graph / Grid View (`http://localhost:8080`)
> **[SCREENSHOT 4: Paste screenshot of Airflow UI (`http://localhost:8080`) showing `currency_exchange_etl_pipeline` Graph/Grid view with all 3 tasks (`extract_currency_exchange_rates` -> `transform_exchange_rate_data` -> `load_data_into_mongodb`) in green `success` state]**

### 5.3 Airflow Named XComs Verification (`http://localhost:8080`)
> **[SCREENSHOT 5: Paste screenshot of Airflow UI -> Admin -> XComs (or Task Instance -> XCom tab) showing the Named XCom keys: `raw_exchange_rates`, `transformed_exchange_rates`, and `mongodb_load_status`]**

### 5.4 Mongo Express Web UI — Loaded Exchange Rate Documents (`http://localhost:8081`)
> **[SCREENSHOT 6: Paste screenshot of Mongo Express (`http://localhost:8081`) -> Database `currency_exchange_db` -> Collection `exchange_rates` showing the inserted JSON documents for `USD`, `GBP`, and `JPY`]**
