# MLOps Lab 3: Building Airflow ETL Pipeline (Frankfurter API -> MongoDB)

An Apache Airflow ETL pipeline built with **Astro CLI** that extracts daily currency exchange rates (`EUR` to `USD, GBP, JPY`) from the **Frankfurter API v2**, transforms the records into a standardized schema using **Airflow Named XComs**, and loads them into **MongoDB** (with **Mongo Express** configured as the web administration interface).

---

## Project Architecture

```text
Lab 3/
├── .astro/
│   └── config.yaml                  # Astro CLI project configuration
├── dags/
│   └── currency_exchange_etl.py     # Main 3-task ETL DAG using Named XComs
├── tests/
│   └── dags/
│       └── test_dag_example.py      # Unit tests for DAG & transformation schema
├── Dockerfile                       # Astro Runtime Docker image
├── docker-compose.override.yml      # MongoDB & Mongo Express services + Networking + Persistent Storage
├── airflow_settings.yaml            # Local Airflow Connections (mongo_default, frankfurter_api)
├── requirements.txt                 # Python dependencies (pymongo, requests, airflow providers)
└── packages.txt                     # OS-level dependencies
```

---

## Key Features & Task Requirements

### 1. Frankfurter API Integration
- **Endpoint**: `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`

### 2. MongoDB & Mongo Express Docker Configuration (`docker-compose.override.yml`)
- **Primary Database (`mongodb`)**: Runs `mongo:latest` on port `27017`.
- **Web Admin UI (`mongo-express`)**: Runs `mongo-express:latest` on port `8081`.
- **a) Docker Networking**: Both `mongodb` and `mongo-express` are attached to the `airflow` bridge network so Airflow containers can reach MongoDB via hostname `mongodb:27017`.
- **b) Persistent Storage**: Named Docker volumes (`mongodb_data:/data/db` and `mongodb_config:/data/configdb`) ensure database records persist across container restarts.
- **Docker Labels**: Custom `com.mlops.lab3.*` metadata labels attached to services and volumes.

### 3. Three ETL Tasks with Named XComs (`dags/currency_exchange_etl.py`)
1. **`extract_currency_exchange_rates`**:
   - Calls `https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`.
   - Pushes the raw JSON response to Named XCom key `"raw_exchange_rates"`.
2. **`transform_exchange_rate_data`**:
   - Pulls `"raw_exchange_rates"` from Named XCom.
   - Transforms each currency rate into the required schema:
     ```json
     {
       "base_currency": "EUR",
       "target_currency": "USD",
       "exchange_rate": 1.15,
       "rate_date": "2026-11-04",
       "source": "Frankfurter API",
       "retrieved_at": "2026-11-05T07:15:00"
     }
     ```
   - Pushes the transformed documents to Named XCom key `"transformed_exchange_rates"`.
3. **`load_data_into_mongodb`**:
   - Pulls `"transformed_exchange_rates"` from Named XCom.
   - Inserts the transformed documents into MongoDB (`currency_exchange_db.exchange_rates`) and pushes the load status summary to Named XCom key `"mongodb_load_status"`.

---

## How to Run with Astro CLI

1. **Start the Astro Airflow + MongoDB + Mongo Express environment**:
   ```bash
   astro dev start
   ```
2. **Access Interfaces**:
   - **Airflow Web UI**: http://localhost:8080 (`admin` / `admin`)
   - **Mongo Express Web UI**: http://localhost:8081 (`admin` / `password`)
   - **MongoDB**: `mongodb://admin:password@localhost:27017/`
3. **Trigger the DAG**:
   - In the Airflow UI, unpause and trigger **`currency_exchange_etl_pipeline`**.
   - Check **XComs** in each task instance to view `raw_exchange_rates`, `transformed_exchange_rates`, and `mongodb_load_status`.
   - Open **Mongo Express** at http://localhost:8081 -> Database `currency_exchange_db` -> Collection `exchange_rates` to view the loaded documents.
