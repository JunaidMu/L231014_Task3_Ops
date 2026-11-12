"""
Airflow ETL Pipeline: Daily Currency Exchange Rates (Frankfurter API -> MongoDB)

Tasks:
  a) extract_currency_exchange_rates : Fetches daily EUR -> USD, GBP, JPY rates from Frankfurter API v2
  b) transform_exchange_rate_data    : Transforms raw API data into standardized MongoDB documents
  c) load_data_into_mongodb          : Loads transformed documents into MongoDB

Uses Airflow Named XComs (`xcom_push` / `xcom_pull` with explicit `key` names)
to transfer data between tasks.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient

logger = logging.getLogger(__name__)

# Frankfurter API v2 endpoint as specified in Task 3
FRANKFURTER_API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_QUOTES = "USD,GBP,JPY"

# Named XCom Keys
XCOM_KEY_RAW_RATES = "raw_exchange_rates"
XCOM_KEY_TRANSFORMED_RATES = "transformed_exchange_rates"
XCOM_KEY_LOAD_STATUS = "mongodb_load_status"

# MongoDB Configuration
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:password@mongodb:27017/?authSource=admin",
)
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "currency_exchange_db")
MONGO_COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "exchange_rates")


def extract_currency_exchange_rates(**context: Any) -> None:
    """
    Task a) Extract Currency Exchange Rates from Frankfurter API v2
    Endpoint: https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY
    Pushes raw JSON response to Airflow Named XCom (`raw_exchange_rates`).
    """
    ti = context["ti"]
    params = {
        "base": BASE_CURRENCY,
        "quotes": TARGET_QUOTES,
    }

    logger.info(
        "Requesting exchange rates from %s with params=%s",
        FRANKFURTER_API_URL,
        params,
    )
    response = requests.get(FRANKFURTER_API_URL, params=params, timeout=30)
    response.raise_for_status()

    raw_data = response.json()
    logger.info("Extracted raw exchange rate payload: %s", raw_data)

    # Push extracted data using Named XCom
    ti.xcom_push(key=XCOM_KEY_RAW_RATES, value=raw_data)


def transform_exchange_rate_data(**context: Any) -> None:
    """
    Task b) Transform Exchange Rate Data
    Pulls `raw_exchange_rates` from Named XCom and transforms each currency quote into:
    {
        "base_currency": "EUR",
        "target_currency": "USD",
        "exchange_rate": 1.15,
        "rate_date": "2026-11-04",
        "source": "Frankfurter API",
        "retrieved_at": "2026-11-05T07:15:00"
    }
    Pushes transformed list to Airflow Named XCom (`transformed_exchange_rates`).
    """
    ti = context["ti"]

    # Pull raw data from Task a using Named XCom
    raw_data = ti.xcom_pull(
        task_ids="extract_currency_exchange_rates",
        key=XCOM_KEY_RAW_RATES,
    )

    if not raw_data:
        raise ValueError(
            f"No data received from Named XCom key '{XCOM_KEY_RAW_RATES}'."
        )

    retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    transformed_records: list[dict[str, Any]] = []

    # Handle Frankfurter API v2 response (list of rate objects)
    if isinstance(raw_data, list):
        for item in raw_data:
            record = {
                "base_currency": str(item.get("base", BASE_CURRENCY)),
                "target_currency": str(item["quote"]),
                "exchange_rate": float(item["rate"]),
                "rate_date": str(item["date"]),
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            transformed_records.append(record)
    # Fallback support for v1-style dict response if ever used
    elif isinstance(raw_data, dict) and "rates" in raw_data:
        base_curr = str(raw_data.get("base", BASE_CURRENCY))
        rate_date = str(raw_data.get("date", ""))
        for quote_curr, rate_val in raw_data["rates"].items():
            record = {
                "base_currency": base_curr,
                "target_currency": str(quote_curr),
                "exchange_rate": float(rate_val),
                "rate_date": rate_date,
                "source": "Frankfurter API",
                "retrieved_at": retrieved_at,
            }
            transformed_records.append(record)
    else:
        raise ValueError(f"Unexpected Frankfurter API payload format: {raw_data}")

    logger.info(
        "Transformed %d exchange rate records: %s",
        len(transformed_records),
        transformed_records,
    )

    # Push transformed data using Named XCom
    ti.xcom_push(key=XCOM_KEY_TRANSFORMED_RATES, value=transformed_records)


def load_data_into_mongodb(**context: Any) -> None:
    """
    Task c) Load Data into MongoDB
    Pulls `transformed_exchange_rates` from Named XCom and inserts records
    into MongoDB database `currency_exchange_db`, collection `exchange_rates`.
    """
    ti = context["ti"]

    # Pull transformed data from Task b using Named XCom
    transformed_records = ti.xcom_pull(
        task_ids="transform_exchange_rate_data",
        key=XCOM_KEY_TRANSFORMED_RATES,
    )

    if not transformed_records:
        raise ValueError(
            f"No transformed records found in Named XCom key '{XCOM_KEY_TRANSFORMED_RATES}'."
        )

    # Prepare clean copy for insertion (pymongo mutates dicts in-place by adding `_id`)
    docs_to_insert = [dict(doc) for doc in transformed_records]

    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
    try:
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLLECTION_NAME]

        result = collection.insert_many(docs_to_insert)
        inserted_ids = [str(obj_id) for obj_id in result.inserted_ids]

        logger.info(
            "Successfully inserted %d documents into MongoDB (%s.%s). IDs: %s",
            len(inserted_ids),
            MONGO_DB_NAME,
            MONGO_COLLECTION_NAME,
            inserted_ids,
        )

        # Push load status summary using Named XCom
        ti.xcom_push(
            key=XCOM_KEY_LOAD_STATUS,
            value={
                "database": MONGO_DB_NAME,
                "collection": MONGO_COLLECTION_NAME,
                "inserted_count": len(inserted_ids),
                "inserted_ids": inserted_ids,
            },
        )
    finally:
        client.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="currency_exchange_etl_pipeline",
    default_args=default_args,
    description="Daily ETL pipeline extracting EUR exchange rates (USD, GBP, JPY) from Frankfurter API and loading into MongoDB",
    schedule="@daily",
    start_date=datetime(2026, 11, 1),
    catchup=False,
    tags=["etl", "currency", "frankfurter", "mongodb", "named-xcom"],
) as dag:

    # Task a) Extract Currency Exchange Rates
    extract_task = PythonOperator(
        task_id="extract_currency_exchange_rates",
        python_callable=extract_currency_exchange_rates,
    )

    # Task b) Transform Exchange Rate Data
    transform_task = PythonOperator(
        task_id="transform_exchange_rate_data",
        python_callable=transform_exchange_rate_data,
    )

    # Task c) Load Data into MongoDB
    load_task = PythonOperator(
        task_id="load_data_into_mongodb",
        python_callable=load_data_into_mongodb,
    )

    # Define ETL task dependencies
    extract_task >> transform_task >> load_task
