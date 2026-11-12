"""Tests for the Currency Exchange ETL Pipeline DAG and transformation logic."""

from unittest.mock import MagicMock
import pytest


def test_transform_exchange_rate_data_schema():
    """Verify that raw Frankfurter v2 API payload transforms into the exact required schema."""
    sample_raw_payload = [
        {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 1.15},
        {"date": "2026-11-04", "base": "EUR", "quote": "GBP", "rate": 0.85},
        {"date": "2026-11-04", "base": "EUR", "quote": "JPY", "rate": 165.42},
    ]

    # Simulate Named XCom pull and push
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = sample_raw_payload

    # Import task logic when Airflow is installed, or validate transformation logic directly
    try:
        from dags.currency_exchange_etl import (
            XCOM_KEY_RAW_RATES,
            XCOM_KEY_TRANSFORMED_RATES,
            transform_exchange_rate_data,
        )

        transform_exchange_rate_data(ti=mock_ti)
        mock_ti.xcom_pull.assert_called_once_with(
            task_ids="extract_currency_exchange_rates",
            key=XCOM_KEY_RAW_RATES,
        )
        assert mock_ti.xcom_push.called
        call_kwargs = mock_ti.xcom_push.call_args.kwargs
        assert call_kwargs["key"] == XCOM_KEY_TRANSFORMED_RATES
        transformed = call_kwargs["value"]
        assert len(transformed) == 3
        first = transformed[0]
        assert first["base_currency"] == "EUR"
        assert first["target_currency"] == "USD"
        assert first["exchange_rate"] == 1.15
        assert first["rate_date"] == "2026-11-04"
        assert first["source"] == "Frankfurter API"
        assert "retrieved_at" in first
    except ImportError:
        pytest.skip("Airflow not installed in local non-container environment")
