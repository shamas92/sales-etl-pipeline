import io
import logging
from datetime import datetime, timedelta
import boto3
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.operators.python import get_current_context # type: ignore
from airflow.utils.email import send_email

MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
BUCKET_NAME = "sales-lake"
PG_DSN = "dbname=sales_dwh user=airflow password=airflow host=postgres port=5432"


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )


def notify_quality_failure(context):
    """Callback triggered immediately when a task fails."""
    task_instance = context.get("task_instance")
    exception = context.get("exception")
    execution_date = context.get("ds")

    subject = f"[CRITICAL] Data Quality Gate Failed: {task_instance.dag_id} ({execution_date})"
    html_content = f"""
    <h3>Pipeline Execution Halted</h3>
    <p><strong>Task:</strong> {task_instance.task_id}</p>
    <p><strong>Execution Date:</strong> {execution_date}</p>
    <p><strong>Error Details:</strong> <pre>{exception}</pre></p>
    <p>Check the raw bucket <code>s3://{BUCKET_NAME}/raw/</code> for corrupted upstream dumps.</p>
    """
    send_email(
        to=["data-engineering-oncall@localpipeline.dev"],
        subject=subject,
        html_content=html_content,
    )


default_args = {
    "owner": "data_engineering",
    "retries": 0,  # Hard fail on quality gate without wasting compute on retries
    "on_failure_callback": notify_quality_failure,
}


@dag(
    dag_id="sales_star_schema_etl",
    default_args=default_args,
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    params={"inject_bad_data": False},  # Toggle to True in Airflow UI to test failure & SMTP alert
    tags=["portfolio", "etl", "star_schema"],
)
def sales_etl_pipeline():

    @task
    def seed_mock_raw_csv() -> str:
        """Simulates an upstream system landing a messy CSV dump into MinIO."""
        context = get_current_context()
        ds = context["ds"]
        inject_bad_data = context["params"].get("inject_bad_data", False)

        # Messy dates, whitespace in IDs, duplicate rows, and optional null transaction_total
        final_row_total = "" if inject_bad_data else "450.00"
        raw_csv = (
            "transaction_id,transaction_date,customer_id,customer_region,product_id,product_category,quantity,unit_price,transaction_total\n"
            "TXN-1001,2026/10/15, CUST-01 ,EU-Central,PROD-99,Electronics,2,199.99,399.98\n"
            "TXN-1001,2026/10/15, CUST-01 ,EU-Central,PROD-99,Electronics,2,199.99,399.98\n"
            "TXN-1002,15-10-2026,CUST-02,NA-East,PROD-42,Hardware,1,89.50,89.50\n"
            f"TXN-1003,Oct 15 2026,CUST-03,EU-West,PROD-15,Peripherals,3,150.00,{final_row_total}\n"
        )

        object_key = f"raw/sales_{ds}.csv"
        s3 = get_s3_client()
        s3.put_object(Bucket=BUCKET_NAME, Key=object_key, Body=raw_csv.encode("utf-8"))
        logging.info(f"Uploaded simulated dump to s3://{BUCKET_NAME}/{object_key}")
        return object_key

    @task
    def extract_validate_and_clean(raw_key: str) -> str:
        """
        Extracts CSV from MinIO, runs the hard Data Quality Gate on transaction_total,
        normalizes dates/strings with pandas, and writes a clean Parquet file to staging.
        """
        context = get_current_context()
        ds = context["ds"]
        s3 = get_s3_client()

        response = s3.get_object(Bucket=BUCKET_NAME, Key=raw_key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))

        # --- HARD DATA QUALITY GATE ---
        null_financials = df["transaction_total"].isna().sum()
        if null_financials > 0:
            bad_txns = df[df["transaction_total"].isna()]["transaction_id"].tolist()
            raise AirflowFailException(
                f"DATA QUALITY GATE BREACH: Found {null_financials} null value(s) in "
                f"'transaction_total'. Offending transaction IDs: {bad_txns}"
            )

        # --- CLEANING & NORMALIZATION ---
        # 1. Deduplicate based on primary transaction identifier
        df = df.drop_duplicates(subset=["transaction_id"], keep="first").copy()

        # 2. Strip whitespace and standardize casing on string keys
        for col in ["transaction_id", "customer_id", "customer_region", "product_id", "product_category"]:
            df[col] = df[col].astype(str).str.strip()

        # 3. Normalize mixed date formats into ISO YYYY-MM-DD and integer date_key
        df["full_date"] = pd.to_datetime(df["transaction_date"], format="mixed").dt.date
        df["date_key"] = pd.to_datetime(df["full_date"]).dt.strftime("%Y%m%d").astype(int)

        # 4. Enforce numeric types
        df["quantity"] = df["quantity"].astype(int)
        df["unit_price"] = df["unit_price"].astype(float).round(2)
        df["transaction_total"] = df["transaction_total"].astype(float).round(2)

        # Write cleaned data to MinIO staging zone as Parquet (avoids XCom bloat)
        staging_key = f"staging/sales_clean_{ds}.parquet"
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        s3.put_object(Bucket=BUCKET_NAME, Key=staging_key, Body=parquet_buffer.getvalue())
        logging.info(f"Staged {len(df)} cleaned records to s3://{BUCKET_NAME}/{staging_key}")
        return staging_key

    @task
    def load_star_schema(staging_key: str):
        """Reads cleaned Parquet from MinIO and performs idempotent upserts into PostgreSQL."""
        s3 = get_s3_client()
        response = s3.get_object(Bucket=BUCKET_NAME, Key=staging_key)
        df = pd.read_parquet(io.BytesIO(response["Body"].read()))

        # Prepare Dimension & Fact Tuples
        dt_series = pd.to_datetime(df["full_date"])
        dim_date_rows = (
            pd.DataFrame({
                "date_key": df["date_key"],
                "full_date": df["full_date"],
                "year": dt_series.dt.year,
                "quarter": dt_series.dt.quarter,
                "month": dt_series.dt.month,
                "day_of_week": dt_series.dt.dayofweek,
                "is_weekend": dt_series.dt.dayofweek >= 5,
            })
            .drop_duplicates(subset=["date_key"])
            .to_records(index=False)
            .tolist()
        )

        dim_customer_rows = (
            df[["customer_id", "customer_region"]]
            .drop_duplicates(subset=["customer_id"])
            .to_records(index=False)
            .tolist()
        )

        dim_product_rows = (
            df[["product_id", "product_category", "unit_price"]]
            .drop_duplicates(subset=["product_id"])
            .to_records(index=False)
            .tolist()
        )

        fact_sales_rows = (
            df[["transaction_id", "date_key", "customer_id", "product_id", "quantity", "transaction_total"]]
            .to_records(index=False)
            .tolist()
        )

        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.dim_date (date_key, full_date, year, quarter, month, day_of_week, is_weekend)
                    VALUES %s
                    ON CONFLICT (date_key) DO NOTHING;
                    """,
                    dim_date_rows,
                )

                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.dim_customer (customer_id, customer_region)
                    VALUES %s
                    ON CONFLICT (customer_id) DO UPDATE
                    SET customer_region = EXCLUDED.customer_region,
                        updated_at = CURRENT_TIMESTAMP;
                    """,
                    dim_customer_rows,
                )

                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.dim_product (product_id, product_category, unit_price)
                    VALUES %s
                    ON CONFLICT (product_id) DO UPDATE
                    SET product_category = EXCLUDED.product_category,
                        unit_price = EXCLUDED.unit_price;
                    """,
                    dim_product_rows,
                )

                execute_values(
                    cur,
                    """
                    INSERT INTO dwh.fact_sales (transaction_id, date_key, customer_id, product_id, quantity, transaction_total)
                    VALUES %s
                    ON CONFLICT (transaction_id) DO UPDATE
                    SET quantity = EXCLUDED.quantity,
                        transaction_total = EXCLUDED.transaction_total,
                        ingested_at = CURRENT_TIMESTAMP;
                    """,
                    fact_sales_rows,
                )
        logging.info(f"Loaded {len(fact_sales_rows)} rows into dwh.fact_sales.")

    raw_file = seed_mock_raw_csv()
    staged_parquet = extract_validate_and_clean(raw_file)
    load_star_schema(staged_parquet)


sales_etl_pipeline()