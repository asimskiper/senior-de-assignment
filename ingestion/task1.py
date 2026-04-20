import csv
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# ==========================================
# Config
# ==========================================

API_BASE_URL = "https://fgbjekjqnbmtkmeewexb.supabase.co/rest/v1"
API_KEY = "sb_publishable_W2MbiakvFFthMHtlrzSkQw_URTiUI6G"
TRANSACTIONS_ENDPOINT = f"{API_BASE_URL}/transactions"

USE_API = True
CSV_FALLBACK_PATH = "/Volumes/workspace/volume/files/transactions.csv"   # update if needed

PAGE_LIMIT = 1000
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.5

LANDING_TABLE = "default.landing_transactions_all"
BRONZE_TABLE = "default.raw_transactions"
QUARANTINE_TABLE = "default.quarantine_transactions"

WRITE_MODE = "overwrite"

spark = SparkSession.builder.appName("task1_ingest_medallion_style").getOrCreate()

# ==========================================
# Constants
# ==========================================

ALLOWED_CURRENCIES = ["USD", "EUR", "GBP", "CHF", "JPY", "AUD", "CAD"]
ALLOWED_TRANSACTION_TYPES = ["debit", "credit"]
ALLOWED_MERCHANT_CATEGORIES = [
    "e-commerce",
    "travel",
    "food_and_beverage",
    "groceries",
    "electronics",
    "retail",
    "entertainment",
    "health",
    "transportation",
    "home_and_garden",
    "payroll",
    "transfer",
]
ALLOWED_STATUSES = ["completed", "pending", "failed", "reversed"]

ASSIGNED_COUNTRY_CODES = [
    "AD","AE","AF","AG","AI","AL","AM","AO","AR","AT","AU","AZ",
    "BA","BB","BD","BE","BF","BG","BH","BI","BJ","BN","BO","BR",
    "BS","BT","BW","BY","BZ","CA","CD","CF","CG","CH","CI","CL",
    "CM","CN","CO","CR","CU","CV","CY","CZ","DE","DJ","DK","DM",
    "DO","DZ","EC","EE","EG","ER","ES","ET","FI","FJ","FM","FR",
    "GA","GB","GD","GE","GH","GM","GN","GQ","GR","GT","GW","GY",
    "HK","HN","HR","HT","HU","ID","IE","IL","IN","IQ","IR","IS",
    "IT","JM","JO","JP","KE","KG","KH","KI","KM","KN","KP","KR",
    "KW","KZ","LA","LB","LC","LI","LK","LR","LS","LT","LU","LV",
    "LY","MA","MC","MD","ME","MG","MH","MK","ML","MM","MN","MR",
    "MT","MU","MV","MW","MX","MY","MZ","NA","NE","NG","NI","NL",
    "NO","NP","NR","NZ","OM","PA","PE","PG","PH","PK","PL","PT",
    "PW","PY","QA","RO","RS","RW","SA","SB","SC","SD","SE","SG",
    "SI","SK","SL","SM","SN","SO","SR","SS","ST","SV","SY","SZ",
    "TD","TG","TH","TJ","TL","TN","TO","TR","TT","TV","TW","TZ",
    "UA","UG","US","UY","UZ","VA","VC","VE","VN","VU","WS","YE",
    "ZA","ZM","ZW"
]

STRICT_TS_REGEX = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ$"
TXN_REGEX = r"^TXN-[A-Z0-9]+$"
ACC_REGEX = r"^ACC-\d{4}$"

# ==========================================
# Helpers
# ==========================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def build_headers() -> Dict[str, str]:
    return {
        "apikey": API_KEY,
        "Authorization": f"Bearer {API_KEY}",
    }


def request_with_retry(url: str, headers: Dict[str, str], params: Dict[str, Any]) -> requests.Response:
    attempt = 0
    last_error: Optional[Exception] = None

    while attempt <= MAX_RETRIES:
        try:
            response = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SECONDS)

            if response.status_code == 200:
                return response

            if response.status_code in {429, 500, 502, 503, 504}:
                sleep_seconds = BACKOFF_BASE_SECONDS * (2 ** attempt)
                print(f"Retryable HTTP {response.status_code}. Sleeping {sleep_seconds:.1f}s")
                time.sleep(sleep_seconds)
                attempt += 1
                continue

            raise RuntimeError(f"Non-retryable HTTP {response.status_code}: {response.text}")

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            sleep_seconds = BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"Network error: {exc}. Sleeping {sleep_seconds:.1f}s")
            time.sleep(sleep_seconds)
            attempt += 1

    raise RuntimeError(f"Request failed after retries. Last error: {last_error}")


def fetch_transactions_from_api(limit: int = PAGE_LIMIT) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    headers = build_headers()

    while True:
        params = {
            "select": "*",
            "limit": limit,
            "offset": offset,
            "order": "transaction_date.asc",
        }

        response = request_with_retry(
            url=TRANSACTIONS_ENDPOINT,
            headers=headers,
            params=params,
        )

        batch = response.json()
        if not isinstance(batch, list):
            raise RuntimeError("Unexpected API response format")

        print(f"Fetched {len(batch)} rows at offset {offset}")

        if not batch:
            break

        all_rows.extend(batch)

        if len(batch) < limit:
            break

        offset += limit

    return all_rows


def fetch_transactions_from_csv(csv_path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    print(f"Read {len(rows)} rows from CSV: {csv_path}")
    return rows


def load_source_records() -> List[Dict[str, Any]]:
    if USE_API:
        try:
            return fetch_transactions_from_api()
        except Exception as exc:
            print(f"API failed: {exc}")
            print("Falling back to CSV")
            return fetch_transactions_from_csv(CSV_FALLBACK_PATH)
    return fetch_transactions_from_csv(CSV_FALLBACK_PATH)

# ==========================================
# Build initial landing DataFrame
# ==========================================

def create_landing_df(records: List[Dict[str, Any]]) -> DataFrame:
    ingestion_ts = utc_now_iso()

    normalized = []
    for r in records:
        normalized.append({
            "transaction_id": None if r.get("transaction_id") is None else str(r.get("transaction_id")),
            "account_id": None if r.get("account_id") is None else str(r.get("account_id")),
            "transaction_date": None if r.get("transaction_date") is None else str(r.get("transaction_date")),
            "amount_raw": None if r.get("amount") is None else str(r.get("amount")),
            "currency": None if r.get("currency") is None else str(r.get("currency")),
            "transaction_type": None if r.get("transaction_type") is None else str(r.get("transaction_type")),
            "merchant_name": None if r.get("merchant_name") is None else str(r.get("merchant_name")),
            "merchant_category": None if r.get("merchant_category") is None else str(r.get("merchant_category")),
            "status": None if r.get("status") is None else str(r.get("status")),
            "country_code": None if r.get("country_code") is None else str(r.get("country_code")),
            "ingestion_timestamp_raw": ingestion_ts,
            "source_system": "supabase_api" if USE_API else "csv_fallback",
        })

    schema = T.StructType([
        T.StructField("transaction_id", T.StringType(), True),
        T.StructField("account_id", T.StringType(), True),
        T.StructField("transaction_date", T.StringType(), True),
        T.StructField("amount_raw", T.StringType(), True),
        T.StructField("currency", T.StringType(), True),
        T.StructField("transaction_type", T.StringType(), True),
        T.StructField("merchant_name", T.StringType(), True),
        T.StructField("merchant_category", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("country_code", T.StringType(), True),
        T.StructField("ingestion_timestamp_raw", T.StringType(), True),
        T.StructField("source_system", T.StringType(), True),
    ])

    return (
        spark.createDataFrame(normalized, schema=schema)
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp_raw", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .drop("ingestion_timestamp_raw")
    )

# ==========================================
# Validation logic in Spark
# ==========================================

def add_validation_columns(df: DataFrame) -> DataFrame:
    amount_decimal = F.col("amount_raw").cast(T.DecimalType(18, 2))
    account_number = F.regexp_extract(F.col("account_id"), r"^ACC-(\d{4})$", 1).cast("int")

    validated = (
        df
        .withColumn("amount", amount_decimal)
        .withColumn(
            "transaction_ts",
            F.expr("try_to_timestamp(transaction_date, \"yyyy-MM-dd'T'HH:mm:ss'Z'\")")
        )
        .withColumn("err_transaction_id",
            F.when(F.col("transaction_id").isNull() | (F.col("transaction_id") == ""), F.lit("transaction_id is missing"))
             .when(~F.col("transaction_id").rlike(TXN_REGEX), F.lit("transaction_id must match ^TXN-[A-Z0-9]+$"))
        )
        .withColumn("err_account_id",
            F.when(F.col("account_id").isNull() | (F.col("account_id") == ""), F.lit("account_id is missing"))
             .when(~F.col("account_id").rlike(ACC_REGEX), F.lit("account_id must match ^ACC-\\d{4}$"))
             .when((account_number < 1001) | (account_number > 1020), F.lit("account_id must be between ACC-1001 and ACC-1020"))
        )
        .withColumn("err_transaction_date_format",
            F.when(F.col("transaction_date").isNull() | (F.col("transaction_date") == ""), F.lit("transaction_date is missing"))
             .when(~F.col("transaction_date").rlike(STRICT_TS_REGEX), F.lit("transaction_date must match strict ISO 8601 format YYYY-MM-DDTHH:MM:SSZ"))
        )
        .withColumn("err_transaction_date_value",
            F.when(
                F.col("transaction_date").rlike(STRICT_TS_REGEX) & F.col("transaction_ts").isNull(),
                F.lit("transaction_date is not a valid calendar date/time")
            )
        )
        .withColumn("err_transaction_date_range",
            F.when(
                F.col("transaction_ts").isNotNull() &
                (
                    (F.col("transaction_ts") < F.lit("2024-01-01 00:00:00").cast("timestamp")) |
                    (F.col("transaction_ts") > F.lit("2024-03-31 23:59:59").cast("timestamp"))
                ),
                F.lit("transaction_date falls outside expected Jan-Mar 2024 range")
            )
        )
        .withColumn("err_amount",
            F.when(F.col("amount_raw").isNull() | (F.trim(F.col("amount_raw")) == ""), F.lit("amount is missing"))
             .when(F.col("amount").isNull(), F.lit("amount is not a valid decimal"))
             .when(F.col("amount") <= F.lit(0), F.lit("amount must be strictly greater than 0"))
             .when(~F.col("amount_raw").rlike(r"^\d+(\.\d{1,2})?$"), F.lit("amount must have at most 2 decimal places"))
        )
        .withColumn("err_currency",
            F.when(F.col("currency").isNull() | (F.col("currency") == ""), F.lit("currency is missing"))
             .when(~F.col("currency").isin(ALLOWED_CURRENCIES), F.lit("currency is invalid"))
        )
        .withColumn("err_transaction_type",
            F.when(F.col("transaction_type").isNull() | (F.col("transaction_type") == ""), F.lit("transaction_type is missing"))
             .when(~F.col("transaction_type").isin(ALLOWED_TRANSACTION_TYPES), F.lit("transaction_type is invalid"))
        )
        .withColumn("err_merchant_name",
            F.when(F.col("merchant_name").isNull() | (F.col("merchant_name") == ""), F.lit("merchant_name is missing"))
             .when(~F.col("merchant_name").rlike(r".*\S.*"), F.lit("merchant_name must contain at least one non-whitespace character"))
        )
        .withColumn("err_merchant_category",
            F.when(F.col("merchant_category").isNull() | (F.col("merchant_category") == ""), F.lit("merchant_category is missing"))
             .when(~F.col("merchant_category").isin(ALLOWED_MERCHANT_CATEGORIES), F.lit("merchant_category is invalid"))
        )
        .withColumn("err_status",
            F.when(F.col("status").isNull() | (F.col("status") == ""), F.lit("status is missing"))
             .when(~F.col("status").isin(ALLOWED_STATUSES), F.lit("status is invalid"))
        )
        .withColumn("err_country_code",
            F.when(F.col("country_code").isNull() | (F.col("country_code") == ""), F.lit("country_code is missing"))
             .when(~F.col("country_code").rlike(r"^[A-Z]{2}$"), F.lit("country_code must be exactly two uppercase letters"))
             .when(~F.col("country_code").isin(ASSIGNED_COUNTRY_CODES), F.lit("country_code must be an assigned ISO 3166-1 alpha-2 code"))
        )
    )

    error_cols = [
        "err_transaction_id",
        "err_account_id",
        "err_transaction_date_format",
        "err_transaction_date_value",
        "err_transaction_date_range",
        "err_amount",
        "err_currency",
        "err_transaction_type",
        "err_merchant_name",
        "err_merchant_category",
        "err_status",
        "err_country_code",
    ]

    validated = (
        validated
        .withColumn("error_array", F.array(*[F.col(c) for c in error_cols]))
        .withColumn("error_array", F.expr("filter(error_array, x -> x is not null)"))
        .withColumn("error_reason", F.concat_ws("; ", F.col("error_array")))
        .withColumn("is_valid", F.size(F.col("error_array")) == 0)
    )

    return validated

# ==========================================
# Duplicate logic
# ==========================================

def mark_duplicates(valid_df: DataFrame) -> DataFrame:
    natural_key_cols = [
        "account_id",
        "transaction_date",
        "amount",
        "currency",
        "transaction_type",
        "merchant_name",
        "merchant_category",
        "status",
        "country_code",
    ]

    w = Window.partitionBy(*natural_key_cols).orderBy(F.col("transaction_id"))

    return (
        valid_df
        .withColumn("duplicate_rank", F.row_number().over(w))
        .withColumn("is_duplicate", F.when(F.col("duplicate_rank") > 1, F.lit(True)).otherwise(F.lit(False)))
        .drop("duplicate_rank")
    )

# ==========================================
# Writes
# ==========================================

def write_table(df: DataFrame, table_name: str, mode: str = WRITE_MODE) -> None:
    df.write.format("delta").mode(mode).saveAsTable(table_name)
    print(f"Wrote {table_name}")

# ==========================================
# Main
# ==========================================

def main() -> None:
    print("Starting Task 1 ingestion...")

    records = load_source_records()
    landing_df = create_landing_df(records)

    # 1. Keep full landing copy for reconciliation
    write_table(landing_df, LANDING_TABLE, WRITE_MODE)

    # 2. Validate
    validated_df = add_validation_columns(landing_df)

    # 3. Split invalid to quarantine
    quarantine_df = (
        validated_df
        .filter(~F.col("is_valid"))
        .select(
            "transaction_id",
            "account_id",
            "transaction_date",
            "amount_raw",
            "currency",
            "transaction_type",
            "merchant_name",
            "merchant_category",
            "status",
            "country_code",
            "source_system",
            "ingestion_timestamp",
            "error_reason",
        )
    )

    # 4. Valid raw/bronze
    valid_df = (
        validated_df
        .filter(F.col("is_valid"))
        .select(
            "transaction_id",
            "account_id",
            "transaction_date",
            "transaction_ts",
            "amount",
            "currency",
            "transaction_type",
            "merchant_name",
            "merchant_category",
            "status",
            "country_code",
            "source_system",
            "ingestion_timestamp",
        )
    )

    bronze_df = mark_duplicates(valid_df)

    # 5. Write tables
    write_table(quarantine_df, QUARANTINE_TABLE, WRITE_MODE)
    write_table(bronze_df, BRONZE_TABLE, WRITE_MODE)

    # 6. Metrics
    landing_count = landing_df.count()
    quarantine_count = quarantine_df.count()
    bronze_count = bronze_df.count()
    duplicate_count = bronze_df.filter(F.col("is_duplicate") == True).count()
    unique_valid_count = bronze_df.filter(F.col("is_duplicate") == False).count()

    print("========== TASK 1 SUMMARY ==========")
    print(f"Landing rows           : {landing_count}")
    print(f"Quarantined rows       : {quarantine_count}")
    print(f"Bronze valid rows      : {bronze_count}")
    print(f"Unique valid rows      : {unique_valid_count}")
    print(f"Duplicate valid rows   : {duplicate_count}")
    print("====================================")

    print("Sample quarantined rows:")
    quarantine_df.select("transaction_id", "error_reason").show(20, truncate=False)

    print("Sample duplicates:")
    bronze_df.filter(F.col("is_duplicate") == True).show(20, truncate=False)

    print("Task 1 completed.")


if __name__ == "__main__":
    main()