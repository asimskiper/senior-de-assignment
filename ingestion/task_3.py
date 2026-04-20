import csv
import json
import os
import time
from datetime import datetime, timezone, timedelta
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

USE_API = False
CSV_FALLBACK_PATH = "/Volumes/workspace/volume/files/transactions.csv"

PAGE_LIMIT = 1000
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 3
BACKOFF_BASE_SECONDS = 1.5

# Use fresh Task 3 table names
LANDING_TABLE = "default.landing_transactions_incremental_v3"
BRONZE_TABLE = "default.raw_transactions_incremental_v3"
QUARANTINE_TABLE = "default.quarantine_transactions_incremental_v3"

# Use fresh watermark file name
WATERMARK_FILE_PATH = "/Volumes/workspace/volume/files/watermark_task3_v3.json"

LOOKBACK_DAYS = 1

spark = SparkSession.builder.appName("task3_incremental_ingest_clean").getOrCreate()

# ==========================================
# Validation constants
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

TXN_REGEX = r"^TXN-[A-Z0-9]+$"
ACC_REGEX = r"^ACC-\d{4}$"
STRICT_TS_REGEX = r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T([01]\d|2[0-3]):[0-5]\d:[0-5]\dZ$"

# ==========================================
# Helpers
# ==========================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_iso_z(ts: str) -> datetime:
    return datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def format_iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def is_df_empty(df: DataFrame) -> bool:
    return df.limit(1).count() == 0


def table_exists(table_name: str) -> bool:
    parts = table_name.split(".")
    if len(parts) != 2:
        raise ValueError(f"Expected table name in schema.table format, got: {table_name}")

    schema_name, short_table_name = parts

    result = spark.sql(f"SHOW TABLES IN {schema_name} LIKE '{short_table_name}'").count()
    return result > 0


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

            raise RuntimeError(f"HTTP {response.status_code}: {response.text}")

        except (requests.Timeout, requests.ConnectionError) as exc:
            last_error = exc
            sleep_seconds = BACKOFF_BASE_SECONDS * (2 ** attempt)
            print(f"Network error: {exc}. Sleeping {sleep_seconds:.1f}s")
            time.sleep(sleep_seconds)
            attempt += 1

    raise RuntimeError(f"Request failed after retries. Last error: {last_error}")

# ==========================================
# Watermark
# ==========================================

def read_watermark() -> Optional[Dict[str, Any]]:
    if not os.path.exists(WATERMARK_FILE_PATH):
        print("No watermark file found. This will be treated as first run.")
        return None

    with open(WATERMARK_FILE_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    print(f"Loaded watermark: {data}")
    return data


def write_watermark(watermark_value: str, rows_processed: int) -> None:
    payload = {
        "last_successful_watermark": watermark_value,
        "last_run_timestamp": utc_now_iso(),
        "rows_processed": rows_processed,
        "lookback_days": LOOKBACK_DAYS,
    }

    with open(WATERMARK_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    print(f"Wrote watermark file: {WATERMARK_FILE_PATH}")
    print(payload)


def compute_effective_watermark(saved_watermark: Optional[str]) -> Optional[str]:
    if saved_watermark is None:
        return None

    wm_dt = parse_iso_z(saved_watermark)
    effective_dt = wm_dt - timedelta(days=LOOKBACK_DAYS)
    return format_iso_z(effective_dt)

# ==========================================
# Source fetch
# ==========================================

def fetch_transactions_from_api(watermark_filter: Optional[str]) -> List[Dict[str, Any]]:
    all_rows: List[Dict[str, Any]] = []
    offset = 0
    headers = build_headers()

    while True:
        params = {
            "select": "*",
            "limit": PAGE_LIMIT,
            "offset": offset,
            "order": "transaction_date.asc",
        }

        if watermark_filter:
            params["transaction_date"] = f"gte.{watermark_filter}"

        response = request_with_retry(TRANSACTIONS_ENDPOINT, headers, params)
        batch = response.json()

        if not isinstance(batch, list):
            raise RuntimeError("Unexpected API response format")

        print(f"Fetched {len(batch)} rows at offset {offset}")

        if not batch:
            break

        all_rows.extend(batch)

        if len(batch) < PAGE_LIMIT:
            break

        offset += PAGE_LIMIT

    return all_rows


def fetch_transactions_from_csv(csv_path: str, watermark_filter: Optional[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    threshold_dt = parse_iso_z(watermark_filter) if watermark_filter else None

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if threshold_dt:
                ts = row.get("transaction_date")
                try:
                    row_dt = parse_iso_z(ts)
                    if row_dt >= threshold_dt:
                        rows.append(dict(row))
                except Exception:
                    # malformed dates cannot be compared reliably for incremental filtering
                    pass
            else:
                rows.append(dict(row))

    print(f"Read {len(rows)} rows from CSV: {csv_path}")
    return rows


def load_source_records(watermark_filter: Optional[str]) -> List[Dict[str, Any]]:
    if USE_API:
        try:
            return fetch_transactions_from_api(watermark_filter)
        except Exception as exc:
            print(f"API failed: {exc}")
            print("Falling back to CSV")
            return fetch_transactions_from_csv(CSV_FALLBACK_PATH, watermark_filter)

    return fetch_transactions_from_csv(CSV_FALLBACK_PATH, watermark_filter)

# ==========================================
# Landing
# ==========================================

def create_landing_df(records: List[Dict[str, Any]]) -> DataFrame:
    ingestion_ts = utc_now_iso()
    source_system = "supabase_api" if USE_API else "csv_fallback"

    rows = []
    for r in records:
        rows.append({
            "transaction_id": None if r.get("transaction_id") is None else str(r.get("transaction_id")).strip().upper(),
            "account_id": None if r.get("account_id") is None else str(r.get("account_id")).strip(),
            "transaction_date": None if r.get("transaction_date") is None else str(r.get("transaction_date")).strip(),
            "amount_raw": None if r.get("amount") is None else str(r.get("amount")).strip(),
            "currency": None if r.get("currency") is None else str(r.get("currency")).strip(),
            "transaction_type": None if r.get("transaction_type") is None else str(r.get("transaction_type")).strip(),
            "merchant_name": None if r.get("merchant_name") is None else str(r.get("merchant_name")),
            "merchant_category": None if r.get("merchant_category") is None else str(r.get("merchant_category")).strip(),
            "status": None if r.get("status") is None else str(r.get("status")).strip(),
            "country_code": None if r.get("country_code") is None else str(r.get("country_code")).strip(),
            "source_system": source_system,
            "ingestion_timestamp_raw": ingestion_ts,
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
        T.StructField("source_system", T.StringType(), True),
        T.StructField("ingestion_timestamp_raw", T.StringType(), True),
    ])

    return (
        spark.createDataFrame(rows, schema=schema)
        .withColumn("ingestion_timestamp", F.to_timestamp("ingestion_timestamp_raw", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
        .drop("ingestion_timestamp_raw")
    )

# ==========================================
# Validation
# ==========================================

def add_validation_columns(df: DataFrame) -> DataFrame:
    account_number = F.regexp_extract(F.col("account_id"), r"^ACC-(\d{4})$", 1).cast("int")

    validated = (
        df
        .withColumn("amount", F.col("amount_raw").cast(T.DecimalType(18, 2)))
        .withColumn(
            "transaction_ts",
            F.expr("""try_to_timestamp(transaction_date, "yyyy-MM-dd'T'HH:mm:ss'Z'")""")
        )
        .withColumn("err_transaction_id",
            F.when(F.col("transaction_id").isNull() | (F.col("transaction_id") == ""), "transaction_id is missing")
             .when(~F.col("transaction_id").rlike(TXN_REGEX), "transaction_id must match ^TXN-[A-Z0-9]+$")
        )
        .withColumn("err_account_id",
            F.when(F.col("account_id").isNull() | (F.col("account_id") == ""), "account_id is missing")
             .when(~F.col("account_id").rlike(ACC_REGEX), "account_id must match ^ACC-\\d{4}$")
             .when((account_number < 1001) | (account_number > 1020), "account_id must be between ACC-1001 and ACC-1020")
        )
        .withColumn("err_transaction_date_format",
            F.when(F.col("transaction_date").isNull() | (F.col("transaction_date") == ""), "transaction_date is missing")
             .when(~F.col("transaction_date").rlike(STRICT_TS_REGEX), "transaction_date must match strict ISO 8601 format YYYY-MM-DDTHH:MM:SSZ")
        )
        .withColumn("err_transaction_date_value",
            F.when(
                F.col("transaction_date").rlike(STRICT_TS_REGEX) & F.col("transaction_ts").isNull(),
                "transaction_date is not a valid calendar date/time"
            )
        )
        .withColumn("err_transaction_date_range",
            F.when(
                F.col("transaction_ts").isNotNull() &
                (
                    (F.col("transaction_ts") < F.lit("2024-01-01 00:00:00").cast("timestamp")) |
                    (F.col("transaction_ts") > F.lit("2024-12-31 23:59:59").cast("timestamp"))
                ),
                "transaction_date falls outside expected loadable range"
            )
        )
        .withColumn("err_amount",
            F.when(F.col("amount_raw").isNull() | (F.trim(F.col("amount_raw")) == ""), "amount is missing")
             .when(~F.col("amount_raw").rlike(r"^-?\d+(\.\d{1,2})?$"), "amount must be a valid decimal with at most 2 decimal places")
             .when(F.col("amount") <= F.lit(0), "amount must be strictly greater than 0")
        )
        .withColumn("err_currency",
            F.when(F.col("currency").isNull() | (F.col("currency") == ""), "currency is missing")
             .when(~F.col("currency").isin(ALLOWED_CURRENCIES), "currency is invalid")
        )
        .withColumn("err_transaction_type",
            F.when(F.col("transaction_type").isNull() | (F.col("transaction_type") == ""), "transaction_type is missing")
             .when(~F.col("transaction_type").isin(ALLOWED_TRANSACTION_TYPES), "transaction_type is invalid")
        )
        .withColumn("err_merchant_name",
            F.when(F.col("merchant_name").isNull() | (F.col("merchant_name") == ""), "merchant_name is missing")
             .when(~F.col("merchant_name").rlike(r".*\S.*"), "merchant_name must contain at least one non-whitespace character")
        )
        .withColumn("err_merchant_category",
            F.when(F.col("merchant_category").isNull() | (F.col("merchant_category") == ""), "merchant_category is missing")
             .when(~F.col("merchant_category").isin(ALLOWED_MERCHANT_CATEGORIES), "merchant_category is invalid")
        )
        .withColumn("err_status",
            F.when(F.col("status").isNull() | (F.col("status") == ""), "status is missing")
             .when(~F.col("status").isin(ALLOWED_STATUSES), "status is invalid")
        )
        .withColumn("err_country_code",
            F.when(F.col("country_code").isNull() | (F.col("country_code") == ""), "country_code is missing")
             .when(~F.col("country_code").rlike(r"^[A-Z]{2}$"), "country_code must be exactly two uppercase letters")
             .when(~F.col("country_code").isin(ASSIGNED_COUNTRY_CODES), "country_code must be an assigned ISO 3166-1 alpha-2 code")
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

    return (
        validated
        .withColumn("error_array", F.array(*[F.col(c) for c in error_cols]))
        .withColumn("error_array", F.expr("filter(error_array, x -> x is not null)"))
        .withColumn("error_reason", F.concat_ws("; ", F.col("error_array")))
        .withColumn("is_valid", F.size(F.col("error_array")) == 0)
    )

# ==========================================
# Existing transaction IDs
# ==========================================

def get_existing_transaction_ids() -> DataFrame:
    schema = T.StructType([T.StructField("transaction_id", T.StringType(), True)])

    if not table_exists(LANDING_TABLE):
        print(f"{LANDING_TABLE} does not exist yet.")
        return spark.createDataFrame([], schema=schema)

    df = (
        spark.table(LANDING_TABLE)
        .select(F.upper(F.trim(F.col("transaction_id"))).alias("transaction_id"))
        .where(F.col("transaction_id").isNotNull())
        .dropDuplicates(["transaction_id"])
    )

    print(f"Loaded existing transaction IDs from {LANDING_TABLE}: {df.count()}")
    return df

# ==========================================
# Existing natural keys for duplicate marking
# ==========================================

def build_natural_key_expr(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "natural_key",
        F.concat_ws(
            "||",
            F.coalesce(F.col("account_id"), F.lit("")),
            F.coalesce(F.col("transaction_date"), F.lit("")),
            F.coalesce(F.col("amount").cast("string"), F.lit("")),
            F.coalesce(F.col("currency"), F.lit("")),
            F.coalesce(F.col("transaction_type"), F.lit("")),
            F.coalesce(F.col("merchant_name"), F.lit("")),
            F.coalesce(F.col("merchant_category"), F.lit("")),
            F.coalesce(F.col("status"), F.lit("")),
            F.coalesce(F.col("country_code"), F.lit("")),
        )
    )


def get_existing_natural_keys() -> DataFrame:
    schema = T.StructType([T.StructField("natural_key", T.StringType(), True)])

    if not table_exists(BRONZE_TABLE):
        return spark.createDataFrame([], schema=schema)

    existing = spark.table(BRONZE_TABLE).select(
        "account_id",
        "transaction_date",
        "amount",
        "currency",
        "transaction_type",
        "merchant_name",
        "merchant_category",
        "status",
        "country_code",
    )

    return build_natural_key_expr(existing).select("natural_key").dropDuplicates(["natural_key"])


def mark_duplicates_incremental(valid_new_df: DataFrame) -> DataFrame:
    existing_keys = get_existing_natural_keys()
    staged = build_natural_key_expr(valid_new_df)

    staged = staged.join(
        existing_keys.withColumnRenamed("natural_key", "existing_natural_key"),
        staged["natural_key"] == F.col("existing_natural_key"),
        "left"
    ).withColumn(
        "existing_duplicate",
        F.col("existing_natural_key").isNotNull()
    ).drop("existing_natural_key")

    w = Window.partitionBy("natural_key").orderBy(F.col("transaction_id"))
    staged = staged.withColumn("batch_duplicate_rank", F.row_number().over(w))

    return (
        staged.withColumn(
            "is_duplicate",
            F.when(F.col("existing_duplicate"), F.lit(True))
             .when(F.col("batch_duplicate_rank") > 1, F.lit(True))
             .otherwise(F.lit(False))
        )
        .drop("existing_duplicate", "batch_duplicate_rank", "natural_key")
    )

# ==========================================
# Writes
# ==========================================

def append_if_not_empty(df: DataFrame, table_name: str, label: str) -> None:
    if is_df_empty(df):
        print(f"No {label} rows to append.")
        return

    df.write.format("delta").mode("append").saveAsTable(table_name)
    print(f"Appended rows to {table_name}")

# ==========================================
# Main
# ==========================================

def main() -> None:
    print("Starting Task 3 incremental ingestion...")

    watermark_data = read_watermark()
    saved_watermark = watermark_data["last_successful_watermark"] if watermark_data else None
    effective_watermark = compute_effective_watermark(saved_watermark)

    print(f"Saved watermark     : {saved_watermark}")
    print(f"Effective watermark : {effective_watermark}")

    records = load_source_records(effective_watermark)

    if len(records) == 0:
        print("No rows returned from source.")
        if saved_watermark:
            write_watermark(saved_watermark, 0)
        return

    landing_df = create_landing_df(records)

    print(f"Using landing table name: {LANDING_TABLE}")
    print(f"Using bronze table name: {BRONZE_TABLE}")
    print(f"Using quarantine table name: {QUARANTINE_TABLE}")
    print(f"Using watermark file: {WATERMARK_FILE_PATH}")
    print(f"Landing table exists? {table_exists(LANDING_TABLE)}")
    existing_ids = get_existing_transaction_ids()
    print(f"Incoming landing row count: {landing_df.count()}")

    new_landing_df = landing_df.join(existing_ids, on="transaction_id", how="left_anti")
    print(f"New landing row count after anti-join: {new_landing_df.count()}")

    if is_df_empty(new_landing_df):
        print("No new rows after comparing against already ingested transaction_ids.")
        if saved_watermark:
            write_watermark(saved_watermark, 0)
        return

    append_if_not_empty(new_landing_df, LANDING_TABLE, "landing")

    validated_df = add_validation_columns(new_landing_df)

    quarantine_new_df = (
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

    valid_new_df = (
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

    bronze_new_df = mark_duplicates_incremental(valid_new_df)

    append_if_not_empty(quarantine_new_df, QUARANTINE_TABLE, "quarantine")
    append_if_not_empty(bronze_new_df, BRONZE_TABLE, "bronze")

    max_ts_row = bronze_new_df.agg(F.max("transaction_ts").alias("max_ts")).collect()[0]
    max_ts = max_ts_row["max_ts"]

    if max_ts is not None:
        new_watermark = max_ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    else:
        new_watermark = saved_watermark

    processed_rows = new_landing_df.count()

    if new_watermark:
        write_watermark(new_watermark, processed_rows)

    print("========== TASK 3 SUMMARY ==========")
    print(f"Fetched rows from source       : {len(records)}")
    print(f"New rows after anti-join       : {processed_rows}")
    print(f"New quarantine rows           : {quarantine_new_df.count()}")
    print(f"New bronze rows               : {bronze_new_df.count()}")
    print(f"New duplicate bronze rows     : {bronze_new_df.filter(F.col('is_duplicate')).count()}")
    print(f"Updated watermark             : {new_watermark}")
    print("====================================")


if __name__ == "__main__":
    main()