# Senior Data Engineer Assignment

## Overview

This project implements Task 1 of the take-home assignment: ingesting transaction data from a Supabase REST API, validating data quality, quarantining invalid records, detecting duplicates, and storing valid records in a bronze/raw layer.

The solution is built in Databricks using PySpark and Delta tables.  
To preserve traceability and reconciliation, I used a landing layer to store all fetched records before validation, then split the data into quarantine and raw/bronze outputs.

---

## Technology Stack

- **Databricks Community Edition**
- **PySpark**
- **Delta tables**
- **Databricks SQL**
- **CSV fallback input**
- **Python requests** for REST API ingestion

### Why this stack
I chose Databricks and PySpark because they are well suited for ingestion, validation, and layered data processing.  
I used plain PySpark and SQL rather than dbt for this task to keep the implementation simple and focused on the assignment requirements.

---

## Repository / File Structure

```text
senior-de-assignment/
  README.md
  ingestion/
    task1_ingest.py
  sql/
  outputs/

## Task 2 Daily_Account_Summary
  ## Task 2 - Daily Aggregations per Account

I built `default.daily_account_summary` on top of the validated raw table.

### Logic
The aggregation uses only:
- valid rows from `default.raw_transactions`
- `status = 'completed'`
- `is_duplicate = false`

This ensures that:
- quarantined rows are excluded
- failed, pending, and reversed transactions are excluded
- duplicate transactions do not inflate daily totals

### Output fields
The output table includes:
- `account_id`
- `transaction_date`
- `total_debit_amount`
- `total_credit_amount`
- `net_amount`
- `transaction_count`
- `distinct_merchants`
- `top_category`
- `currencies`
- `updated_at`

### Business logic
- `total_debit_amount` = sum of completed debit amounts per account/day
- `total_credit_amount` = sum of completed credit amounts per account/day
- `net_amount` = total_credit_amount - total_debit_amount
- `transaction_count` = count of completed transactions
- `distinct_merchants` = number of distinct merchant names per day
- `top_category` = merchant category with the highest total spend for that account/day
- `currencies` = comma-separated distinct list of currencies used that day

### Idempotency
The model is implemented using:

`CREATE OR REPLACE TABLE default.daily_account_summary AS ...`

This makes the transformation idempotent because rerunning the SQL rebuilds the table deterministically from the same validated source data.

### Equivalent data quality assertions
Because I did not use dbt, I implemented equivalent SQL checks after building the table.

The following checks all passed:
- no null values in required output fields
- no duplicate `(account_id, transaction_date)` rows
- `net_amount = total_credit_amount - total_debit_amount`
- `transaction_count > 0`

## Task 3 - Incremental Ingestion with Watermark Logic

I implemented incremental ingestion using a JSON watermark file that stores the maximum successfully ingested valid `transaction_date`.

### Watermark strategy
- On the first run, if no watermark file exists, the pipeline performs a full load.
- After a successful run, the pipeline stores the maximum valid `transaction_date` as the new watermark.
- On subsequent runs, the pipeline reads that watermark and computes an effective watermark using a 1-day lookback window.
- The lookback window protects against late-arriving data by re-reading a small overlap before the last processed timestamp.
- To keep the process idempotent, previously ingested rows are removed before writing by anti-joining on `transaction_id` against the incremental landing table.
- Valid rows are then processed, quarantined if invalid, and appended to the incremental bronze table.

### Run results
- **Run 1:** full load, 352 rows fetched, 3 quarantined, 349 valid bronze rows, 5 duplicates flagged, watermark set to `2024-03-30T22:35:29Z`
- **Run 2:** watermark loaded, overlap window reread 3 rows, anti-join removed all already processed rows, 0 new rows processed, watermark unchanged

### Edge cases handled
- If no watermark exists, the pipeline performs a full load.
- If no new records exist, the pipeline completes successfully with 0 rows processed.
- If late-arriving records appear slightly behind the watermark, the lookback window allows them to be picked up on the next run.