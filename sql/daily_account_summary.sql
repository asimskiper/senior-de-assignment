CREATE OR REPLACE TABLE default.daily_account_summary AS

WITH base AS (
    SELECT
        account_id,
        CAST(transaction_ts AS DATE) AS transaction_date,
        amount,
        transaction_type,
        merchant_name,
        merchant_category,
        currency
    FROM default.raw_transactions
    WHERE status = 'completed'
      AND is_duplicate = false
),

daily_metrics AS (
    SELECT
        account_id,
        transaction_date,
        CAST(ROUND(SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END), 2) AS DECIMAL(18,2)) AS total_debit_amount,
        CAST(ROUND(SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END), 2) AS DECIMAL(18,2)) AS total_credit_amount,
        CAST(
            ROUND(
                SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END)
                - SUM(CASE WHEN transaction_type = 'debit' THEN amount ELSE 0 END),
                2
            ) AS DECIMAL(18,2)
        ) AS net_amount,
        COUNT(*) AS transaction_count,
        COUNT(DISTINCT merchant_name) AS distinct_merchants,
        CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(currency))) AS currencies
    FROM base
    GROUP BY
        account_id,
        transaction_date
),

category_spend AS (
    SELECT
        account_id,
        transaction_date,
        merchant_category,
        SUM(amount) AS category_amount
    FROM base
    GROUP BY
        account_id,
        transaction_date,
        merchant_category
),

ranked_categories AS (
    SELECT
        account_id,
        transaction_date,
        merchant_category,
        ROW_NUMBER() OVER (
            PARTITION BY account_id, transaction_date
            ORDER BY category_amount DESC, merchant_category ASC
        ) AS rn
    FROM category_spend
),

top_category_per_day AS (
    SELECT
        account_id,
        transaction_date,
        merchant_category AS top_category
    FROM ranked_categories
    WHERE rn = 1
)

SELECT
    d.account_id,
    d.transaction_date,
    d.total_debit_amount,
    d.total_credit_amount,
    d.net_amount,
    d.transaction_count,
    d.distinct_merchants,
    t.top_category,
    d.currencies,
    current_timestamp() AS updated_at
FROM daily_metrics d
LEFT JOIN top_category_per_day t
    ON d.account_id = t.account_id
   AND d.transaction_date = t.transaction_date;