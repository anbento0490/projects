-- NAV Claculation Step 1: 
-- Calculate NAV by Fund focusing on Vlaution, Call and Distribution trxs
WITH fund_data AS (
  SELECT * FROM {{ ref('int_fund_trx_denorm') }}
)
,ranked_trxs as (
SELECT
  fund_name,
  transaction_date,
  transaction_type,
  transaction_amount,
  ROW_NUMBER() OVER (PARTITION BY fund_name, transaction_date, transaction_type ORDER BY transaction_index DESC) AS rn
FROM fund_data
WHERE transaction_type IN ('Valuation', 'Call', 'Distribution')
)
,latest_trxs as (
SELECT
  fund_name,
  transaction_date,
  transaction_type,
  transaction_amount
FROM ranked_trxs
WHERE rn = 1
)
,valuations_trxs as (
SELECT
  fund_name,
  transaction_date,
  transaction_amount as amount
FROM latest_trxs
WHERE transaction_type = 'Valuation'
)
,other_trxs as (
SELECT
  fund_name,
  transaction_date,
  transaction_amount AS amount
FROM latest_trxs
WHERE transaction_type IN ('Call', 'Distribution')
)
,all_trxs as (
SELECT fund_name, transaction_date, 'valuation' AS event_type, amount AS val_amount, NULL AS other_amount
FROM valuations_trxs
UNION ALL
SELECT fund_name, transaction_date, 'other' AS event_type, NULL AS val_amount, amount AS other_amount
FROM other_trxs
)
,all_trxs_ff as(
SELECT
  at.fund_name,
  at.transaction_date,
  at.event_type,
  COALESCE(at.val_amount,
    ( SELECT at2.val_amount FROM all_trxs at2
      WHERE at2.fund_name = at.fund_name
        AND at2.transaction_date < at.transaction_date
        AND at2.val_amount IS NOT NULL
      ORDER BY at2.transaction_date DESC
      LIMIT 1)
  ) AS ff_val_amount,
  at.other_amount
FROM all_trxs at
ORDER BY at.fund_name, at.transaction_date
)
select
fund_name
,transaction_date as date
,ff_val_amount + coalesce(other_amount, 0) as nav
from all_trxs_ff