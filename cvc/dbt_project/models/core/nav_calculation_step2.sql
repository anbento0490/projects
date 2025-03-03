-- NAV Claculation Step 2: 
-- Calculate NAV by Company
with companies_trxs as (
select * from {{ ref('int_companies_trx_denorm') }}
)
,funds_trxs as(
select * from {{ ref('int_fund_trx_denorm') }}
)
,ownership_data AS (
  SELECT
    c.fund_name,
    c.company_id,
    c.company_name,
    c.transaction_date,
    c.transaction_amount AS company_valuation,
    (SELECT SUM(f.transaction_amount) FROM funds_trxs f WHERE f.fund_name = c.fund_name AND f.transaction_type = 'Commitment' AND f.transaction_date <= c.transaction_date) AS cumulative_commitment,
    (SELECT MAX(f.fund_size) FROM funds_trxs f WHERE f.fund_name = c.fund_name ) AS latest_fund_size
  FROM companies_trxs c
)
SELECT
  fund_name,
  company_id,
  company_name,
  transaction_date,
  company_valuation,
  cumulative_commitment,
  latest_fund_size,
  round(cumulative_commitment / latest_fund_size, 2) AS ownership_percentage,
  round(company_valuation * (cumulative_commitment / latest_fund_size),2) AS company_NAV
FROM ownership_data
ORDER BY fund_name, transaction_date, company_id