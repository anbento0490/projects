
-- NAV Claculation Step 3: 
-- Calculate NAV by Company With Different Calculation of Scaling Factor
WITH companies_trxs AS (
select * from {{ ref('int_companies_trx_denorm') }}
)
,fund_nav_by_date AS (
select * from {{ ref('nav_calculation_step1') }}
)
,company_agg AS (
  SELECT
    fund_name,
    transaction_date,
    SUM(transaction_amount) AS total_company_valuation
  FROM companies_trxs
  GROUP BY fund_name, transaction_date
)
,company_detail AS (
  SELECT 
    c.fund_name,
    c.company_id,
    c.company_name,
    c.transaction_date,
    c.transaction_amount AS company_valuation,
    ca.total_company_valuation
  FROM companies_trxs c
  JOIN company_agg ca 
    ON c.fund_name = ca.fund_name
   AND c.transaction_date = ca.transaction_date
)
,scaling AS (
  SELECT 
    f.fund_name,
    f.date,
    f.nav as fund_nav,
    ca.total_company_valuation,
    round(f.nav / ca.total_company_valuation,2) AS scaling_factor
  FROM fund_nav_by_date f
  JOIN company_agg ca
    ON f.fund_name = ca.fund_name
   AND f.date = ca.transaction_date
)
SELECT
  cd.fund_name,
  cd.company_id,
  cd.company_name,
  cd.transaction_date,
  cd.company_valuation,
  s.fund_nav,
  s.total_company_valuation,
  s.scaling_factor,
  round(cd.company_valuation * s.scaling_factor, 2) AS company_NAV
FROM company_detail cd
JOIN scaling s 
  ON cd.fund_name = s.fund_name 
 AND cd.transaction_date = s.date
ORDER BY cd.fund_name, cd.transaction_date, cd.company_id