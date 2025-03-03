with company_mapping as (
  select
    company_name,
    count(distinct company_id) as unique_company_ids
  from {{ ref('int_companies_trx_denorm') }}
  group by company_name
)
select *
from company_mapping
where unique_company_ids > 1