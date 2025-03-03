with company_trx as (
  select * from {{ ref('stg_company_transactions') }}
)
,funds as (
  select * from {{ ref('stg_funds') }}
)
,companies as (
  select * from {{ ref('stg_companies') }}
)
,trx_type as (
  select * from {{ ref('stg_transaction_type') }}
)
,sectors as (
  select * from {{ ref('stg_sectors') }}
)
,countries as (
  select * from {{ ref('stg_countries') }}
)

select 
    ct.transaction_id
    ,f.fund_name
    ,ct.company_id
    ,comp.company_name
    ,tt.transaction_type_name as transaction_type
    ,ct.transaction_index
    ,ct.transaction_date
    ,ct.transaction_amount
    ,case when comp.company_name = 'Other Assets' then 'Other' else s.sector_name end as sector
    ,case when c.country_name is NULL then 'Not Available' else c.country_name end as country
    ,case when c.region_name is NULL then 'Not Available' else c.region_name end as region
    ,current_timestamp as last_processed_at
from company_trx ct
left join funds f on ct.fund_id = f.fund_id
left join companies comp on ct.company_id = comp.company_id
left join trx_type tt on ct.transaction_type_id = tt.transaction_type_id
left join sectors s on ct.sector_id = s.sector_id
left join countries c on ct.country_id = c.country_id