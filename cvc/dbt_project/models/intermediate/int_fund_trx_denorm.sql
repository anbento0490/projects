with fund_trx as (
  select * from {{ ref('stg_fund_transactions') }}
)
,funds as (
  select * from {{ ref('stg_funds') }}
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
    ft.transaction_id
    ,f.fund_name
    ,f.fund_size
    ,tt.transaction_type_name as transaction_type
    ,ft.transaction_index
    ,ft.transaction_date
    ,ft.transaction_amount
    ,s.sector_name as sector
    ,c.country_name as country
    ,c.region_name as region
    ,current_timestamp as last_processed_at
from fund_trx ft
left join funds f on ft.fund_id = f.fund_id
left join trx_type tt on ft.transaction_type_id = tt.transaction_type_id
left join sectors s on f.sector_id = s.sector_id
left join countries c on f.country_id = c.country_id
