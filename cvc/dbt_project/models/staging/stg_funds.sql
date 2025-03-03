with source as (
  select * from {{ source('cvc_raw', 'funds') }}
)

select
  fund_id
  ,fund_name
  ,cast(fund_size as decimal(35,2)) as fund_size
  ,sector_id
  ,country_id
from source