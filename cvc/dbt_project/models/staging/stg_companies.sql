with source as (
  select * from {{ source('cvc_raw', 'companies') }}
)

select
company_id
,company_name
,sector_id
,country_id
from source