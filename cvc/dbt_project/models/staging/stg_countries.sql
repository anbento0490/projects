with source as (
  select * from {{ source('cvc_raw', 'countries') }}
)

select
country_id
,country_name
,region_name
from source