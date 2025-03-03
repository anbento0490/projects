with source as (
  select * from {{ source('cvc_raw', 'sectors') }}
)

select
sector_id
,sector_name
from source