with source as (
  select * from {{ source('cvc_raw', 'transaction_type') }}
)

select
transaction_type_id
,transaction_type_name
from source