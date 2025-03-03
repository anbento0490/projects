with source as (
  select * from {{ source('cvc_raw', 'fund_transactions') }}
)

select
transaction_id
,fund_id
,transaction_type_id
,transaction_index
,to_char(to_date(transaction_date, 'MM/DD/YY'), 'YYYY-MM-DD') AS transaction_date
,cast(transaction_amount as decimal(35,2)) as transaction_amount
from source