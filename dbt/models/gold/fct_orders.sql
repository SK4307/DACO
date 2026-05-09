select
    order_id,
    customer_id,
    order_amount,
    order_date,
    order_status,
    _batch_id,
    _source_file,
    _source_row_number,
    _loaded_at_utc
from {{ ref('stg_orders') }}
