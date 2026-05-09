with source as (
    select
        order_id,
        customer_id,
        try_convert(decimal(18, 2), order_amount) as order_amount,
        try_convert(date, order_date) as order_date,
        nullif(trim(order_status), '') as order_status,
        _batch_id,
        _source_file,
        _source_row_number,
        _loaded_at_utc
    from {{ source('bronze', 'raw_orders') }}
)

select *
from source
where order_id is not null
