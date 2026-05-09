select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    min(_loaded_at_utc) as first_loaded_at_utc,
    max(_loaded_at_utc) as last_loaded_at_utc
from {{ ref('stg_customers') }}
group by
    customer_id,
    first_name,
    last_name,
    email,
    created_at
