with source as (
    select
        customer_id,
        nullif(trim(first_name), '') as first_name,
        nullif(trim(last_name), '') as last_name,
        nullif(trim(email), '') as email,
        try_convert(datetime2, created_at) as created_at,
        _batch_id,
        _source_file,
        _source_row_number,
        _loaded_at_utc
    from {{ source('bronze', 'raw_customers') }}
),

deduplicated as (
    select *
    from source
    where customer_id is not null
)

select * from deduplicated
