with source as (
    select * from {{ source('public', 'expiry_metrics') }}
),

cleaned as (
    select
        id,
        batch_id,
        symbol,
        expiry,
        spot_price,
        pcr,
        max_pain,
        total_ce_oi,
        total_pe_oi,
        computed_at
    from source
    where pcr is not null
      and max_pain is not null
)

select * from cleaned
