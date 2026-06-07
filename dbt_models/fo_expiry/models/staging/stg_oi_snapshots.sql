with source as (
    select * from {{ source('public', 'oi_snapshots') }}
),

cleaned as (
    select
        id,
        batch_id,
        symbol,
        expiry,
        strike,
        option_type,
        coalesce(open_interest, 0)    as open_interest,
        coalesce(oi_change, 0)        as oi_change,
        coalesce(oi_shift, 0)         as oi_shift,
        coalesce(last_price, 0.0)     as last_price,
        coalesce(implied_volatility, 0.0) as implied_volatility,
        coalesce(volume, 0)           as volume,
        spot_price,
        fetched_at,
        created_at
    from source
    where strike is not null
      and option_type in ('CE', 'PE')
)

select * from cleaned
