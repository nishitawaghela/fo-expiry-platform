with snapshots as (
    select * from {{ ref('stg_oi_snapshots') }}
),

latest_batch as (
    select batch_id
    from {{ ref('stg_expiry_metrics') }}
    order by computed_at desc
    limit 1
),

oi_by_strike as (
    select
        s.strike,
        s.expiry,
        s.spot_price,
        sum(case when s.option_type = 'CE' then s.open_interest else 0 end) as ce_oi,
        sum(case when s.option_type = 'PE' then s.open_interest else 0 end) as pe_oi,
        sum(case when s.option_type = 'CE' then s.oi_shift else 0 end)      as ce_oi_shift,
        sum(case when s.option_type = 'PE' then s.oi_shift else 0 end)      as pe_oi_shift,
        sum(case when s.option_type = 'CE' then s.implied_volatility else 0 end) as ce_iv,
        sum(case when s.option_type = 'PE' then s.implied_volatility else 0 end) as pe_iv
    from snapshots s
    inner join latest_batch lb on s.batch_id = lb.batch_id
    group by s.strike, s.expiry, s.spot_price
    order by s.strike
)

select * from oi_by_strike
