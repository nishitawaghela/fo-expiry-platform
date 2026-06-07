with pcr_trend as (
    select * from {{ ref('int_pcr_trend') }}
),

oi_by_strike as (
    select * from {{ ref('int_oi_by_strike') }}
),

latest_metrics as (
    select *
    from pcr_trend
    order by computed_at desc
    limit 1
)

select
    lm.batch_id,
    lm.expiry,
    lm.spot_price,
    lm.pcr,
    lm.max_pain,
    lm.market_sentiment,
    lm.total_ce_oi,
    lm.total_pe_oi,
    lm.computed_at,
    count(distinct obs.strike) as total_strikes
from latest_metrics lm
cross join oi_by_strike obs
group by
    lm.batch_id, lm.expiry, lm.spot_price, lm.pcr,
    lm.max_pain, lm.market_sentiment, lm.total_ce_oi,
    lm.total_pe_oi, lm.computed_at
