with metrics as (
    select * from {{ ref('stg_expiry_metrics') }}
),

pcr_trend as (
    select
        batch_id,
        expiry,
        spot_price,
        pcr,
        max_pain,
        total_ce_oi,
        total_pe_oi,
        computed_at,
        case
            when pcr >= 1.2 then 'bearish'
            when pcr <= 0.7 then 'bullish'
            else 'neutral'
        end as market_sentiment
    from metrics
    order by computed_at desc
)

select * from pcr_trend
