with oi_by_strike as (
    select * from {{ ref('int_oi_by_strike') }}
),

latest_metrics as (
    select max_pain, expiry
    from {{ ref('stg_expiry_metrics') }}
    order by computed_at desc
    limit 1
)

select
    obs.strike,
    obs.expiry,
    obs.ce_oi,
    obs.pe_oi,
    obs.ce_oi + obs.pe_oi           as total_oi,
    obs.ce_oi_shift,
    obs.pe_oi_shift,
    obs.ce_iv,
    obs.pe_iv,
    obs.spot_price,
    lm.max_pain,
    case
        when obs.strike = lm.max_pain then true
        else false
    end as is_max_pain_strike
from oi_by_strike obs
cross join latest_metrics lm
order by (obs.ce_oi + obs.pe_oi) desc
