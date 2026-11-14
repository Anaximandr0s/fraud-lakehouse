with per_merchant as (
    select
        merchant,
        {{ fraud_metrics() }}
    from {{ ref('stg_transactions') }}
    group by merchant
)

select
    *,
    rank() over (order by fraud_rate desc, fraud_count desc) as risk_rank
from per_merchant
where tx_count >= 100
