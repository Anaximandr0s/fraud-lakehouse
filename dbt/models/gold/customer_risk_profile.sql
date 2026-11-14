select
    card_hash,
    {{ fraud_metrics() }},
    sum(amt) as total_amt,
    min(trans_ts) as first_ts,
    max(trans_ts) as last_ts
from {{ ref('stg_transactions') }}
group by card_hash
