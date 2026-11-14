select
    state,
    {{ fraud_metrics() }},
    avg(lat) as lat,
    avg(long) as long
from {{ ref('stg_transactions') }}
group by state
