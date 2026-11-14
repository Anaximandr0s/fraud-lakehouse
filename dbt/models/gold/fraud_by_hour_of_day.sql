select
    hour_of_day,
    {{ fraud_metrics() }}
from {{ ref('stg_transactions') }}
group by hour_of_day
