-- The data holds cardholders from 13 years old, hence a band below 18.
with banded as (
    select
        *,
        case
            when age < 18 then 1
            when age <= 25 then 2
            when age <= 40 then 3
            when age <= 60 then 4
            when age > 60 then 5
        end as band_order
    from {{ ref('stg_transactions') }}
)

select
    band_order,
    element_at(array('<18', '18-25', '26-40', '41-60', '>60'), band_order) as age_band,
    {{ fraud_metrics() }}
from banded
group by band_order
