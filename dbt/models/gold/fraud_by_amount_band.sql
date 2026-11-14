with banded as (
    select
        *,
        case
            when amt < 10 then 1
            when amt < 50 then 2
            when amt < 100 then 3
            when amt < 500 then 4
            when amt < 1000 then 5
            else 6
        end as band_order
    from {{ ref('stg_transactions') }}
)

select
    band_order,
    element_at(array('<10', '10-50', '50-100', '100-500', '500-1000', '>1000'), band_order) as amount_band,
    {{ fraud_metrics() }}
from banded
group by band_order
