{{ config(tags=['ml']) }}

-- Every card window looks at earlier transactions only, so no feature sees the future.
with tx as (
    select *, unix_timestamp(trans_ts) as ts_seconds
    from {{ ref('stg_transactions') }}
),

windowed as (
    select
        *,
        count(*) over (
            partition by card_hash order by ts_seconds range between 3600 preceding and 1 preceding
        ) as card_tx_count_1h,
        count(*) over (
            partition by card_hash order by ts_seconds range between 86400 preceding and 1 preceding
        ) as card_tx_count_24h,
        avg(amt) over (
            partition by card_hash order by ts_seconds range between 86400 preceding and 1 preceding
        ) as card_avg_amt_24h,
        ts_seconds - lag(ts_seconds) over (partition by card_hash order by ts_seconds) as secs_since_last_tx
    from tx
)

select
    trans_num,
    trans_ts,
    card_hash,
    category,
    gender,
    age,
    amt,
    ln(amt) as log_amt,
    hour_of_day,
    day_of_week,
    hour_of_day >= 22 or hour_of_day < 6 as is_night,
    distance_km,
    ln(city_pop) as log_city_pop,
    card_tx_count_1h,
    card_tx_count_24h,
    -- null when the card has no transaction in the previous 24 h
    cast(amt / card_avg_amt_24h as double) as card_amt_ratio,
    secs_since_last_tx,
    is_fraud,
    case when trans_ts < timestamp '{{ var("test_split_ts") }}' then 'train' else 'test' end as split
from windowed
