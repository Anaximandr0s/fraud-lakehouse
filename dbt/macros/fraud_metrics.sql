{% macro fraud_metrics() -%}
    count(*) as tx_count,
    sum(is_fraud) as fraud_count,
    cast(sum(is_fraud) / count(*) as double) as fraud_rate,
    sum(case when is_fraud = 1 then amt else 0 end) as fraud_amt
{%- endmacro %}
