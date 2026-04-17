with base as (
    select * from {{ ref('stg_transactions') }}
),

summary as (
    select
        product_cd,
        card4                                           as card_network,
        is_night,
        is_weekend,
        is_high_value,
        COUNT(*)                                        as total_transactions,
        SUM(is_fraud)                                   as actual_fraud,
        SUM(fraud_flag)                                 as flagged,
        ROUND(AVG(fraud_score)::numeric, 2)             as avg_fraud_score,
        ROUND(AVG(transaction_amt)::numeric, 2)         as avg_amount,
        ROUND(SUM(is_fraud)::numeric / COUNT(*), 4)     as fraud_rate
    from base
    group by
        product_cd, card4, is_night, is_weekend, is_high_value
)

select * from summary
order by fraud_rate desc
