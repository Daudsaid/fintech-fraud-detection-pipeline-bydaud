with flagged as (
    select * from {{ ref('stg_transactions') }}
    where fraud_flag = 1
)

select
    transaction_id,
    transaction_amt,
    product_cd,
    card1,
    card4,
    card6,
    p_emaildomain,
    device_type,
    is_mobile,
    hour_of_day,
    is_night,
    is_weekend,
    is_high_value,
    is_high_fraud_product,
    card_velocity,
    p_email_risky,
    email_match,
    has_identity,
    v_null_density,
    amt_zscore,
    fraud_score,
    fraud_flag,
    is_fraud,
    transaction_timestamp
from flagged
