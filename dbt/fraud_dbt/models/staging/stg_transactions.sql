with source as (
    select * from {{ source('fraud', 'raw_transactions') }}
),

renamed as (
    select
        transaction_id,
        is_fraud,
        transaction_dt,
        transaction_amt,
        product_cd,
        card1,
        card4,
        card6,
        addr1,
        addr2,
        p_emaildomain,
        r_emaildomain,
        device_type,
        device_info,
        transaction_timestamp,
        hour_of_day,
        day_of_week,
        is_weekend,
        is_night,
        amt_zscore,
        amt_log,
        is_round_amount,
        is_high_value,
        is_high_fraud_product,
        card_velocity,
        p_email_risky,
        email_match,
        has_identity,
        is_mobile,
        v_null_density,
        fraud_score,
        fraud_flag,
        loaded_at
    from source
)

select * from renamed
