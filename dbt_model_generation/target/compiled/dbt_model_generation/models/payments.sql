

WITH source_data AS (
    SELECT 
        payment_id,
        payment_mode
    FROM demo_db.raw.raw_payments
),

transformed AS (
    SELECT 
        CAST(payment_id AS STRING) AS payment_id,
        CASE 
            WHEN UPPER(payment_mode) IN ('CREDIT_CARD', 'DEBIT_CARD', 'CARD') THEN 'Card'
            WHEN UPPER(payment_mode) IN ('DIGITAL_WALLET', 'WALLET', 'PAYPAL', 'GPAY', 'PAYTM') THEN 'Wallet'
            WHEN UPPER(payment_mode) IN ('CASH', 'COD') THEN 'Cash'
            ELSE 'Cash'
        END AS payment_category
    FROM source_data
)

SELECT * FROM transformed