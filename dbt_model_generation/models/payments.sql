{#
Model: payments
Description: Staging model for payments data with payment mode categorization
Created Date: 2024-12-19
Author: AI Generated
#}

WITH source_data AS (
    SELECT 
        payment_id,
        payment_mode
    FROM {{ source('raw', 'raw_payments') }}
),

transformed AS (
    SELECT 
        CAST(payment_id AS STRING) AS payment_id,
        CASE 
            WHEN UPPER(payment_mode) IN ('CREDIT_CARD', 'DEBIT_CARD', 'CARD') THEN 'Card'
            WHEN UPPER(payment_mode) IN ('DIGITAL_WALLET', 'WALLET', 'PAYTM', 'GPAY') THEN 'Wallet'
            WHEN UPPER(payment_mode) IN ('CASH', 'COD') THEN 'Cash'
            ELSE 'Card'  -- Default fallback
        END AS payment_category
    FROM source_data
    WHERE payment_id IS NOT NULL
)

SELECT * FROM transformed