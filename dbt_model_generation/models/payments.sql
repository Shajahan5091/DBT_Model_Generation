WITH categorized_payments AS (
                SELECT
                    payment_id,
                    CASE
                        WHEN payment_mode IN ('Credit Card', 'Debit Card') THEN 'Card'
                        WHEN payment_mode = 'E-Wallet' THEN 'Wallet'
                        ELSE 'Cash'
                    END AS payment_category
                FROM {{ source('raw', 'raw_payments') }}
            )
            SELECT
                payment_id,
                payment_category
            FROM categorized_payments