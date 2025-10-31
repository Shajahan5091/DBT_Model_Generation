WITH payment_category AS (
                SELECT
                    payment_id,
                    CASE
                        WHEN payment_mode IN ('VISA', 'MASTERCARD', 'AMEX') THEN 'Card'
                        WHEN payment_mode IN ('PAYPAL', 'SKRILL') THEN 'Wallet'
                        ELSE 'Cash'
                    END AS payment_category
                FROM {{ source('raw', 'raw_payments') }}
            ),

            payments AS (
                SELECT
                    payment_id,
                    payment_category
                FROM payment_category
            )

            SELECT * FROM payments;