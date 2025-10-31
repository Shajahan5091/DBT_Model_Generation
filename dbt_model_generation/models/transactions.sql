WITH raw_transactions AS (
                SELECT * FROM {{ source('raw', 'raw_transactions') }}
            ),

            transactions AS (
                SELECT
                    transaction_id,
                    amount * 1.18 AS amount_with_tax
                FROM raw_transactions
            )

            SELECT * FROM transactions