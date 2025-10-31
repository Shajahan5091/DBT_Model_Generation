WITH cte_transactions AS (
                SELECT
                    transaction_id,
                    amount * 1.18 AS amount_with_tax
                FROM
                    {{ source('raw', 'raw_transactions') }}
            )

            SELECT
                transaction_id,
                amount_with_tax
            FROM
                cte_transactions